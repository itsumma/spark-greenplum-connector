package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.rmi.GPConnectorModes.GPConnectorMode
import com.itsumma.gpconnector.{GPClient, GPOffset, GreenplumRowSet}
import com.itsumma.gpconnector.rmi.{GPConnectorModes, NetUtils, RMIMaster}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, Scan, Statistics, SupportsReportPartitioning, SupportsReportStatistics}
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, GPTarget, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import java.util.{OptionalLong, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil.guessMaxParallelTasks

//import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable.ListBuffer

class GreenplumScan(optionsFactory: GPOptionsFactory,
                    rowSet: GreenplumRowSet,
                    schema: StructType,
                    whereClause: String
                   )
  extends Scan
    with SupportsReportPartitioning
    with SupportsReportStatistics
    with Logging
{
  private var connectorMode: GPConnectorMode = GPConnectorModes.Batch
  private val dbConnectionGuard: Boolean = true
  private var dbConnection: Connection = gpConnect(true)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)

  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  val processing = new AtomicBoolean(false)
  private val settleMs = new AtomicLong(0)
  private val processingMs = new AtomicLong(0)
  private val commitMs = new AtomicLong(0)
  private val epochStart = new AtomicLong(0)
  private val epochTotalMs = new AtomicLong(0)
  private val planInputPartitionsDelayMs = new AtomicLong(0)
  private val groupCommitStart = new AtomicLong(0)
  private val prevEpoch = new AtomicLong(-1)
  private val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  private val tableOrQuery: String = optionsFactory.tableOrQuery
  if (tableOrQuery.isEmpty && schema.isEmpty)
    throw new IllegalArgumentException(s"Must pass a source table name or sql query string in the dbtable option")
  private val targetDetails: GPTarget = GPTarget(tableOrQuery)
  private val dbDefaultSchemaName: String = GPClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
  private val dbTableName: String = targetDetails.getCanonicalName(dbDefaultSchemaName)
  private val sqlThreadPass = new AtomicLong(0)
  private val startOffset: GPOffset = GPOffset()
  startOffset.extract(dbConnection, optionsFactory.offsetRestoreSql)
  private val endOffset: GPOffset = GPOffset(startOffset.get())
  private val endOffsetPrev: GPOffset = endOffset.copy()
  private val sqlThread: Thread = new Thread(new SqlThread(), s"gpfdist-read$queryId")
  sqlThread.setDaemon(true)
  private var numParts: Int = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())

  var rmiMaster: RMIMaster = null
  private val gpVersion: String = GPClient.queryGPVersion(dbConnection)
  private val useTempExtTables: Boolean = optionsFactory.useTempExtTables &&
    (GPClient.versionCompare(gpVersion, "5.0.0") >= 0)
  private val tempTableClause: String = if (useTempExtTables) "TEMP" else ""
  private val distributionPol: String = GPClient.getTableDistributionPolicy(dbConnection,
    targetDetails.getTableSchema(dbDefaultSchemaName),
    targetDetails.cleanDbTableName)
  private val (distributedByClause: String, distributionColNames: String) = GPClient.formatDistributedByClause(
    if (optionsFactory.distributedBy.nonEmpty) optionsFactory.distributedBy
    else distributionPol
  )
  dbConnectionGuard.synchronized {
    if (!dbConnection.getAutoCommit)
      dbConnection.commit()
    dbConnection.close()
    dbConnection = null
  }
  logDebug(s"distributionPol=$distributionPol,\n" +
    s"tblSchema=${targetDetails.getTableSchema(dbDefaultSchemaName)},\n" +
    s"distributedByClause=$distributedByClause,\n")

  override def readSchema(): StructType = schema

  private val toBatchCallNo: AtomicLong = new AtomicLong(0)

  private def gpConnect(autoCommit: Boolean = false): Connection = {
    dbConnectionGuard.synchronized {
      if (dbConnection == null) {
        dbConnection = rowSet.getGpClient.getConnection()
        if (autoCommit || (connectorMode == GPConnectorModes.Batch) || optionsFactory.readStreamAutoCommit) {
          dbConnection.setAutoCommit(true)
        } else {
          dbConnection.setAutoCommit(false)
        }
      }
    }
    dbConnection
  }

  private def getNPartsAndLocations(): (Int, Map[String, Set[String]]) = {
    val numPartsPlan = numParts.synchronized {
      numParts = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())
      numParts
    }
    val segmentLocations: Map[String, Set[String]] = {
      import com.itsumma.gpconnector.rmi.NetUtils
      val hosts = GPClient.nodeNamesWithSegments(dbConnection)
      val resolvedHosts = hosts.map({ case (host, segSet) => NetUtils().resolveHost2Ip(host) -> segSet })
      hosts ++ resolvedHosts
    }
    (numPartsPlan, segmentLocations)
  }

  private val inputPartitions = ListBuffer[InputPartition]()
  private val nPlanCalls = new AtomicLong(0)
  private val planDone = new AtomicBoolean(false)
  def planInputPartitions(): Array[InputPartition] = {
    /**
     * Note: Spark calls this method too often, usually 3 times per every available executor per each batch.
     * We have to fight it tough here.
     */
    val now = System.currentTimeMillis()
    nPlanCalls.incrementAndGet()
    gpConnect()
    if (rmiMaster == null) {
      val (numPartsPlan, segmentLocations) = getNPartsAndLocations()
      rmiMaster = new RMIMaster(optionsFactory, queryId, numPartsPlan, done, aborted, segmentLocations, connectorMode)
    }
    var epoch: Long = rmiMaster.getCurrentEpoch
    if (groupCommitStart.get() > 0)
      epoch += 1
    logDebug(s"planInputPartitions called: epoch=${epoch}, groupCommitStart=${groupCommitStart.get()}")
    if (epoch == prevEpoch.get() + 1) {
      planInputPartitionsDelayMs.set(now - epochStart.get())
      val epochCommitMs = now - groupCommitStart.get()
      if (groupCommitStart.get() > 0) {
        logInfo(s"Epoch ${prevEpoch.get()} read ${rmiMaster.successTasksBatch} tasks " +
          s"in ${epochTotalMs.get()} ms, " +
          s"processingMs=${processingMs.get()}, " +
          s"settleMs=${settleMs.get()}-${planInputPartitionsDelayMs.get()}, " +
          s"commitMs=${commitMs.get()}+${epochCommitMs - commitMs.get()}" +
          s", plan calls=${nPlanCalls.get()}")
        nPlanCalls.set(0)
      }
      inputPartitions.clear()
      val (numPartsPlan, segmentLocations) = getNPartsAndLocations()
      for (i <- 0 until numPartsPlan) {
        val locations: Array[String] = segmentLocations.filter(node => node._2.contains(i.toString)).keySet.toArray
        val inpPart = new GreenplumInputPartitionMeta(i, locations)
        logDebug(s"Scheduling partition $i for epoch ${epoch} " +
          s"with locations=${locations.mkString("Array(", ", ", ")")}")
        inputPartitions += inpPart
      }
      rmiMaster.setGpSegmentsNum(numPartsPlan)
      rmiMaster.address2PrefSeg = segmentLocations
      prevEpoch.set(epoch)
      logDebug(s"Creating ${inputPartitions.size} partitions for epoch ${epoch} queryId=${queryId}")
      planDone.set(true)
    }
    if (!sqlThread.isAlive) {
      logDebug(s"\nStarting new batch, queryId=${queryId} ..")
      sqlThread.start()
    }
    inputPartitions.toArray
  }

  //import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
  private var batch: GreenplumBatch = null
  override def toBatch: Batch = {
    toBatchCallNo.incrementAndGet()
    if (toBatchCallNo.get() == 1) {
      connectorMode = GPConnectorModes.Batch
      batch = new GreenplumBatch(optionsFactory, queryId, schema, this)
    }
    logDebug(s"toBatch callNo=${toBatchCallNo.get()}")
    batch
  }

  private var microBatch: GreenplumMicroBatch = null
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    toBatchCallNo.incrementAndGet()
    if (toBatchCallNo.get() == 1) {
      connectorMode = GPConnectorModes.MicroBatch
      microBatch = new GreenplumMicroBatch(optionsFactory, queryId, schema, this)
    }
    logDebug(s"toMicroBatchStream callNo=${toBatchCallNo.get()}")
    microBatch
  }

  private class SqlThread() extends Runnable {
    def run(): Unit = {
      try {
        var (numActiveTasks,
        numCompletedTasks,
        numFailedTasks) = (0, 0, 0)
        var lastEpoch: Long = -1

        /** Batch loop */
        while (!Thread.currentThread().isInterrupted) {
          sqlThreadPass.set(sqlThreadPass.get() + 1)
          numCompletedTasks = rmiMaster.totalTasks
          numFailedTasks = rmiMaster.failedTasks
          logDebug(s" SqlThread pass ${sqlThreadPass.get()}:" +
            s" queryId=${queryId}, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")

          val settleStart = System.currentTimeMillis()
          epochStart.set(settleStart)
          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch(optionsFactory.networkTimeout) // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          settleMs.set(System.currentTimeMillis() - settleStart)
          planDone.set(false)

          /** Done executors initialization */
          numActiveTasks = rmiMaster.numActiveTasks

          if ((batchNo < 0) || (numActiveTasks == 0)) {
            logDebug(s" SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" queryId=${queryId}, done=${done.get()}, aborted=${aborted.get}, " +
              s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
            return
          }
          processing.set(true)
          val processingStart = System.currentTimeMillis()
          if (rmiMaster.getCurrentEpoch != lastEpoch) { // never repeat query for the same epoch, its due to slow executors settlement
            lastEpoch = rmiMaster.getCurrentEpoch
            val locationClause = new StringBuilder("")
            var i = 0
            rmiMaster.partUrls.foreach {
              gpfdistUrl => {
                if (i > 0) {
                  locationClause.append(", ")
                }
                locationClause.append(s"'$gpfdistUrl'")
                i += 1
              }
            }
            val externalTableName = s"ext${queryId}_${sqlThreadPass.get().toString}"
            logTrace(s" SqlThread pass ${sqlThreadPass.get()}: queryId=${queryId}, " +
              s"locationClause=(${locationClause.toString()})")
            try {
              var schemaOrPlaceholder = schema
              var distributedBy = distributedByClause
              if (schemaOrPlaceholder.isEmpty) {
                schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
                distributedBy = ""
              }
              dbConnectionGuard.synchronized {
                if (!useTempExtTables && GPClient.tableExists(dbConnection, externalTableName))
                  GPClient.executeStatement(dbConnection, s"DROP EXTERNAL TABLE $externalTableName")
              }
              val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.ExternalWritable)
              //if (columnsClause.isEmpty) columnsClause = s"dummy char(1)"
              val createExternalTable = s"CREATE WRITABLE EXTERNAL ${tempTableClause} TABLE $externalTableName" +
                //s" (LIKE ${table})" +
                s"($createColumnsClause)" +
                s" LOCATION (${locationClause.toString()})" +
                s" FORMAT 'TEXT' (" +
                //s"    DELIMITER '${fieldDelimiter}' " +
                s"    NULL 'NULL' " +
                //s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
                s"  ) " +
                s" ENCODING 'UTF8'" +
                s" $distributedBy"
              logTrace(s"createExternalTable: $createExternalTable")
              dbConnectionGuard.synchronized {
                GPClient.executeStatement(dbConnection, createExternalTable)
              }
              val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.None)
              val colListSelect = if (schema.nonEmpty) {
                SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.Target)
              } else "null"
              var insertSelectSql = ""
              if (optionsFactory.sqlTransfer.isEmpty) {
                insertSelectSql = s"insert into $externalTableName (${colListInsert}) "
                if (dbTableName.nonEmpty) {
                  insertSelectSql += s"select $colListSelect from $dbTableName"
                } else {
                  insertSelectSql += s"select $colListSelect from ($tableOrQuery) sqr"
                }
                if (whereClause.nonEmpty)
                  insertSelectSql += s" where $whereClause"
              } else {
                insertSelectSql = optionsFactory.sqlTransfer.replaceAll("(?i)<ext_table>", externalTableName).
                  replaceAll("(?i)<select_colList>", colListSelect).
                  replaceAll("(?i)<current_epoch>", rmiMaster.getCurrentEpoch.toString)
                if (whereClause.nonEmpty)
                  insertSelectSql = insertSelectSql.replaceAll("(?i)<pushed_filters>", whereClause)
              }
              if (connectorMode != GPConnectorModes.Batch) {
                insertSelectSql = insertSelectSql.
                  replaceAll("(?i)<start_offset_json>", startOffset.json()).
                  replaceAll("(?i)<end_offset_json>", endOffset.json())
              }
              logTrace(s"SQL: $insertSelectSql")
              try {
                val nRows = dbConnectionGuard.synchronized {
                  GPClient.executeStatement(dbConnection, insertSelectSql, optionsFactory.dbMessageLogLevel)
                }
                logDebug(s"\nSqlThread queryId=${queryId} '${insertSelectSql}'" +
                  s" nRows=${nRows}")
              } catch {
                case e: Exception => {
                  if (aborted.get()) {
                    logDebug(s"\nSqlThread queryId=${queryId} failed '${insertSelectSql}'" +
                      s", ${e.getClass.getCanonicalName} ${e.getMessage}")
                  } else {
                    logError(s"\nSqlThread queryId=${queryId} failed '${insertSelectSql}'" +
                      s", ${e.getClass.getCanonicalName} ${e.getMessage}")
                    throw e
                  }
                  numActiveTasks = 0
                }
              }
              logDebug(s"About to commit batch ${rmiMaster.batchNo.get()}")
              val commitStart = System.currentTimeMillis()
              groupCommitStart.set(System.currentTimeMillis())
              processingMs.set(commitStart - processingStart)
              rmiMaster.commitBatch(optionsFactory.networkTimeout)
              commitMs.set(System.currentTimeMillis() - commitStart)

              if (connectorMode == GPConnectorModes.Batch) {
                dbConnectionGuard.synchronized {
                  if (!dbConnection.getAutoCommit)
                    dbConnection.commit()
                }
              }
            } finally {
              dbConnectionGuard.synchronized {
                if (!useTempExtTables && GPClient.tableExists(dbConnection, externalTableName)) {
                  try {
                    GPClient.executeStatement(dbConnection, s"DROP EXTERNAL TABLE $externalTableName")
                  } catch {
                    case e: Exception => logError(s"${e.getMessage}")
                  }
                }
              }
            }
          } else {
            // extra partitions - dummy pass
            logDebug(s"About to commit dummy batch ${rmiMaster.batchNo.get()}")
            rmiMaster.commitBatch(optionsFactory.networkTimeout)
          }
          epochTotalMs.set(System.currentTimeMillis() - epochStart.get())
          processing.set(false)
          numActiveTasks = rmiMaster.numActiveTasks
          logDebug(s"\nSqlThread end of pass ${sqlThreadPass.get()}: " +
            s"queryId=${queryId}, partUrlsCount=${numActiveTasks}")

          /** Batch loop end */
        }

      } finally {
        try {
          logDebug(s"\nSqlThread terminated: pass ${sqlThreadPass.get()}:" +
            s" queryId=${queryId}")
          if (rmiMaster != null) {
            //rmiMaster.waitLeftPartsAndStop()
            rmiMaster.stop()
            rmiMaster = null
          }
          stop()
/*          dbConnectionGuard.synchronized {
            if (dbConnection != null) {
              if (done.get()) {
                if (!dbConnection.getAutoCommit)
                  dbConnection.commit()
              } else {
                if (!dbConnection.getAutoCommit)
                  dbConnection.rollback()
              }
              try {
                dbConnection.close()
                dbConnection = null
              } catch {
                case _: Throwable =>
              }
            }
          }
*/
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  //private var dbAbortThread: Executor = null
  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.of(2000L)
    override def numRows(): OptionalLong = OptionalLong.of(20L)
  }

  private def plainNumPartitions(): Int = {
    gpConnect()
    val numPartsPlan = numParts.synchronized {
      dbConnectionGuard.synchronized {
        numParts = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())
      }
      numParts
    }
    if (rmiMaster != null)
      rmiMaster.setGpSegmentsNum(numPartsPlan)
    logDebug(s"outputPartitioning().numPartitions() = ${numPartsPlan}")
    numPartsPlan
  }

  override def outputPartitioning(): Partitioning = new UnknownPartitioning(plainNumPartitions())

  def setOffsetRange(start: Option[Offset], end: Option[Offset]): Array[InputPartition] = {
    var newOffset: Boolean = false
    start.getOrElse(None) match {
      case offset: GPOffset =>
        if (startOffset.get() != offset.get()) {
          logDebug(s"setOffsetRange(start=${offset.get()})")
          startOffset.set(offset.get())
          newOffset = true
        }
      case _ =>
    }
    end.getOrElse(None) match {
      case offset: GPOffset =>
        if (endOffset.get() != offset.get()) {
          logDebug(s"setOffsetRange(end=${offset.get()})")
          endOffset.set(offset.get())
          newOffset = true
        }
      case _ =>
    }
    if ((newOffset && (endOffset.get() != startOffset.get())) || inputPartitions.isEmpty)
      return planInputPartitions()
    inputPartitions.toArray
  }

  private var lastBatchId: Int = -1
  private var lastOffsetGetTime: Long = 0

  def getEndOffset: Offset = {
    val batchId = if (rmiMaster == null) -1 else rmiMaster.batchNo.get()
    //sparkContext.getLocalProperty("streaming.sql.batchId")
    if (((startOffset.get() == endOffset.get()) ||
      batchId != lastBatchId
      ) && (System.currentTimeMillis() - lastOffsetGetTime > 100)) {
      gpConnect()
      dbConnectionGuard.synchronized {
        endOffset.extract(dbConnection, optionsFactory.offsetRestoreSql)
      }
      if (endOffsetPrev.get() != endOffset.get()) {
        logDebug(s"getEndOffset=${endOffsetPrev.get()} >> ${endOffset.get()}, batchId=${batchId}")
        planInputPartitions()
      }
      lastOffsetGetTime = System.currentTimeMillis()
      lastBatchId = batchId
      endOffsetPrev.set(endOffset.get())
    }
    GPOffset(endOffset.get())
  }

  def commit(end: Offset): Unit = {
    //val epochCommitMs = System.currentTimeMillis() - groupCommitStart.get()
    NetUtils().waitForCompletion(optionsFactory.networkTimeout) {!processing.get()}
    val offset = end match {
      case offset: GPOffset =>
        //startOffset.set(offset.get())
        logDebug(s"Commit: endOffset ${offset.get()}")
        offset
      case _ => null
    }
    dbConnectionGuard.synchronized {
      if (dbConnection != null) {
        if (offset != null)
          offset.store(dbConnection, optionsFactory.offsetStoreSql)
        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.close()
        dbConnection = null
      }
    }
  }

  def stop(): Unit = {
    try {
      dbConnectionGuard.synchronized {
        batch = null
        microBatch = null
        if (dbConnection != null) {
          if (connectorMode == GPConnectorModes.Batch) {
            logInfo(s"Batch reader stop")
          } else {
            logInfo(s"Streaming reader stop: endOffset=${endOffset.get()}")
          }
          if (!done.get()) {
            /*
              try {
                dbAbortThread = Executors.newFixedThreadPool(1)
                dbConnection.abort(dbAbortThread)
              } catch {
                case _: Throwable =>
              }
            */
            if (!dbConnection.getAutoCommit)
              dbConnection.rollback()
            done.set(true)
          } else {
            if (!dbConnection.getAutoCommit)
              dbConnection.commit()
          }
          dbConnection.close()
          dbConnection = null
          rowSet.clear()
        }
      }
    } catch {
      case _: Throwable =>
    }
  }
}

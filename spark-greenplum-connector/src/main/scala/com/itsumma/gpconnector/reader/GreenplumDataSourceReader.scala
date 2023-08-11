package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.rmi.{GPConnectorModes, RMIMaster}

import java.sql.Connection
import java.util
import java.util.{Optional, OptionalLong, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import com.itsumma.gpconnector.{GPClient, GPOffset}
import com.typesafe.scalalogging.Logger
//import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.streaming.MicroBatchExecution
import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil.guessMaxParallelTasks
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
//import org.apache.spark.annotation.DeveloperApi
//import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, GPTarget, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.util.concurrent.{Executor, Executors}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

// scalafix:off DisableSyntax.null
// scalastyle:off null

class GreenplumDataSourceReader(optionsFactory: GPOptionsFactory,
                                usersSchema: Option[StructType] = None,
                                checkpointLocation: Option[String] = None
                               )
  extends MicroBatchReader
    with SupportsReportPartitioning
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with SupportsReportStatistics {
  private val dbConnection: Connection = GPClient.getConn(optionsFactory)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
  if (checkpointLocation.isEmpty || optionsFactory.readStreamAutoCommit) {
    dbConnection.setAutoCommit(true)
  } else {
    dbConnection.setAutoCommit(false)
  }
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val streamStop = new AtomicBoolean(checkpointLocation.isEmpty)
  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  private val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  private val tableOrQuery: String = optionsFactory.tableOrQuery
  if (tableOrQuery.isEmpty && usersSchema.isEmpty)
    throw new IllegalArgumentException(s"Must pass a source table name or sql query string in the dbtable option")
  private val targetDetails: GPTarget = GPTarget(tableOrQuery)
  private val dbDefaultSchemaName: String = GPClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
  private val dbTableName: String = targetDetails.getCanonicalName(dbDefaultSchemaName)
  private val sqlThreadPass = new AtomicLong(0)
  private val startOffset: GPOffset = GPOffset()
  startOffset.extract(dbConnection, optionsFactory.offsetRestoreSql)
  private val endOffset: GPOffset = GPOffset(startOffset.get())
  private val endOffsetPrev: GPOffset = endOffset.copy()
  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-read$queryId")
  sqlThread.setDaemon(true)
  var whereClause = ""
  private var pushedDownFilters: Array[Filter] = new Array[Filter](0)
  private var schema = usersSchema.getOrElse(
    SparkSchemaUtil.getGreenplumTableSchema(optionsFactory, dbConnection, tableOrQuery))
  private var numParts: Int = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())
  private val mbExecutor: MicroBatchExecution = null

  private val segmentLocations: Map[String, Set[String]] = {
    import com.itsumma.gpconnector.rmi.NetUtils
    val hosts = GPClient.nodeNamesWithSegments(dbConnection)
    val resolvedHosts = hosts.map({case (host, segSet) => NetUtils().resolveHost2Ip(host) -> segSet})
    hosts ++ resolvedHosts
  }
  private var rmiMaster: RMIMaster = null
  private val gpVersion: String = GPClient.queryGPVersion(dbConnection)
  private val useTempExtTables: Boolean = optionsFactory.useTempExtTables &&
    (GPClient.versionCompare(gpVersion, "5.0.0") >= 0)
  private val tempTableClause: String = if (useTempExtTables) "TEMP" else ""
  private val distributionPol: String = GPClient.getTableDistributionPolicy(dbConnection,
    targetDetails.getTableSchema(dbDefaultSchemaName),
    targetDetails.cleanDbTableName)
  private val (distributedByClause: String, _: String) = GPClient.formatDistributedByClause(
    if (optionsFactory.distributedBy.nonEmpty) optionsFactory.distributedBy
    else distributionPol
  )
  logger.info(s"distributionPol=$distributionPol,\n" +
    s"tblSchema=${targetDetails.getTableSchema(dbDefaultSchemaName)},\n" +
    s"distributedByClause=$distributedByClause,\n" +
    s"checkpointLocation=$checkpointLocation")

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val inputPartitions = ListBuffer[InputPartition[InternalRow]]()
    val numPartsPlan = numParts.synchronized {
      numParts = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())
      numParts
    }
    if (rmiMaster == null) {
      rmiMaster = new RMIMaster(optionsFactory, queryId, numPartsPlan, done, aborted, segmentLocations,
        if (checkpointLocation.isEmpty) GPConnectorModes.Batch else GPConnectorModes.MicroBatch)
    } else {
      rmiMaster.setGpSegmentsNum(numPartsPlan)
    }
    for (i <- 0 until numPartsPlan) {
      val locations: Array[String] = segmentLocations.filter(node => node._2.contains(i.toString)).keySet.toArray
      val inpPart =
        new GreenplumInputPartition(optionsFactory, queryId, schema, i, rmiMaster.rmiRegistryAddress, locations)
      logger.info(s"Scheduling partition $i for epoch ${rmiMaster.getCurrentEpoch} " +
        s"with locations=${locations.mkString("Array(", ", ", ")")}")
      inputPartitions += inpPart
    }
    logger.debug(s"Creating ${inputPartitions.size} partitions for queryId=${queryId}")
    if (!sqlThread.isAlive)
      sqlThread.start()
    inputPartitions
  }

  override def pruneColumns(structType: StructType): Unit = {
    schema = structType
/*
    if (structType.nonEmpty) {
      schema = structType
    } else {
      schema = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
    }
*/
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val tuple3 = SparkSchemaUtil(optionsFactory.dbTimezone).pushFilters(filters)
    whereClause = tuple3._1
    pushedDownFilters = tuple3._3
    tuple3._2
  }

  override def pushedFilters: Array[Filter] = pushedDownFilters

  private class SqlThread(private val conn: Connection) extends Runnable {
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
          logger.debug(s" SqlThread pass ${sqlThreadPass.get()}:" +
            s" queryId=${queryId}, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")

          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch(optionsFactory.networkTimeout) // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          /** Done executors initialization */
          numActiveTasks = rmiMaster.numActiveTasks

          //if (streamStop.get() && ((batchNo < 0) || (numActiveTasks == 0))) {
          if ((batchNo < 0) || (numActiveTasks == 0)) {
            logger.debug(s" SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" queryId=${queryId}, " +
              s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
            return
          }
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
            val externalTableName: String = s"ext${queryId}_${sqlThreadPass.get().toString}"
            logger.debug(s" SqlThread pass ${sqlThreadPass.get()}: queryId=${queryId}, " +
              s"locationClause=(${locationClause.toString()})")
            try {
              var schemaOrPlaceholder = schema
              var distributedBy = distributedByClause
              if (schemaOrPlaceholder.isEmpty) {
                schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
                distributedBy = ""
              }
              dbConnection.synchronized {
                if (!useTempExtTables && GPClient.tableExists(conn, externalTableName))
                  GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
              }
              val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.ExternalWritable)
              //if (columnsClause.isEmpty) columnsClause = s"dummy char(1)"
              val createExternalTable = s"CREATE WRITABLE EXTERNAL ${tempTableClause} TABLE $externalTableName" +
                //s" (LIKE ${table})" +
                s" ($createColumnsClause)" +
                s" LOCATION (${locationClause.toString()})" +
                s" FORMAT 'TEXT' (" +
                //s"    DELIMITER '${fieldDelimiter}' " +
                s"    NULL 'NULL' " +
                //s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
                s"  ) " +
                s" ENCODING 'UTF8'" +
                s" $distributedBy"
              logger.info(s"createExternalTable: $createExternalTable")
              dbConnection.synchronized {
                GPClient.executeStatement(conn, createExternalTable)
              }
              val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.None)
              val colListSelect = if (schema.nonEmpty) {
                SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.Target)
              } else "null"
              var insertSelectSql: String = ""
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
              if (checkpointLocation.nonEmpty) {
                insertSelectSql = insertSelectSql.
                  replaceAll("(?i)<start_offset_json>", startOffset.json()).
                  replaceAll("(?i)<end_offset_json>", endOffset.json())
              }
              logger.debug(s"SQL: $insertSelectSql")
              try {
                val nRows = dbConnection.synchronized {
                  GPClient.executeStatement(conn, insertSelectSql, optionsFactory.dbMessageLogLevel)
                }
                logger.info(s"\nSqlThread queryId=${queryId} '${insertSelectSql}'" +
                  s" nRows=${nRows}")
              } catch {
                case e: Exception => {
                  if (aborted.get()) {
                    logger.info(s"\nSqlThread queryId=${queryId} failed '${insertSelectSql}'" +
                      s", ${e.getClass.getCanonicalName} ${e.getMessage}")
                  } else {
                    logger.error(s"\nSqlThread queryId=${queryId} failed '${insertSelectSql}'" +
                      s", ${e.getClass.getCanonicalName} ${e.getMessage}")
                    throw e
                  }
                  numActiveTasks = 0
                }
              }
              logger.info(s"About to commit batch ${rmiMaster.batchNo.get()}")
              rmiMaster.commitBatch(optionsFactory.networkTimeout)
              if (checkpointLocation.isEmpty) {
                dbConnection.synchronized {
                  if (!conn.getAutoCommit)
                    conn.commit()
                }
              }
            } finally {
              dbConnection.synchronized {
                if (!useTempExtTables && GPClient.tableExists(conn, externalTableName)) {
                  try {
                    GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
                  } catch {
                    case e: Exception => logger.error(s"${e.getMessage}")
                  }
                }
              }
            }
          } else {
            // extra partitions - dummy pass
            logger.info(s"About to commit batch ${rmiMaster.batchNo.get()}")
            rmiMaster.commitBatch(optionsFactory.networkTimeout)
          }
          numActiveTasks = rmiMaster.numActiveTasks
          logger.debug(s"\nSqlThread end of pass ${sqlThreadPass.get()}: " +
            s"queryId=${queryId}, partUrlsCount=${numActiveTasks}")

          /** Batch loop end*/
        }

      } finally {
        try {
          logger.debug(s"\nSqlThread terminated: pass ${sqlThreadPass.get()}:" +
            s" queryId=${queryId}")
          if (rmiMaster != null) {
            //rmiMaster.waitLeftPartsAndStop()
            rmiMaster.stop()
            rmiMaster = null
          }
          if (done.get()) {
            if (!conn.getAutoCommit)
              conn.commit()
          } else {
            //System.exit(1)
            try {
              conn.abort(dbAbortThread)
            } catch {
              case _: Throwable =>
            }
            if (!conn.getAutoCommit)
              conn.rollback()
            ///TODO: do we need that?
            //sparkContext.cancelAllJobs()
          }
          //sparkContext.clearJobGroup()
          try {
            conn.close()
          } catch {
            case _: Throwable =>
          }
        } catch {
          case _: Throwable =>
        }
      }
    }
  }
  private val dbAbortThread: Executor = Executors.newFixedThreadPool(1)

  override def outputPartitioning(): Partitioning = new Partitioning {
    override def numPartitions(): Int = {
      val numPartsPlan = numParts.synchronized {
        numParts = math.min(GPClient.queryNSegments(dbConnection), guessMaxParallelTasks())
        numParts
      }
      if (rmiMaster != null)
        rmiMaster.setGpSegmentsNum(numPartsPlan)
      logger.info(s"outputPartitioning().numPartitions() = ${numPartsPlan}")
      numPartsPlan
    }

    override def satisfy(distribution: Distribution): Boolean = true
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.of(123456789000L)

    override def numRows(): OptionalLong = OptionalLong.of(123456789000L)
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    if (start.isPresent) {
      start.get() match {
        case offset: GPOffset =>
            if (startOffset.get() != offset.get()) {
              logger.info(s"setOffsetRange(start=${offset.get()})")
              startOffset.set(offset.get())
            }
        case _ =>
      }
    }
    if (end.isPresent) {
      end.get() match {
        case offset: GPOffset =>
          if (endOffset.get() != offset.get()) {
            logger.info(s"setOffsetRange(end=${offset.get()})")
            endOffset.set(offset.get())
          }
        case _ =>
      }
    }
  }

  override def getStartOffset: Offset = {
    logger.info(s"getStartOffset=${startOffset.get()}")
    GPOffset(startOffset.get())
  }

  private var lastBatchId: Int = -1
  private var lastOffsetGetTime: Long = 0

  override def getEndOffset: Offset = {
    val batchId = if (rmiMaster == null) -1 else rmiMaster.batchNo.get()
      //sparkContext.getLocalProperty("streaming.sql.batchId")
    if (((startOffset.get() == endOffset.get()) ||
      batchId != lastBatchId
    ) && (System.currentTimeMillis() - lastOffsetGetTime > 100)) {
        dbConnection.synchronized {
          endOffset.extract(dbConnection, optionsFactory.offsetRestoreSql)
        }
      if (endOffsetPrev.get() != endOffset.get())
        logger.info(s"getEndOffset=${endOffsetPrev.get()} >> ${endOffset.get()}, batchId=${batchId}")
      lastOffsetGetTime = System.currentTimeMillis()
      lastBatchId = batchId
      endOffsetPrev.set(endOffset.get())
    }
    GPOffset(endOffset.get())
  }

  override def deserializeOffset(json: String): Offset = GPOffset(Option(json))

  override def commit(end: Offset): Unit = {
    end match {
      case offset: GPOffset =>
        startOffset.set(offset.get())
        logger.info(s"Commit: endOffset ${offset.get()} => new startOffset}")
      case _ =>
        startOffset.set(Option(end.json()))
        logger.info(s"Commit_: endOffset ${end.json()} => new startOffset}")
    }
    dbConnection.synchronized {
      startOffset.store(dbConnection, optionsFactory.offsetStoreSql)
      if (!dbConnection.getAutoCommit)
        dbConnection.commit()
    }
  }

  override def stop(): Unit = {
    done.set(true)
    streamStop.set(true)
    logger.info(s"CStreaming stop: endOffset=${endOffset.get()}")
  }
}

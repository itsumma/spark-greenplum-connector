package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.rmi.{GPConnectorModes, RMIMaster}
import com.itsumma.gpconnector.GPClient
import com.typesafe.scalalogging.Logger
import com.itsumma.gpconnector.rmi.NetUtils
//import org.apache.spark.TaskContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPColumnMeta, GPOptionsFactory, GPTarget, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

//import java.util.concurrent.Executor
//import java.util.concurrent.Executors
import java.sql.Connection
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable.ListBuffer

class GreenplumDataSourceWriter(writeUUID: String,
                                schema: StructType,
                                saveMode: SaveMode,
                                optionsFactory: GPOptionsFactory,
                                gpClient: GPClient,
                                streamingOutputMode: Option[OutputMode] = None
                               )
  extends StreamWriter {
  private val epochProcessingStart = new AtomicLong(System.currentTimeMillis())
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val dbConnection: Connection = gpClient.getConnection()
  private val gpVersion: String = GPClient.queryGPVersion(dbConnection)
  private val nGpSegments = GPClient.queryNSegments(dbConnection)
  private val dbDefaultSchemaName: String = GPClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
  if (!dbConnection.getAutoCommit)
    dbConnection.commit()
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
  dbConnection.setAutoCommit(false)
  private val dbTableName: String = optionsFactory.tableOrQuery
  private val targetDetails: GPTarget = GPTarget(dbTableName)
  private val targetTableCanonicalName: String = targetDetails.getCanonicalName(dbDefaultSchemaName)
  private val tableDbSchema = targetDetails.getTableSchema(dbDefaultSchemaName)
  private val sqlThreadPass = new AtomicInteger(0)
//  private val fieldDelimiter = "\t"
  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  private val processing = new AtomicBoolean(false)
  private var rmiMaster: RMIMaster = null


  private var gpTableMeta: Map[String, GPColumnMeta] = null
  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-write$writeUUID")
  sqlThread.setDaemon(true)

  private val numPartFactories: AtomicLong = new AtomicLong(0)
  //private val dbAbortThread: Executor = Executors.newFixedThreadPool(1)

  private val useTempExtTables: Boolean = optionsFactory.useTempExtTables &&
    (GPClient.versionCompare(gpVersion, "5.0.0") >= 0)
  private val tempTableClause: String = if (useTempExtTables) "TEMP" else ""
  private var totalTasks: Int = 0
  private val initMs = new AtomicLong(0)
  private val settleMs = new AtomicLong(0)
  private val commitMs = new AtomicLong(0)
  private val groupCommitStart = new AtomicLong(0)

  logger.debug(s"\nPassed in properties = ${optionsFactory.dumpParams()}")

  private var ignoreInsert: Boolean = false
  if (targetTableCanonicalName.nonEmpty) {
    if (!GPClient.tableExists(dbConnection, targetTableCanonicalName)) {
      val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target)
      var createTable = s"create table ${targetTableCanonicalName} ($createColumnsClause)"
      if (optionsFactory.distributedBy.nonEmpty) {
        createTable += GPClient.formatDistributedByClause(optionsFactory.distributedBy)
      }
      if (optionsFactory.partitionClause.nonEmpty) {
        createTable += s" (${optionsFactory.partitionClause})"
      }
      GPClient.executeStatement(dbConnection, createTable)
      gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
    } else {
      gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
      if (saveMode == SaveMode.ErrorIfExists) {
        throw new Exception(s"table ${targetTableCanonicalName} exists and saveMode=ErrorIfExists")
      } else if (saveMode == SaveMode.Ignore) {
        ignoreInsert = true
      } else if (saveMode == SaveMode.Overwrite) {
        logger.debug(s"saveMode = overwrite, truncate = ${optionsFactory.truncate}")
        if (optionsFactory.truncate) {
          GPClient.executeStatement(dbConnection, s"truncate table ${targetTableCanonicalName}")
        } else {
          val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target, gpTableMeta)
          GPClient.executeStatement(dbConnection, s"drop table ${targetTableCanonicalName}")
          var createTable = s"create table ${targetTableCanonicalName} ($createColumnsClause)"
          if (optionsFactory.distributedBy.nonEmpty) {
            createTable += GPClient.formatDistributedByClause(optionsFactory.distributedBy)
          }
          if (optionsFactory.partitionClause.nonEmpty) {
            createTable += s" (${optionsFactory.partitionClause})"
          }
          GPClient.executeStatement(dbConnection, createTable)
        }
      } else {
        logger.debug(s"saveMode = append")
      }
    }
    if (!dbConnection.getAutoCommit)
      dbConnection.commit()
  } else if (optionsFactory.sqlTransfer.isEmpty) {
    throw new IllegalArgumentException(s"Must specify either dbtable or sqltransfer option, or both")
  }
  if (schema.isEmpty)
    logger.warn(s"Spark schema is empty for writeUUID=$writeUUID")
  private val extTblCreateColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.None)
  private val colListSelect = SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val segmentLocations: Map[String, Set[String]] = {
    val hosts = GPClient.nodeNamesWithSegments(dbConnection)
    val resolvedHosts = hosts.map({ case (host, segSet) => NetUtils().resolveHost2Ip(host) -> segSet })
    hosts ++ resolvedHosts
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    var factory: GreenplumDataWriterFactory = null
    if (done.get() || aborted.get()) {
      val msg = s"An attempt to revive an already finished action ${writeUUID}"
      logger.warn(msg)
      throw new Exception(msg)
    }
    if (ignoreInsert) {
      logger.debug(s"" +
        s"createWriterFactory returns null due to saveMode=Ignore and table ${targetTableCanonicalName} exists" +
        s"writeUUID=${writeUUID}")
      return null
    }
    var nFact: Long = 0
    nFact = numPartFactories.incrementAndGet()
    logger.debug(s"createWriterFactory " +
      s"writeUUID=${writeUUID} numGpSegments=$nGpSegments numPartFactories=${nFact}")
    if (nFact == 1) {
      logger.debug(s"SqlThread(${sqlThread.getName}) start")
      rmiMaster = new RMIMaster(optionsFactory, writeUUID, nGpSegments, done, aborted, segmentLocations,
        if (streamingOutputMode.nonEmpty) GPConnectorModes.MicroBatch else GPConnectorModes.Batch,
        false
      )
      sqlThread.start()
    }
    if (rmiMaster != null)
      factory = new GreenplumDataWriterFactory(writeUUID, schema, optionsFactory, rmiMaster.rmiRegistryAddress)
    factory
  }

  private class SqlThread(private val conn: Connection) extends Runnable {
    override def run(): Unit = {
      try {
        conn.setAutoCommit(false)
        var numActiveTasks = 0
        initMs.set(System.currentTimeMillis() - epochProcessingStart.get())

        /** Batch loop */
        while (!done.get() && !aborted.get() && !Thread.currentThread().isInterrupted) {

          /** Start batch pass */
          sqlThreadPass.incrementAndGet()
          val settleStart = System.currentTimeMillis()
          groupCommitStart.set(settleStart) // the last pass will start to wait for the epoch commit done here

          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch(optionsFactory.networkTimeout) // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          /** Done executors initialization */
          settleMs.addAndGet(System.currentTimeMillis() - settleStart)
          numActiveTasks = rmiMaster.numActiveTasks

          if ((batchNo < 0) || (numActiveTasks == 0)) {
            logger.debug(s"\nSqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" writeUUID=${writeUUID}, " +
              s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
              s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
            )
            logger.debug(s"Processed ${totalTasks} tasks in ${sqlThreadPass.get()-1} passes")
            return
          }
          processing.set(true)
          logger.debug(s"\nSqlThread pass ${sqlThreadPass.get()}:" +
            s" writeUUID=${writeUUID}, " +
            s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
            s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
          )
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

          val externalTableName: String = s"ext${SparkSchemaUtil.stripChars(writeUUID, "-")}_${batchNo}"
          try {
            try {
              //if (!useTempExtTables)
              dbConnection.synchronized {
                GPClient.executeStatement(conn, s"drop external table if exists ${externalTableName}")
              }
            } catch {
              case e: Exception => logger.error(s"${e.getMessage}")
            }
            val createExtTbl = s"CREATE READABLE EXTERNAL ${tempTableClause} TABLE ${externalTableName}" +
              //s" (LIKE ${table})" +
              s"($extTblCreateColumnsClause)" +
              s" LOCATION (${locationClause.toString()})" +
              s" FORMAT 'TEXT' (" +
              //s"    DELIMITER '${fieldDelimiter}' " +
              s"    NULL 'NULL' " +
              s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
              s"  ) " +
              s" ENCODING 'UTF8'"
            logger.debug(s"\n$createExtTbl")
            dbConnection.synchronized {
              GPClient.executeStatement(conn, createExtTbl)
            }
            var insertSelectSql = ""
            if (optionsFactory.sqlTransfer.isEmpty) {
              insertSelectSql = s"insert into ${targetTableCanonicalName} (${colListInsert}) " +
                s"select ${colListSelect} from ${externalTableName}"
            } else {
              insertSelectSql = optionsFactory.sqlTransfer.
                replaceAll("(?i)<ext_table>", externalTableName).
                replaceAll("(?i)<current_epoch>", rmiMaster.getCurrentEpoch.toString)
              if (streamingOutputMode.nonEmpty) {
                insertSelectSql = insertSelectSql.replaceAll("(?i)<stream_mode>",
                  s"${streamingOutputMode.get}")
              }
            }
            logger.debug(s"SQL: ${insertSelectSql}")

            try {
              val nRows = dbConnection.synchronized {
                GPClient.executeStatement(conn, insertSelectSql, optionsFactory.dbMessageLogLevel)
              }
              logger.debug(s"\nSqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                s" nRows=${nRows}")
            } catch {
              case e: Exception => {
                logger.error(s"\nSqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                  s" failed, ${e.getMessage}")
                //if (!aborted.get())
                throw e
              }
            }
          } finally {
            try {
              //if (!useTempExtTables)
              dbConnection.synchronized {
                GPClient.executeStatement(conn, s"drop external table if exists $externalTableName")
              }
            } catch {
              case e: Exception => logger.error(s"${e.getMessage}")
            }
          }

          /** Wait for all active dataWriter commits */
          val commitStart = System.currentTimeMillis()
          rmiMaster.commitBatch(optionsFactory.networkTimeout)
          processing.set(false)
          commitMs.addAndGet(System.currentTimeMillis() - commitStart)

          logger.debug(s"\nSqlThread end of pass ${sqlThreadPass.get()}: " +
            s"writeUUID=${writeUUID}, " +
            s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
            s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
          )
          totalTasks += numActiveTasks
          /** Done batch pass */
        }
        /** Done batch loop */
      } finally {
        /** Wait for final abort or commit */
        val completeOk = waitJobComplete()
        try {
          logger.debug(s"\nSqlThread terminated: complete=${completeOk}, pass ${sqlThreadPass.get()}:" +
            s" writeUUID=${writeUUID}")
          if (rmiMaster != null)
            rmiMaster.stop()
          rmiMaster = null
          if (!done.get()) {
/*
            try {
              conn.abort(dbAbortThread)
            } catch {
              case _: Throwable =>
            }
*/
            if (!conn.getAutoCommit)
              conn.rollback()
          }
          try {
            conn.close()
          } catch {
            case _: Throwable =>
          }
        } catch {
          case _: Throwable =>
        }
        if (!completeOk)
          throw new Exception((s"writeUUID=${writeUUID} timeout on waiting job completion"))
      }
    }
  }

  private def waitJobComplete(msMax: Int = 60000): Boolean = {
    NetUtils().waitForCompletion(msMax) {done.get() || aborted.get()}
  }

  override def useCommitCoordinator: Boolean = false

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case msg: GreenplumWriterCommitMessage if msg.writeUUID.equals(writeUUID) =>
        logger.debug(s"\nonDataWriterCommit writeUUID=${writeUUID}, " +
          s"instanceId=${msg.instanceId}, node=${msg.gpfdistUrl}, " +
          s"epochId=${msg.epochId}, partitionId=${msg.partitionId}, taskId=${msg.taskId}, " +
          s"nRowsWritten=${msg.nRowsWritten}, rmiPutMs=${msg.rmiPutMs}, rmiGetMs=${msg.rmiGetMs}")
      case _ =>
        logger.debug(s"\nwriteUUID=${writeUUID} onDataWriterCommit ${message.toString}")
    }
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val streamingBatchId = { /*TaskContext.get.getLocalProperty("streaming.sql.batchId") !!! for executors only ! */
      if (writerCommitMessages.length > 0) {
        writerCommitMessages(0) match {
          case part: GreenplumWriterCommitMessage => part.epochId
          case _ => 0
        }
      } else 0
    }
    commit(streamingBatchId, writerCommitMessages)
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val streamingBatchId = {
      /*TaskContext.get.getLocalProperty("streaming.sql.batchId") !!! for executors only ! */
      if (writerCommitMessages.length > 0) {
        writerCommitMessages(0) match {
          case part: GreenplumWriterCommitMessage => part.epochId
          case _ => 0
        }
      } else 0
    }
    abort(streamingBatchId.toInt, writerCommitMessages)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    NetUtils().waitForCompletion(optionsFactory.networkTimeout) {!processing.get()}
    dbConnection.synchronized {
      dbConnection.commit()
    }
    logger.debug(s"Commit epoch ${epochId}, batch=${rmiMaster.batchNo.get()}, " +
      s"messages=\n${messages.mkString("{", "\n", "}")}")
    var (epoch: Long, nParts: Int, nRows: Long) = (0L, 0, 0L)
    val writeMs: ListBuffer[Long] = new ListBuffer[Long]()
    val readMs: ListBuffer[Long] = new ListBuffer[Long]()
    messages.foreach {
      case part: GreenplumWriterCommitMessage =>
        epoch = part.epochId
        nParts += 1
        nRows += part.nRowsWritten
        writeMs += part.rmiPutMs
        readMs += part.rmiGetMs
    }
    val now = System.currentTimeMillis()
    val processingMs = now - epochProcessingStart.get()
    val writeMsAvg = if (nParts > 0) writeMs.sum.toDouble / nParts.toDouble else 0.0
    val readMsAvg = if (nParts > 0) readMs.sum.toDouble / nParts.toDouble else 0.0
    logger.info(s"Epoch $epoch written $nRows rows in ${sqlThreadPass.get() - 1} passes and $nParts parts, " +
      s"processingMs=$processingMs, buffPutMs/part=$writeMsAvg, buffGetMs/part=$readMsAvg, " +
      s"initMs=${initMs.get()}, settleMs=${settleMs.get()}, commitMs=${commitMs.get()}+${now - groupCommitStart.get()}")
    done.set(true)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"Abort epoch ${epochId}, batch=${sqlThreadPass.get()-1}, " +
      s"messages=\n${messages.mkString("{", "\n", "}")}")
    try {
      dbConnection.synchronized {
        dbConnection.rollback()
        if (optionsFactory.undoSideEffectsSQL.nonEmpty) {
          GPClient.executeStatement(dbConnection, optionsFactory.undoSideEffectsSQL)
        }
      }
    } catch {
      case e: Exception => logger.warn(s"On abort epoch ${epochId}: ${e}")
    }
    aborted.set(true)
  }
}

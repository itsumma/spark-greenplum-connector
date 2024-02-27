package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.GPClient
import com.itsumma.gpconnector.rmi.GPConnectorModes.GPConnectorMode
import com.itsumma.gpconnector.rmi.{GPConnectorModes, NetUtils, RMIMaster}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.itsumma.gpconnector.{GPColumnMeta, GPOptionsFactory, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType

import java.sql.Connection
//import java.util.concurrent.{Executor, Executors}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable.ListBuffer

class GreenplumBatchWrite(writeUUID: String,
                          optionsFactory: GPOptionsFactory,
                          schema: StructType,
                          gpTableMeta: Map[String, GPColumnMeta],
                          targetTableCanonicalName: String,
                          gpClient: GPClient,
                          connectorMode: GPConnectorMode = GPConnectorModes.Batch,
                          ignoreInsert: Boolean = false
                         )
  extends BatchWrite
  with StreamingWrite
  with Logging
{
  private val epochProcessingStart = new AtomicLong(System.currentTimeMillis())

  private val dbConnectionGuard = new Object()
  private var dbConnection: Connection = null
  private var rmiMaster: RMIMaster = null
  private var sqlThread: Thread = null

  private val sqlThreadPass = new AtomicInteger(0)
  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  private val processing = new AtomicBoolean(false)
  private val numPartFactories: AtomicLong = new AtomicLong(0)
  private var totalTasks: Int = 0
  private val initMs = new AtomicLong(0)
  private val settleMs = new AtomicLong(0)
  private val commitMs = new AtomicLong(0)
  private val groupCommitStart = new AtomicLong(0)

  private def createWriterFactory(info: PhysicalWriteInfo): GreenplumDataWriterFactory = {
    var factory: GreenplumDataWriterFactory = null
    if (done.get() || aborted.get()) {
      val msg = s"An attempt to revive an already finished action ${writeUUID}"
      logWarning(msg)
      throw new Exception(msg)
    }
    if (ignoreInsert) {
      logTrace(s"" +
        s"createWriterFactory returns null due to saveMode=Ignore and table ${targetTableCanonicalName} exists" +
        s"writeUUID=${writeUUID}")
      return null
    }
    val nFact: Long = numPartFactories.incrementAndGet()
    if (nFact == 1) {
      dbConnection = gpClient.getConnection()
      val nGpSegments = GPClient.queryNSegments(dbConnection)
      val segmentLocations: Map[String, Set[String]] = {
        val hosts = GPClient.nodeNamesWithSegments(dbConnection)
        val resolvedHosts = hosts.map({ case (host, segSet) => NetUtils().resolveHost2Ip(host) -> segSet })
        hosts ++ resolvedHosts
      }
      logTrace(s"createWriterFactory " +
        s"writeUUID=${writeUUID}, numGpSegments=$nGpSegments")
      rmiMaster = new RMIMaster(optionsFactory, writeUUID, nGpSegments, done, aborted, segmentLocations,
        connectorMode,
        false
      )
      sqlThread = new Thread(new SqlThread(dbConnection), s"gpfdist-write$writeUUID")
      sqlThread.setDaemon(true)
      sqlThread.start()
    }
    if (rmiMaster != null)
      factory = new GreenplumDataWriterFactory(writeUUID, schema, optionsFactory, rmiMaster.rmiRegistryAddress)
    factory
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    createWriterFactory(info)
  }
  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    createWriterFactory(info)
  }

  private class SqlThread(private val conn: Connection) extends Runnable {
    override def run(): Unit = {
      try {
        if (schema.isEmpty)
          logWarning(s"Spark schema is empty for writeUUID=$writeUUID")
        val extTblCreateColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
        val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.None)
        val colListSelect = SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)

        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        dbConnection.setAutoCommit(false)
        val gpVersion: String = GPClient.queryGPVersion(dbConnection)
        val useTempExtTables: Boolean = optionsFactory.useTempExtTables &&
          (GPClient.versionCompare(gpVersion, "5.0.0") >= 0)
        val tempTableClause: String = if (useTempExtTables) "TEMP" else ""
        var numActiveTasks = 0
        initMs.set(System.currentTimeMillis() - epochProcessingStart.get())
        logDebug(s"SqlThread started for writeUUID=${writeUUID}")

        /** Batch loop */
        while (!done.get() && !aborted.get() && !Thread.currentThread().isInterrupted) {

          /** Start batch pass */
          sqlThreadPass.incrementAndGet()
          val settleStart = System.currentTimeMillis()

          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch(optionsFactory.networkTimeout) // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          /** Done executors initialization */
          settleMs.addAndGet(System.currentTimeMillis() - settleStart)
          numActiveTasks = rmiMaster.numActiveTasks

          //if ((batchNo < 0) || (numActiveTasks == 0))
          if (batchNo < 0) /*i.e. commit or abort is complete*/ {
            logTrace(s"\nSqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" writeUUID=${writeUUID}, " +
              s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
              s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
            )
            logTrace(s"Processed ${totalTasks} tasks in ${sqlThreadPass.get()-1} passes")
            return
          }
          if (numActiveTasks == 0) {
            logTrace(s"\nSqlThread dummy pass ${sqlThreadPass.get()}:" +
              s" writeUUID=${writeUUID}, " +
              s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
              s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
            )
          } else {
            processing.set(true)
            logTrace(s"\nSqlThread pass ${sqlThreadPass.get()}:" +
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
                dbConnectionGuard.synchronized {
                  GPClient.executeStatement(dbConnection, s"drop external table if exists ${externalTableName}")
                }
              } catch {
                case e: Exception => logError(s"${e.getMessage}")
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
              logDebug(s"\n$createExtTbl")
              dbConnectionGuard.synchronized {
                GPClient.executeStatement(dbConnection, createExtTbl)
              }
              var insertSelectSql = ""
              if (optionsFactory.sqlTransfer.isEmpty) {
                insertSelectSql = s"insert into ${targetTableCanonicalName} (${colListInsert}) " +
                  s"select ${colListSelect} from ${externalTableName}"
              } else {
                insertSelectSql = optionsFactory.sqlTransfer.
                  replaceAll("(?i)<ext_table>", externalTableName).
                  replaceAll("(?i)<current_epoch>", rmiMaster.getCurrentEpoch.toString)
                /*
                              if (streamingOutputMode.nonEmpty) {
                                insertSelectSql = insertSelectSql.replaceAll("(?i)<stream_mode>",
                                  s"${streamingOutputMode.get}")
                              }
                */
              }
              logTrace(s"SQL: ${insertSelectSql}")

              try {
                val nRows = dbConnectionGuard.synchronized {
                  GPClient.executeStatement(dbConnection, insertSelectSql, optionsFactory.dbMessageLogLevel)
                }
                logTrace(s"\nSqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                  s" nRows=${nRows}")
              } catch {
                case e: Exception => {
                  logError(s"\nSqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                    s" failed, ${e.getMessage}")
                  //if (!aborted.get())
                  throw e
                }
              }
            } finally {
              try {
                //if (!useTempExtTables)
                dbConnectionGuard.synchronized {
                  GPClient.executeStatement(dbConnection, s"drop external table if exists $externalTableName")
                }
              } catch {
                case e: Exception => logError(s"${e.getMessage}")
              }
            }

            /** Wait for all active dataWriter commits */
            val commitStart = System.currentTimeMillis()
            rmiMaster.commitBatch(optionsFactory.networkTimeout)
            groupCommitStart.set(System.currentTimeMillis()) // the last pass will start to wait for the epoch commit done here
            processing.set(false)
            commitMs.addAndGet(System.currentTimeMillis() - commitStart)

            logTrace(s"\nSqlThread end of pass ${sqlThreadPass.get()}: " +
              s"writeUUID=${writeUUID}, " +
              s"numTasks=${rmiMaster.totalTasks}, numActiveTasks=${numActiveTasks}, " +
              s"numCompletedTasks=${rmiMaster.successTasks}, numFailedTasks=${rmiMaster.failedTasks}"
            )
            totalTasks += numActiveTasks
          }
          /** Done batch pass */
        }
        /** Done batch loop */
      } finally {
        if (Thread.currentThread().isInterrupted) {
          logWarning(s"writeUUID=${writeUUID} SqlThread interrupted")
        }
        /** Wait for final abort or commit */
        val completeOk = waitJobComplete(optionsFactory.networkTimeout)
        try {
          logTrace(s"\nSqlThread terminated: complete=${completeOk}, pass ${sqlThreadPass.get()}:" +
            s" writeUUID=${writeUUID}")
          if (rmiMaster != null)
            rmiMaster.stop()
          rmiMaster = null
          dbConnectionGuard.synchronized {
            if (!completeOk) {
              /*
                          try {
                            dbConnection.abort(dbAbortThread)
                          } catch {
                            case _: Throwable =>
                          }
              */
              if (!dbConnection.getAutoCommit)
                dbConnection.rollback()
            }
            try {
              dbConnection.close()
              dbConnection = null
            } catch {
              //case _: Throwable =>
              case e: Exception => logWarning(s"dbConnection.close(): ${e}")
            }
          }
        } catch {
          case _: Throwable =>
        }
        if (!completeOk)
          throw new Exception(s"writeUUID=${writeUUID} timeout on waiting job completion")
      }
    }
  }

  private def waitJobComplete(msMax: Long = 60000): Boolean = {
    NetUtils().waitForCompletion(msMax) {done.get() || aborted.get()}
  }

  override def useCommitCoordinator: Boolean = false

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case msg: GreenplumWriterCommitMessage if msg.writeUUID.equals(writeUUID) =>
        logTrace(s"\nonDataWriterCommit writeUUID=${writeUUID}, " +
          s"instanceId=${msg.instanceId}, node=${msg.gpfdistUrl}, " +
          s"epochId=${msg.epochId}, partitionId=${msg.partitionId}, taskId=${msg.taskId}, " +
          s"nRowsWritten=${msg.nRowsWritten}, rmiPutMs=${msg.rmiPutMs}, rmiGetMs=${msg.rmiGetMs}")
      case _ =>
        logTrace(s"\nwriteUUID=${writeUUID} onDataWriterCommit ${message.toString}")
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

  //override
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    NetUtils().waitForCompletion(optionsFactory.networkTimeout) {!processing.get()}
    dbConnectionGuard.synchronized {
      if (dbConnection != null) {
        dbConnection.commit()
      } else {
        throw new Exception(s"writeUUID=${writeUUID} too late commit attempt, transaction is rolled back")
      }
    }
    logTrace(s"Commit epoch ${epochId}, batch=${rmiMaster.batchNo.get()}, " +
      s"messages=\n${messages.mkString("{", "\n", "}")}")
    var (epoch: Long, nParts: Int, nRows: Long) = (0L, 0, 0L)
    val writeMs: ListBuffer[Long] = new ListBuffer[Long]()
    val readMs: ListBuffer[Long] = new ListBuffer[Long]()
    val transferBytes: ListBuffer[Long] = new ListBuffer[Long]()
    val transferMs: ListBuffer[Long] = new ListBuffer[Long]()
    messages.foreach {
      case part: GreenplumWriterCommitMessage =>
        epoch = part.epochId
        nParts += 1
        nRows += part.nRowsWritten
        writeMs += part.rmiPutMs
        readMs += part.rmiGetMs
        transferBytes += part.nBytesWritten
        transferMs += part.webTransferMs
    }
    val now = System.currentTimeMillis()
    val processingMs = now - epochProcessingStart.get()
    val writeMsAvg = if (nParts > 0) writeMs.sum.toDouble / nParts.toDouble else 0.0
    val readMsAvg = if (nParts > 0) readMs.sum.toDouble / nParts.toDouble else 0.0
    logInfo(s"Epoch $epoch written $nRows rows in ${sqlThreadPass.get() - 1} passes and $nParts parts, " +
      s"processingMs=$processingMs, buffPutMs/part=$writeMsAvg, buffGetMs/part=$readMsAvg, " +
      s"initMs=${initMs.get()}, settleMs=${settleMs.get()}, commitMs=${commitMs.get()}+${now - groupCommitStart.get()}," +
      s" totalBytes=${transferBytes.sum}, webTransferMs=${transferMs.sum}")
    done.set(true)
  }

  //override
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    logWarning(s"Abort epoch ${epochId}, batch=${sqlThreadPass.get()-1}, " +
      s"messages=\n${messages.mkString("{", "\n", "}")}")
    try {
      dbConnectionGuard.synchronized {
        if (dbConnection != null) {
          dbConnection.rollback()
          if (optionsFactory.undoSideEffectsSQL.nonEmpty) {
            GPClient.executeStatement(dbConnection, optionsFactory.undoSideEffectsSQL)
          }
        }
      }
    } catch {
      case e: Exception => logWarning(s"On abort epoch ${epochId}: ${e}")
    }
    aborted.set(true)
  }
}

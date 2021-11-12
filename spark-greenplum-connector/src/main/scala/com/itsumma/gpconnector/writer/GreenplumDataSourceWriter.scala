package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.rmi.RMIMaster
import com.itsumma.gpconnector.{ExternalProcessUtil, GPClient}
import com.typesafe.scalalogging.Logger
import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPColumnMeta, GPOptionsFactory, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.net.{InetAddress, ServerSocket}
import java.nio.file.{Files, Path, Paths}
import java.sql.Connection
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
//import scala.::
import scala.collection.mutable.{ListBuffer, HashMap => MutableHashMap, Set => MutableSet}
import scala.util.control.Breaks

class GreenplumDataSourceWriter(writeUUID: String, schema: StructType, saveMode: SaveMode, optionsFactory: GPOptionsFactory)
  extends DataSourceWriter {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
  private val dbConnection: Connection = GPClient.getConn(optionsFactory)
  // private val numGpSegments: Int = optionsFactory.numSegments
  private val nGpSegments = GPOptionsFactory.queryNSegments(dbConnection)
  private val numPartFactories = new AtomicLong(0)
  private val sparkContext = SparkContext.getOrCreate
  private val statusTracker = sparkContext.statusTracker
  private val sqlThreadPass = new AtomicInteger(0)
  private val dbTableName: String = optionsFactory.tableOrQuery
  private val fieldDelimiter = "|"
  private var ignoreInsert: Boolean = false
  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  private val maxParallelTasks = new AtomicInteger(sparkContext.getExecutorMemoryStatus.keys.size - 1)
  if (sparkContext.deployMode == "cluster")
    maxParallelTasks.decrementAndGet()
  private val nTasksLeft = new AtomicInteger(-1)
  private val spareSocket = new ServerSocket(0)
  private val registryPort = spareSocket.getLocalPort
  spareSocket.close()
  private val rmiMaster = new RMIMaster(registryPort, nGpSegments, done, aborted)

  def gessMaxParallelTasks(): Unit = {
    var gess = sparkContext.getExecutorMemoryStatus.keys.size - 1
    if (sparkContext.deployMode == "cluster")
      gess -= 1
    maxParallelTasks.set(gess)
  }

/*
  private val executorsLocality: MutableHashMap[String, Int] = new MutableHashMap[String, Int]
  executorsLocality.put("node1", 1)
  executorsLocality.put("node2", 1)
  if (sparkContext.requestTotalExecutors(2, 2, executorsLocality.toMap)) {
    logger.info(s"requestTotalExecutors success")
  } else {
    logger.info(s"requestTotalExecutors fail")
  }
*/
  //sparkContext.setJobGroup(writeUUID, s"greenplum connector writer $writeUUID jobs", true)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
  dbConnection.setAutoCommit(false)

  private var gpTableMeta: Map[String, GPColumnMeta] = null

  if (!GPClient.tableExists(dbConnection, dbTableName)) {
    val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target)
    GPClient.executeStatement(dbConnection, s"create table $dbTableName ($createColumnsClause)")
    gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, optionsFactory.dbSchema, dbTableName)
  } else {
    gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, optionsFactory.dbSchema, dbTableName)
    if (saveMode == SaveMode.ErrorIfExists) {
      throw new Exception(s"table $dbTableName exists and saveMode=ErrorIfExists")
    } else if (saveMode == SaveMode.Ignore) {
      ignoreInsert = true
    } else if (saveMode == SaveMode.Overwrite) {
      if (optionsFactory.truncate) {
        GPClient.executeStatement(dbConnection, s"truncate table $dbTableName")
      } else {
        val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target, gpTableMeta)
        GPClient.executeStatement(dbConnection, s"drop table $dbTableName")
        var createTable = s"create table $dbTableName ($createColumnsClause)"
        if (optionsFactory.distributedBy != null) {
          createTable += s" DISTRIBUTED BY (${optionsFactory.distributedBy})"
        }
        GPClient.executeStatement(dbConnection, createTable)
      }
    }
  }

  if (!dbConnection.getAutoCommit)
    dbConnection.commit()

  private val extTblCreateColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.None)
  private val colListSelect = SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-write$writeUUID")
  sqlThread.setDaemon(true)

  private var latchJobId: Int = -1
  private var latchStageId: Int = -1

  private def getSchedulingInfo: (Int, Int, Int, Int) = {
    var numTasks = 0
    var numActiveTasks = 0
    var numCompletedTasks = 0
    var numFailedTasks = 0
    if (aborted.get() || done.get())
      return (numActiveTasks, numCompletedTasks, numFailedTasks, numTasks)
    if (latchStageId < 0) {
      if (latchJobId < 0) {
        val start = System.currentTimeMillis()
        while (!Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < 12000)
          && !done.get() && !aborted.get()
          //&& (statusTracker.getJobIdsForGroup(writeUUID).isEmpty)
          && statusTracker.getActiveJobIds().isEmpty
          )
          Thread.sleep(10)
        val groupJobIds = statusTracker.getActiveJobIds() //.getJobIdsForGroup(writeUUID)
        if (groupJobIds.isEmpty) {
          logger.warn(s"${(System.currentTimeMillis() % 1000).toString} getSchedulingInfo: no jobs for group $writeUUID")
          throw new Exception(s"getSchedulingInfo: no jobs for group $writeUUID")
          // return (numActiveTasks, numCompletedTasks, numFailedTasks, numTasks)
        }
        val jobIdLoopOuter = new Breaks
        jobIdLoopOuter.breakable {
          groupJobIds.foreach(jobId => {
            statusTracker.getJobInfo(jobId) match {
              case Some(jobInfo) =>
                if (jobInfo.status() == JobExecutionStatus.RUNNING) {
                  latchJobId = jobId
                  logger.info(s"${(System.currentTimeMillis() % 1000).toString} Latched, latchJobId=$latchJobId")
                  jobIdLoopOuter.break
                }
              case None =>
            }
          })
        }
      }
      if (latchJobId >= 0) {
        val activeStageIds = statusTracker.getActiveStageIds
        statusTracker.getJobInfo(latchJobId) match {
          case Some(jobInfo) => {
            //            if (jobInfo.status() == JobExecutionStatus.RUNNING) {
            //            }
            val stageIds = jobInfo.stageIds().intersect(activeStageIds)
            val stageInfoLoopOuter = new Breaks
            stageInfoLoopOuter.breakable(
              stageIds.foreach(stageId => {
                statusTracker.getStageInfo(stageId) match {
                  case Some(stageInfo) =>
                    if (stageInfo.numTasks() > 0) {
                      numTasks = stageInfo.numTasks()
                      numActiveTasks = stageInfo.numActiveTasks()
                      numCompletedTasks = stageInfo.numCompletedTasks()
                      numFailedTasks = stageInfo.numFailedTasks()
                      latchStageId = stageId
                      logger.info(s"${(System.currentTimeMillis() % 1000).toString} Latched, latchStageId=$latchStageId, " +
                        s"numTasks=$numTasks, numActiveTasks=$numActiveTasks")
                      stageInfoLoopOuter.break
                    }
                  case None =>
                }
              })
            )
          }
          case None =>
        }
      }
    } else {
      statusTracker.getStageInfo(latchStageId) match {
        case Some(stageInfo) =>
          if (stageInfo.numTasks() > 0) {
            numTasks = stageInfo.numTasks()
            numActiveTasks = stageInfo.numActiveTasks()
            numCompletedTasks = stageInfo.numCompletedTasks()
            numFailedTasks = stageInfo.numFailedTasks()
          }
        case None =>
      }
    }
    if ((maxParallelTasks.get() > 0) && (numActiveTasks > maxParallelTasks.get()))
      numActiveTasks = maxParallelTasks.get()
    if ((nTasksLeft.get() >= 0) && (numActiveTasks > nTasksLeft.get()))
      numActiveTasks = nTasksLeft.get()
    (numActiveTasks, numCompletedTasks, numFailedTasks, numTasks)
  }

  import java.util.concurrent.Executor
  import java.util.concurrent.Executors
  val dbAbortThread: Executor = Executors.newFixedThreadPool(1)

  private class SqlThread(private val conn: Connection) extends Runnable {
    override def run(): Unit = {
      try {
        conn.setAutoCommit(false)
        var (
          numTasks,
          numActiveTasks,
          numCompletedTasks,
          numFailedTasks) = (0, 0, 0, 0)

        /** Batch loop */
        while (!done.get() && !aborted.get() && !Thread.currentThread().isInterrupted) {

          {
            val schedulingInfo = getSchedulingInfo
            // numActiveTasks = schedulingInfo._1
            numCompletedTasks = schedulingInfo._2
            numFailedTasks = schedulingInfo._3
            numTasks = schedulingInfo._4
          }

          /** Start batch pass */
          logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.incrementAndGet()}:" +
            s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId, numTasks=$numTasks, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")

          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch() // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          /** Done executors initialization */
          numActiveTasks = rmiMaster.numActiveTasks

          if ((batchNo < 0) || (numActiveTasks == 0)) {
            logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
              s"numTasks=${numTasks}, numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
            return
          }
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
          val activeWorkers = new StringBuilder("")
          i = 0
          sparkContext.getExecutorMemoryStatus.keys.map(node => {
            if (i > 0)
              activeWorkers.append(",")
            activeWorkers.append(node)
            i += 1
          })
          val externalTableName: String = s"ext${SparkSchemaUtil.stripChars(writeUUID, "-")}_${sqlThreadPass.get().toString}"
          if (sqlThreadPass.get() == 1)
            nTasksLeft.set(numTasks)
          logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.get()}: writeUUID=${writeUUID}, " +
            s"numActiveTasks=$numActiveTasks, activeWorkers='${activeWorkers.toString()}'")
          try {
            GPClient.executeStatement(conn, s"drop external table if exists $externalTableName")
            val createExtTbl = s"CREATE READABLE EXTERNAL TABLE $externalTableName" +
              //s" (LIKE ${table})" +
              s"($extTblCreateColumnsClause)" +
              s" LOCATION (${locationClause.toString()})" +
              s" FORMAT 'TEXT' (DELIMITER '$fieldDelimiter' " +
              s"    NULL 'NULL' " +
              s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
              s"  ) " +
              s" ENCODING 'UTF8'"
            logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} $createExtTbl")
            GPClient.executeStatement(conn, createExtTbl)
            var insertSelectSql = s"insert into $dbTableName (${colListInsert}) "
            insertSelectSql += s"select $colListSelect from $externalTableName"
            logger.info(s"SQL: $insertSelectSql")

            try {
              val nRows = GPClient.executeStatement(conn, insertSelectSql)
              logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                s" nRows=${nRows}")
            } catch {
              case e: Exception => {
                logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                  s" failed, ${e.getMessage}")
                //if (!aborted.get())
                throw e
              }
            }
          } finally {
            try {
              GPClient.executeStatement(conn, s"drop external table if exists $externalTableName")
            } catch {
              case e: Exception => logger.error(s"${e.getMessage}")
            }
          }

          /** Wait for all active dataWriter commits */
          rmiMaster.commitBatch()

          {
            val schedulingInfo = getSchedulingInfo
            // numActiveTasks = schedulingInfo._1
            numCompletedTasks = schedulingInfo._2
            numFailedTasks = schedulingInfo._3
            numTasks = schedulingInfo._4
          }

          nTasksLeft.addAndGet(- numActiveTasks)
          logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread end of pass ${sqlThreadPass.get()}: " +
            s"writeUUID=${writeUUID}, " +
            s"numTasks=${numTasks}, numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
          /** Done batch pass */
        }
        /** Done batch loop */
        /** Wait for final abort or commit */
        waitJobComplete()
      } finally {
        try {
          logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} SqlThread terminated: pass ${sqlThreadPass.get()}:" +
            s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId")
          if (rmiMaster != null)
            rmiMaster.stop
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
            //sparkContext.cancelJobGroup(writeUUID)
            sparkContext.cancelAllJobs()
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

  private def waitJobComplete(msMax: Int = 60000): Boolean = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var result: Boolean = false
    while (!result && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      result = done.get() || aborted.get()
      if (!result)
        Thread.sleep(rnd.nextInt(100) + 1)
    }
    if (!result)
      throw new Exception(s"writeUUID=${writeUUID} timeout on waiting job completion")
    result
  }


  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    var factory: GreenplumDataWriterFactory = null
    if (ignoreInsert) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} " +
        s"createWriterFactory returns null due to saveMode=Ignore and table $dbTableName exists" +
        s"writeUUID=${writeUUID}")
      return null
    }
    var nFact: Long = 0
    factory = new GreenplumDataWriterFactory(writeUUID, schema, saveMode, optionsFactory, nGpSegments, s"${localIpAddress}:${registryPort}")
    nFact = numPartFactories.incrementAndGet()
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} createWriterFactory " +
      s"writeUUID=${writeUUID} numGpSegments=$nGpSegments numPartFactories=${nFact}")
    if (nFact == 1) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread(${sqlThread.getName}) start")
      sqlThread.start()
    }
    factory
  }

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case msg: GreenplumWriterCommitMessage if (msg.writeUUID.equals(writeUUID)) =>
        logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} onDataWriterCommit writeUUID=${writeUUID}, " +
          s"instanceId=${msg.instanceId}, node=${msg.gpfdistUrl}, " +
          s"epochId=${msg.epochId}, partitionId=${msg.partitionId}, taskId=${msg.taskId}, " +
          s"nRowsWritten=${msg.nRowsWritten}, rmiPutMs=${msg.rmiPutMs}, rmiGetMs=${msg.rmiGetMs}")
      case _ =>
        logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} onDataWriterCommit ${message.toString}")
    }
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} commit nCommitMessages=${writerCommitMessages.length}")
    done.set(true)
  }

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    aborted.set(true)
    logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} abort nCommitMessages=${writerCommitMessages.length}")
  }
}

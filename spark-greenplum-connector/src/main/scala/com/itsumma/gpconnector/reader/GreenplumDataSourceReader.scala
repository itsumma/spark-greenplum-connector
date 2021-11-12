package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.rmi.RMIMaster

import java.sql.Connection
import java.util
import java.util.{OptionalLong, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import com.itsumma.gpconnector.{ExternalProcessUtil, GPClient}
import com.typesafe.scalalogging.Logger
import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil.using
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.net.{InetAddress, ServerSocket}
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, HashMap => MutableHashMap}
import scala.util.control._
//import scala.collection.mutable.{Set => MutableSet}

// scalafix:off DisableSyntax.null
// scalastyle:off null

@DeveloperApi
case class GPInputPartitionReaderReadyEvent(
                                             queryId: String,
                                             instanceId: String,
                                             //hostName: String,
                                             executorId: String,
                                             numPartsDetected: Int,
                                             gpfdistUrl: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

@DeveloperApi
case class GPInputPartitionReaderCloseEvent(
                                             queryId: String,
                                             instanceId: String,
                                             //hostName: String,
                                             executorId: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

class GreenplumDataSourceReader(optionsFactory: GPOptionsFactory)
  extends DataSourceReader
    with SupportsReportPartitioning
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with SupportsReportStatistics {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  val tableOrQuery: String = optionsFactory.tableOrQuery
  val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  val dbTableName: String = if (tableOrQuery.contains(" ")) "" else tableOrQuery
  //val sparkSession = SparkSession.builder().getOrCreate()
  private val sparkContext = SparkContext.getOrCreate
  private val fieldDelimiter = "|"
  private val dbConnection: Connection = GPClient.getConn(optionsFactory)
  //  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
  dbConnection.setAutoCommit(true)
  private val notificationsTimeout: Int = sparkContext.getConf.getOption("spark.batch.duration.seconds").orElse(Option("120")).get.toInt * 1000
  var whereClause = ""
  var pushedDownFilters: Array[Filter] = new Array[Filter](0)
  private var schema = SparkSchemaUtil.getGreenplumTableSchema(optionsFactory, tableOrQuery)
  //private val partIds = ListBuffer[String]() //new util.ArrayList[String]()
  private val sqlThreadPass = new AtomicInteger()
  private val maxParallelTasks = new AtomicInteger(0)
  private val localIpAddress: String = InetAddress.getLocalHost.getHostAddress

   //sparkContext.setJobGroup(queryId, "greenplum connector reader jobs", true)
   private val statusTracker = sparkContext.statusTracker

  def guessMaxParallelTasks(): Unit = {
    var guess = sparkContext.getExecutorMemoryStatus.keys.size - 1
    if (sparkContext.deployMode == "cluster")
      guess -= 1
    maxParallelTasks.set(guess)
  }

  while ((maxParallelTasks.get() <= 0) && !Thread.currentThread().isInterrupted) {
    guessMaxParallelTasks()
  }
  private val numParts: Int = math.min(optionsFactory.numSegments, maxParallelTasks.get()) //math.max(sc.defaultMinPartitions, optionsFactory.numSegments)
  //private val nGpSegments = GPOptionsFactory.queryNSegments(dbConnection)

  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-read$queryId")
  sqlThread.setDaemon(true)
  private val aborted = new AtomicBoolean(false)
  private val done = new AtomicBoolean(false)
  private val spareSocket = new ServerSocket(0)
  private val registryPort = spareSocket.getLocalPort
  spareSocket.close()
  private val rmiMaster = new RMIMaster(registryPort, numParts, done, aborted)
  private val rmiRegistryAddress = s"${localIpAddress}:${registryPort}"

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val inputPartitions = ListBuffer[InputPartition[InternalRow]]()
    val numPartsPlan = numParts
    for (i <- 0 until numPartsPlan) {
      val partInstanceId = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
      val inpPart = new GreenplumInputPartition(optionsFactory, queryId, partInstanceId, schema, i, rmiRegistryAddress)
      //partIds += partInstanceId
      inputPartitions += inpPart
    }
    logger.info(s"Initially creating ${inputPartitions.size} partitions for queryId=${queryId}")
    //TODO: detect and drop GP external tables possibly left from previous failed operations
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

  private var latchJobId: Int = -1
  private var latchStageId: Int = -1

  private def getSchedulingInfo: (Int, Int, Int) = {
    var numActiveTasks = 0
    var numCompletedTasks = 0
    var numFailedTasks = 0
    if (latchStageId < 0) {
      if (latchJobId < 0) {
        val groupJobIds = statusTracker.getActiveJobIds() //.getJobIdsForGroup(queryId)
        val jobIdLoopOuter = new Breaks
        jobIdLoopOuter.breakable {
          groupJobIds.foreach(jobId => {
            statusTracker.getJobInfo(jobId) match {
              case Some(jobInfo) =>
                if (jobInfo.status() == JobExecutionStatus.RUNNING) {
                  latchJobId = jobId
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
                      numActiveTasks = stageInfo.numActiveTasks()
                      numCompletedTasks = stageInfo.numCompletedTasks()
                      numFailedTasks = stageInfo.numFailedTasks()
                      latchStageId = stageId
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
            numActiveTasks = stageInfo.numActiveTasks()
            numCompletedTasks = stageInfo.numCompletedTasks()
            numFailedTasks = stageInfo.numFailedTasks()
          }
        case None =>
      }
    }
    (numActiveTasks, numCompletedTasks, numFailedTasks)
  }

  private class SqlThread(private val conn: Connection) extends Runnable {
    def run(): Unit = {
      try {
        //        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        conn.setAutoCommit(true)
        var (numActiveTasks,
        numCompletedTasks,
        numFailedTasks) = (0, 0, 0)
        while ((numActiveTasks == 0) && !Thread.currentThread().isInterrupted) {
          val schedulingInfo = getSchedulingInfo
          numActiveTasks = schedulingInfo._1
          numCompletedTasks = schedulingInfo._2
          numFailedTasks = schedulingInfo._3
        }
        /** Batch loop */
        while ((numActiveTasks > 0) && !Thread.currentThread().isInterrupted) {
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.incrementAndGet()}:" +
            s" queryId=${queryId}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")

          /** Start the loop waiting executors initialization */
          val batchNo = rmiMaster.waitBatch() // Throws exception if no executors has been allocated, or not all of them commit within given time interval
          /** Done executors initialization */
          numActiveTasks = rmiMaster.numActiveTasks

          if ((batchNo < 0) || (numActiveTasks == 0)) {
            logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" queryId=${queryId}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
              s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
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
          val externalTableName: String = s"ext${queryId}_${sqlThreadPass.get().toString}"
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.get()}: queryId=${queryId}, " +
            s"locationClause=(${locationClause.toString()})")
          try {
            var schemaOrPlaceholder = schema
            if (schemaOrPlaceholder.isEmpty)
              schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
            if (GPClient.tableExists(conn, externalTableName))
              GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
            val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.ExternalWritable)
            //if (columnsClause.isEmpty) columnsClause = s"dummy char(1)"
            val createExternalTable = s"CREATE WRITABLE EXTERNAL TABLE $externalTableName" +
              //s" (LIKE ${table})" +
              s"($createColumnsClause)" +
              s" LOCATION (${locationClause.toString()})" +
              s" FORMAT 'TEXT' (DELIMITER '$fieldDelimiter' " +
              s"    NULL 'NULL' " +
              //s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
              s"  ) " +
              s" ENCODING 'UTF8'"
            //+ s" DISTRIBUTED BY (exp_id)"
            logger.info(s"createExternalTable: $createExternalTable")
            GPClient.executeStatement(conn, createExternalTable)
            //TODO: consider adding DISTRIBUTED clause here, if appropriate
            val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.None)
            val colListSelect = if (schema.nonEmpty) {
              // colListInsert
              SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.Target)
            } else "null"
            var insertSelectSql = s"insert into $externalTableName (${colListInsert}) "
            if (dbTableName.nonEmpty) {
              insertSelectSql += s"select $colListSelect from $dbTableName"
            } else {
              insertSelectSql += s"select $colListSelect from ($tableOrQuery) sqr"
            }
            if (whereClause.nonEmpty)
              insertSelectSql += s" where $whereClause"
            logger.info(s"SQL: $insertSelectSql")
            try {
              val nRows = GPClient.executeStatement(conn, insertSelectSql)
              logger.info(s"${(System.currentTimeMillis() % 1000).toString}\nSqlThread queryId=${queryId} '${insertSelectSql}'" +
                s" nRows=${nRows}")
            } catch {
              case e: Exception => {
                logger.info(s"${(System.currentTimeMillis() % 1000).toString}\nSqlThread queryId=${queryId} '${insertSelectSql}'" +
                  s" failed, ${e.getMessage}")
                numActiveTasks = 0
                // TODO: throw only when not all active tasks get completed
                /*
                                  val schedulingInfo = getSchedulingInfo
                                  numCompletedTasks = schedulingInfo._2
                                  //  if (numPartsClosed.get() == 0)
                                  if (numActiveTasks < numCompletedTasks)
                                    throw e
                */
              }
            }
            if (!conn.getAutoCommit)
              conn.commit()
            rmiMaster.commitBatch()
          } finally {
            if (GPClient.tableExists(conn, externalTableName)) {
              try {
                GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
              } catch {
                case e: Exception => logger.error(s"${e.getMessage}")
              }
            }
          }

          val schedulingInfo = getSchedulingInfo
          //numActiveTasks = schedulingInfo._1
          numCompletedTasks = schedulingInfo._2
          numFailedTasks = schedulingInfo._3
          logger.info(s"${(System.currentTimeMillis() % 1000).toString}\nSqlThread end of pass ${sqlThreadPass.get()}: " +
            s"queryId=${queryId}, partUrlsCount=${numActiveTasks}")

          /** Batch loop end*/
        }

      } finally {
        // sc.removeSparkListener(partListener)
        // GPClient.executeStatement(conn, s"delete from gp_spark_instance where query_id = '${queryId}'")
        conn.close()
        try {
          //ExternalProcessUtil.deleteRecursively(tempDir)
          rmiMaster.stop
        } catch { case _: Throwable => }
        //sparkContext.clearJobGroup()
        // TODO: Distinguish partial fetch from real fault. Kill job only on fault.
        //if (!done.get()) sparkContext.cancelAllJobs()
      }
    }
  }

  override def outputPartitioning(): Partitioning = new Partitioning {
    override def numPartitions(): Int = {
      logger.info(s"outputPartitioning().numPartitions() = ${numParts}")
      numParts
    }

    override def satisfy(distribution: Distribution): Boolean = true
  }

  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.of(123456789000L)

    override def numRows(): OptionalLong = OptionalLong.of(123456789000L)
  }
}

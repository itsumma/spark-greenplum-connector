package com.itsumma.gpconnector.reader

import java.sql.Connection
import java.util
import java.util.{OptionalLong, UUID}
import java.util.concurrent.atomic.AtomicInteger
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
  val tableOrQuery: String = optionsFactory.tableOrQuery
  val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  private val tempDir: Path = Files.createDirectories(Paths.get("/tmp", "gp", "reader", queryId)) // port.toString
  val dbTableName: String = if (tableOrQuery.contains(" ")) "" else tableOrQuery
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
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
  private val stage: AtomicInteger = new AtomicInteger()
  private val partUrlsCount = new AtomicInteger()
  private val sqlThreadPass = new AtomicInteger()
  private val maxParallelTasks = new AtomicInteger(0)

   sparkContext.setJobGroup(queryId, "greenplum connector reader jobs", true)
   private val statusTracker = sparkContext.statusTracker

  def gessMaxParallelTasks(): Unit = {
    var gess = sparkContext.getExecutorMemoryStatus.keys.size - 1
    if (sparkContext.deployMode == "cluster")
      gess -= 1
    maxParallelTasks.set(gess)
  }

  while ((maxParallelTasks.get() <= 0) && !Thread.currentThread().isInterrupted) {
    gessMaxParallelTasks()
  }
  private val numParts: Int = math.min(optionsFactory.numSegments, maxParallelTasks.get()) //math.max(sc.defaultMinPartitions, optionsFactory.numSegments)

  /*
    private val partListener: SparkListener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case partitionReaderReadyEvent: GPInputPartitionReaderReadyEvent => {
          if (partitionReaderReadyEvent.queryId.equals(queryId)) {
            logger.info(s"-------${partitionReaderReadyEvent.toString}-------")
            partUrls.put(partitionReaderReadyEvent.instanceId, partitionReaderReadyEvent.gpfdistUrl)
            partUrlsCount.set(partUrls.size)
            numPartsDetected.set(partitionReaderReadyEvent.numPartsDetected)
          }
        }
        case partitionCloseEvent: GPInputPartitionReaderCloseEvent => {
          if (partitionCloseEvent.queryId.equals(queryId))
            numPartsClosed.getAndIncrement()
        }
        case jobStartEvent: SparkListenerJobStart => {
          logger.info(s"-------jobStartEvent=${jobStartEvent.toString}-------")
        }
        case _ => {
          val activeStageIds = statusTracker.getActiveStageIds
          statusTracker.getJobIdsForGroup(queryId).foreach( jobId => {
            statusTracker.getJobInfo(jobId) match {
              case Some(jobInfoData) => {
                val stageIds = jobInfoData.stageIds().intersect(activeStageIds)
                stageIds.foreach(stageId => {
                  statusTracker.getStageInfo(stageId) match {
                    case Some(stageInfo) =>
                      if (stageInfo.numTasks() > numPartsDetected.get())
                        numPartsDetected.set(stageInfo.numTasks())
                    case None =>
                  }
                })
              }
              case None =>
            }
          })
        }
      }
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          statusTracker.getJobIdsForGroup(queryId).foreach( jobId => {
            if (jobId == jobStart.jobId) {
              jobStart.stageInfos.foreach(stageInfo => {
                stageInfo.completionTime match {
                  case Some(x) =>
                  case None => if (stageInfo.numTasks > numPartsDetected.get()) numPartsDetected.set(stageInfo.numTasks)
                }
              })
            }
  //          val jobInfo = statusTracker.getJobInfo(jobId)
  //          jobInfo match {
  //            case Some(jobInfoData) => {
  //              val stageIds = jobInfoData.stageIds().intersect(jobStart.stageIds)
  //              stageIds.foreach(stageId => {
  //                statusTracker.getStageInfo(stageId) match {
  //                  case Some(stageInfo) => logger.info(s"Query=$queryId/ "
  //                    + "stageId=" + stageId + "; name = " + stageInfo.name()
  //                    + "; completed tasks:" + stageInfo.numCompletedTasks()
  //                    + "; active tasks: " + stageInfo.numActiveTasks()
  //                    + "; all tasks: " + stageInfo.numTasks()
  //                    + "; submission time: " + stageInfo.submissionTime())
  //                    numPartsDetected.set(stageInfo.numTasks())
  //                  case None =>
  //                }
  //              })
  //            }
  //            case None =>
  //          }
  //        })
        })
      }
    }

  //  sc.addSparkListener(partListener)
  */

  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-read$queryId")
  sqlThread.setDaemon(true)

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    if (stage.get() != 0)
      throw new UnsupportedOperationException(
        s"Attempt to re-run previously executed query"
      )
    if (!GPClient.tableExists(dbConnection, "gp_spark_instance")) {
      GPClient.executeStatement(dbConnection, s"create table gp_spark_instance (" +
        s"query_id varchar(50) not null, " +
        s"instance_id varchar(50) not null, " +
        s"part_id int not null, " +
        s"dir varchar(10) default 'r', " +
        s"part_url varchar(255) null, " +
        s"row_count decimal(20) default 0, " +
        s"dt_start timestamp default CURRENT_TIMESTAMP, " +
        s"dt_end timestamp null, " +
        s"status varchar(10) null, " +
        s"batch_no int null, " +
        s"node_ip varchar(50) null, " +
        s"executor_id int null, " +
        s"primary key (query_id, instance_id, part_id) ) " +
        s"DISTRIBUTED REPLICATED")
      if (!dbConnection.getAutoCommit)
        dbConnection.commit()
    }

    var inputPartitions = ListBuffer[InputPartition[InternalRow]]()
    val numPartsPlan = numParts
    for (i <- 0 until numPartsPlan) {
      val partInstanceId = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
      val inpPart = new GreenplumInputPartition(optionsFactory, queryId, partInstanceId, schema, i)
      //partIds += partInstanceId
      inputPartitions += inpPart
      GPClient.executeStatement(dbConnection, s"insert into gp_spark_instance (query_id, instance_id, part_id) " +
        s"values ('${queryId}', '${partInstanceId}', ${i})")
    }
    logger.info(s"Initially creating ${inputPartitions.size} partitions for queryId=${queryId}")
    //TODO: detect and drop GP external tables possibly left from previous failed operations
    stage.incrementAndGet()
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
    // (numParts, 0, 0)
    var numActiveTasks = 0
    var numCompletedTasks = 0
    var numFailedTasks = 0
    if (latchStageId < 0) {
      if (latchJobId < 0) {
        val groupJobIds = statusTracker.getJobIdsForGroup(queryId)
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
    def run {
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
        while ((numActiveTasks > 0) && !Thread.currentThread().isInterrupted) {
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.incrementAndGet()}:" +
            s" queryId=${queryId}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")

          val partUrls: MutableHashMap[String, String] = MutableHashMap()
          partUrlsCount.set(0)
          val start = System.currentTimeMillis()
          while (!Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < 12000) && (partUrls.size < numActiveTasks)) {
            //SparkSchemaUtil.using(GPClient.getConn(optionsFactory)) { connection => {
            SparkSchemaUtil.using(conn.prepareStatement(s"select instance_id, part_url, dt_end from gp_spark_instance where query_id = ? " +
              s"and part_url is not null and status is null")) {
              statement => {
                statement.setString(1, queryId)
                SparkSchemaUtil.using(statement.executeQuery()) {
                  rs => {
                    while (rs.next()) {
                      val instanceId = rs.getString(1)
                      val gpfUrl = rs.getString(2)
                      val dtEnd = rs.getTimestamp(3)
                      if (rs.wasNull()) {
                        if (!partUrls.contains(instanceId)) {
                          partUrls.put(instanceId, gpfUrl)
                          logger.info(s"${(System.currentTimeMillis() % 1000).toString} Added location: queryId=${queryId}, instanceId=${instanceId}, gpfUrl='${gpfUrl}'")
                          GPClient.executeStatement(conn, s"update gp_spark_instance set status = 'exec' " +
                            s"where query_id = '${queryId}' and instance_id = '${instanceId}' ")
                          partUrlsCount.incrementAndGet()
                        }
                      } else {
                        if (partUrls.contains(instanceId)) {
                          partUrls.remove(instanceId)
                          logger.info(s"${(System.currentTimeMillis() % 1000).toString} Removed location: queryId=${queryId}, instanceId=${instanceId}, gpfUrl='${gpfUrl}', dtEnd='${dtEnd.toString}")
                          GPClient.executeStatement(conn, s"update gp_spark_instance set status = 'kill' " +
                            s"where query_id = '${queryId}' and instance_id = '${instanceId}' ")
                          partUrlsCount.decrementAndGet()
                        }
                      }
                    }
                  }
                }
              }
            }
            //}}
            if (partUrls.size < numActiveTasks) {
              Thread.sleep(100)
              val schedulingInfo = getSchedulingInfo
              numActiveTasks = schedulingInfo._1
              numCompletedTasks = schedulingInfo._2
              numFailedTasks = schedulingInfo._3
            }
          }
          stage.incrementAndGet()
          if (numActiveTasks == 0) {
            logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" queryId=${queryId}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
              s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
            return
          }
          if (partUrls.size == 0) {
            Thread.sleep(100)
          } else {
            GPClient.executeStatement(dbConnection, s"update gp_spark_instance set status = 'bypass', " +
              s"dt_end = CURRENT_TIMESTAMP " +
              s"where query_id = '${queryId}' and status is null ")
            val locationClause = new StringBuilder("")
            val instanceIdInClause = new StringBuilder("")
            var i = 0
            partUrls.foreach {
              case (instanceId, gpfdistUrl) => {
                if (i > 0) {
                  locationClause.append(", ")
                  instanceIdInClause.append(", ")
                }
                locationClause.append(s"'$gpfdistUrl'")
                instanceIdInClause.append(s"'${instanceId}'")
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
              stage.incrementAndGet()
              //TODO: consider adding DISTRIBUTED clause here, if appropriate
              val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schemaOrPlaceholder, GpTableTypes.None)
              val colListSelect = if (schema.nonEmpty) {
                // colListInsert
                SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.Target)
              } else "null"
              var insertSelectSql = s"insert into $externalTableName (${colListInsert}) "
              if (dbTableName.length > 0) {
                insertSelectSql += s"select $colListSelect from $dbTableName"
              } else {
                insertSelectSql += s"select $colListSelect from ($tableOrQuery) sqr"
              }
              if (whereClause.length > 0)
                insertSelectSql += s" where $whereClause"
              logger.info(s"SQL: $insertSelectSql")
              try {
                val nRows = GPClient.executeStatement(conn, insertSelectSql)
                logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread queryId=${queryId} '${insertSelectSql}'" +
                  s" nRows=${nRows}")
                insertSelectSql = s"update gp_spark_instance set status = '${nRows.toString}' where query_id = '${queryId}' " +
                  s"and instance_id in (${instanceIdInClause.toString()})"
                GPClient.executeStatement(conn, insertSelectSql)
              } catch {
                case e: Exception => {
                  logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread queryId=${queryId} '${insertSelectSql}'" +
                    s" failed, ${e.getMessage}")
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
            } finally {
              if (GPClient.tableExists(conn, externalTableName)) {
                try {
                  GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
                } catch {
                  case e: Exception => logger.error(s"${e.getMessage}")
                }
              }
            }
          }
          val schedulingInfo = getSchedulingInfo
          numActiveTasks = schedulingInfo._1
          numCompletedTasks = schedulingInfo._2
          numFailedTasks = schedulingInfo._3
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread end of pass ${sqlThreadPass.get()}: " +
            s"queryId=${queryId}, partUrlsCount=${partUrlsCount.get()}")
        }

      } finally {
        // sc.removeSparkListener(partListener)
        // GPClient.executeStatement(conn, s"delete from gp_spark_instance where query_id = '${queryId}'")
        conn.close()
        try {
          // Files.deleteIfExists(tempDir)
          ExternalProcessUtil.deleteRecursively(tempDir)
        } catch { case _: Throwable => }
        sparkContext.clearJobGroup()
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

package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.{ExternalProcessUtil, GPClient}
import com.typesafe.scalalogging.Logger
import org.apache.spark.{JobExecutionStatus, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPColumnMeta, GPOptionsFactory, GpTableTypes, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import java.sql.Connection
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
//import scala.::
import scala.collection.mutable.{ListBuffer, HashMap => MutableHashMap, Set => MutableSet}
import scala.util.control.Breaks

class GreenplumDataSourceWriter(writeUUID: String, schema: StructType, saveMode: SaveMode, optionsFactory: GPOptionsFactory)
  extends DataSourceWriter {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val dbConnection: Connection = GPClient.getConn(optionsFactory)
  private val numGpSegments: Int = optionsFactory.numSegments
  private val numPartFactories = new AtomicLong(0)
  private val factories = new ListBuffer[GreenplumDataWriterFactory]
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
  sparkContext.setJobGroup(writeUUID, s"greenplum connector writer $writeUUID jobs", true)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)

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
  private val extTblCreateColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val colListInsert = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.None)
  private val colListSelect = SparkSchemaUtil.getGreenplumSelectColumns(schema, GpTableTypes.ExternalReadable, gpTableMeta)
  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-write$writeUUID")
  sqlThread.setDaemon(true)

  private var latchJobId: Int = -1
  private var latchStageId: Int = -1

  private def getSchedulingInfo: (Int, Int, Int, Int) = synchronized {
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
          && (statusTracker.getJobIdsForGroup(writeUUID).isEmpty)
          )
          Thread.sleep(10)
        val groupJobIds = statusTracker.getJobIdsForGroup(writeUUID)
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

  def enumWriters: Long = {
    var ret: Long = 0
    this.synchronized {
      factories.foreach(factory => {
        ret += factory.getNWriters
      })
    }
    ret
  }

  val partIds: MutableSet[Int] = MutableSet[Int]()

  private class SqlThread(private val conn: Connection) extends Runnable {
    override def run(): Unit = {
      try {
        conn.setAutoCommit(false)
        var (
          numTasks,
          numActiveTasks,
          numCompletedTasks,
          numFailedTasks) = (0, 0, 0, 0)
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread waiting for numActiveTasks > 0 ..")
        while (((latchJobId < 0) || (numTasks < 1)
          || (numActiveTasks < Math.min(numTasks, maxParallelTasks.get()))
          || (maxParallelTasks.get() <= 0)
          )
          && !Thread.currentThread().isInterrupted
          && !aborted.get()
        ) {
          if (maxParallelTasks.get() <= 0)
            gessMaxParallelTasks()
          val schedulingInfo = getSchedulingInfo
          numActiveTasks = schedulingInfo._1
          numCompletedTasks = schedulingInfo._2
          numFailedTasks = schedulingInfo._3
          numTasks = schedulingInfo._4
          if (numActiveTasks == 0)
            Thread.sleep(10)
        }
        //val rddInfos = sparkContext.getRDDStorageInfo
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread start numTasks=$numTasks, " +
          s"numActiveTasks=$numActiveTasks")
        SparkSchemaUtil.using(conn.prepareStatement(s"insert into gp_spark_instance " +
          s"(query_id, instance_id, part_id, dir) " +
          s"values (?, ?, ?, ?)")) {
          statement => {
            Seq.range(0, numTasks).foreach( partId => {
              statement.setString(1, writeUUID)
              statement.setString(2, partId.toString)
              statement.setInt(3, partId)
              statement.setString(4, "w")
              statement.execute()
            })
          }
        }
        conn.commit()
        while (!done.get() && !aborted.get() && (numActiveTasks > 0) && !Thread.currentThread().isInterrupted) {
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.incrementAndGet()}:" +
            s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId, numTasks=$numTasks, " +
            s"numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
          val partUrls: MutableHashMap[String, String] = MutableHashMap()
          var partUrlsCount: Int = 0
          var partIdsCount: Int = 0

          val start = System.currentTimeMillis()
          while (!Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < 12000)
            // && (partUrls.size < numActiveTasks)
            && !done.get() && !aborted.get()
            && (partIdsCount < numActiveTasks)
          ) {
            partIdsCount = 0
            //partUrlsCount = 0
            //partUrls.clear()
            //SparkSchemaUtil.using(GPClient.getConn(optionsFactory)) { connection => {
            this.synchronized {
              partIds.clear()
              SparkSchemaUtil.using(conn.prepareStatement(s"select node_ip, part_url, part_id from gp_spark_instance where query_id = ? " +
                s"and part_url is not null and status is null and dt_end is null ")) {
                statement => {
                  statement.setString(1, writeUUID)
                  SparkSchemaUtil.using(statement.executeQuery()) {
                    rs => {
                      while (rs.next()) {
                        val nodeIp = rs.getString(1)
                        val gpfUrl = rs.getString(2)
                        val partId = rs.getInt(3)
                        if (!partUrls.contains(nodeIp)) {
                          partUrls.put(nodeIp, gpfUrl)
                          logger.info(s"${(System.currentTimeMillis() % 1000).toString} " +
                            s"Added location: writeUUID=${writeUUID}, nodeIp=${nodeIp}, gpfUrl='${gpfUrl}', partId=$partId")
                          partUrlsCount += 1
                        }
                        partIds.add(partId)
                        partIdsCount += 1
                      }
                    }
                  }
                }
              }
            }
            if (!conn.getAutoCommit)
              conn.commit()
            //}}
            if (true) {
              val schedulingInfo = getSchedulingInfo
              numActiveTasks = schedulingInfo._1
              numCompletedTasks = schedulingInfo._2
              numFailedTasks = schedulingInfo._3
              numTasks = schedulingInfo._4
            }
            if (partIdsCount < numActiveTasks) {
              Thread.sleep(100)
            }
          }
          if (numActiveTasks == 0) {
            logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread terminating: pass ${sqlThreadPass.get()}:" +
              s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId, " +
              s"numTasks=${numTasks}, numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
            return
          }
          if (partUrls.isEmpty) {
            Thread.sleep(100)
          } else {
            val locationClause = new StringBuilder("")
            val partIdInClause = new StringBuilder("")
            var i = 0
            partUrls.foreach {
              case (instanceId, gpfdistUrl) => {
                if (i > 0) {
                  locationClause.append(", ")
                }
                locationClause.append(s"'$gpfdistUrl'")
                i += 1
              }
            }
            this.synchronized{
              i = 0
              partIds.foreach(partId => {
                if (i > 0)
                  partIdInClause.append(", ")
                partIdInClause.append(partId.toString)
                i += 1
              })
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
            logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread pass ${sqlThreadPass.get()}: writeUUID=${writeUUID}, " +
              s"partUrlsCount=$partUrlsCount, partIdsCount=$partIdsCount, numActiveTasks=$numActiveTasks, activeWorkers='${activeWorkers.toString()}'")
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
              logger.info(s"${(System.currentTimeMillis() % 1000).toString} $createExtTbl")
              GPClient.executeStatement(conn, createExtTbl)
              var insertSelectSql = s"insert into $dbTableName (${colListInsert}) "
              insertSelectSql += s"select $colListSelect from $externalTableName"
              logger.info(s"SQL: $insertSelectSql")

              try {
                val nRows = GPClient.executeStatement(conn, insertSelectSql)
                val statusUpdate = s"update gp_spark_instance set status = '${nRows.toString}', " +
                  s"batch_no = ${sqlThreadPass.get()} where query_id = '${writeUUID}' " +
                  s"and part_id in (${partIdInClause.toString()})"
                logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                  s" nRows=${nRows} '$statusUpdate'")
                GPClient.executeStatement(conn, statusUpdate)
              } catch {
                case e: Exception => {
                  logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread writeUUID=${writeUUID} '${insertSelectSql}'" +
                    s" failed, ${e.getMessage}")
                  if (!aborted.get())
                    throw e
                }
              }
              if (!conn.getAutoCommit) {
                if (!aborted.get()) {
                  conn.commit()
                } else {
                  conn.rollback()
                }
              }
            } finally {
              try {
                GPClient.executeStatement(conn, s"drop external table if exists $externalTableName")
                if (!conn.getAutoCommit)
                  conn.commit()
              } catch {
                case e: Exception => logger.error(s"${e.getMessage}")
              }
            }
          }

          {
            val schedulingInfo = getSchedulingInfo
            numActiveTasks = schedulingInfo._1
            numCompletedTasks = schedulingInfo._2
            numFailedTasks = schedulingInfo._3
            numTasks = schedulingInfo._4
          }
          this.synchronized {
            // Wait for all active dataWriter commits
            while (!Thread.currentThread().isInterrupted && (numActiveTasks > 0) && partIds.nonEmpty) {
              val schedulingInfo = getSchedulingInfo
              numActiveTasks = schedulingInfo._1
              numCompletedTasks = schedulingInfo._2
              numFailedTasks = schedulingInfo._3
              numTasks = schedulingInfo._4
              Thread.sleep(10)
            }
          }
          nTasksLeft.addAndGet(- partIdsCount)
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread end of pass ${sqlThreadPass.get()}: " +
            s"writeUUID=${writeUUID}, partUrlsCount=${partUrlsCount}, " +
            s"numTasks=${numTasks}, numActiveTasks=${numActiveTasks}, numCompletedTasks=$numCompletedTasks, numFailedTasks=$numFailedTasks")
        }
      } finally {
        try {
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread terminated: pass ${sqlThreadPass.get()}:" +
            s" writeUUID=${writeUUID}, latchJobId=$latchJobId, latchStageId=$latchStageId")
          // sparkContext.cancelJobGroup(writeUUID)
          sparkContext.clearJobGroup()
          GPClient.executeStatement(conn, s"delete from gp_spark_instance where query_id = '${writeUUID}'")
          if (!conn.getAutoCommit)
            conn.commit()
          conn.close()
        } catch {
          case _: Throwable =>
        }
      }
    }
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
    this.synchronized {
      factory = new GreenplumDataWriterFactory(writeUUID, schema, saveMode, optionsFactory, 1)
      factories += factory
      nFact = numPartFactories.incrementAndGet()
    }
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} createWriterFactory " +
      s"writeUUID=${writeUUID} numGpSegments=$numGpSegments numPartFactories=${nFact}")
    if (nFact == 1) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} SqlThread(${sqlThread.getName}) start")
      sqlThread.start()
    }
    factory
  }

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    message match {
      case msg: GreenplumWriterCommitMessage if (msg.writeUUID.equals(writeUUID)) =>
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} onDataWriterCommit writeUUID=${writeUUID}, " +
          s"instanceId=${msg.instanceId}, node=${msg.gpfdistUrl}, " +
          s"epochId=${msg.epochId}, partitionId=${msg.partitionId}, taskId=${msg.taskId}, nRowsWritten=${msg.nRowsWritten}")
        this.synchronized{
          partIds.remove(msg.partitionId)
        }
      case _ =>
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} onDataWriterCommit ${message.toString}")
    }
  }

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} commit nCommitMessages=${writerCommitMessages.length}")
    done.set(true)
  }


  import java.util.concurrent.Executor
  import java.util.concurrent.Executors

  val executor: Executor = Executors.newFixedThreadPool(1)

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    aborted.set(true)
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} writeUUID=${writeUUID} abort nCommitMessages=${writerCommitMessages.length}")
    sparkContext.cancelJobGroup(writeUUID)
    sparkContext.clearJobGroup()
    try {
      dbConnection.abort(executor)
    } catch {
      case _: Throwable =>
    }
  }
}

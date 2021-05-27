package com.itsumma.gpconnector.reader

import java.lang.reflect.Method
import java.net.InetAddress
import java.nio.file.{Files, Path, Paths}
import com.itsumma.gpconnector.{ExternalProcessUtil, GPClient}
import com.typesafe.scalalogging.Logger
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, SparkEnv, SparkJobInfo, TaskContext}
import org.slf4j.LoggerFactory

import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import scala.io.BufferedSource
import scala.language.postfixOps
import scala.sys.process._

// scalafix:off DisableSyntax.null
// scalastyle:off null

class GreenplumInputPartitionReader(optionsFactory: GPOptionsFactory,
                                    queryId: String,
                                    instanceId: String,
                                    schema: StructType,
                                    partitionNo: Int,
                                    bufferSize: Int = 1024 * 1024,
                                    useFifo: Boolean = true) extends InputPartitionReader[InternalRow] {
  // val start: Long = System.nanoTime()
  val partitionId: Int = TaskContext.getPartitionId()
  //val rowEncoder: ExpressionEncoder[Row] = RowEncoder.apply(schema).resolveAndBind()
  val exId: String = SparkEnv.get.executorId
  private val tempDir: Path = Files.createDirectories(Paths.get("/tmp", "gp", "reader", queryId, instanceId, exId)) // port.toString
  val pipeFile: Path = Paths.get(tempDir.toString, s"$instanceId.pipe")
  val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  // val port: Int = 1808 + partitionNo
  private val (gpfdistProcess, port) = ExternalProcessUtil(optionsFactory, tempDir, inheritStdIO = true).gpfdistProcess(true)
  // private val serverOut: java.io.OutputStream = gpfdistProcess.getOutputStream
  val gpfdistUrl: String = s"gpfdist://${localIpAddress}:${port}/${pipeFile.getFileName.toString}"
  private var schemaOrPlaceholder = schema

  if (schemaOrPlaceholder.isEmpty) {
    schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
  }

  //  private val sparkContext = SparkContext.getOrCreate(SparkEnv.get.conf)
  //  private val listenerBus: AnyRef = sparkContext.getClass.getMethod("listenerBus").invoke(sparkContext)
  private var numPartsDetected = 0
  private val listenerBus: AnyRef = null
  //  private val statusTracker = sparkContext.statusTracker
  //  //private var jobInfo: Option[SparkJobInfo] = None
  //  private val activeStageIds = statusTracker.getActiveStageIds
  //  statusTracker.getJobIdsForGroup(queryId).foreach( jobId => {
  //    val jobInfo = statusTracker.getJobInfo(jobId)
  //    jobInfo match {
  //      case Some(jobInfoData) => {
  //        val stageIds = jobInfoData.stageIds().intersect(activeStageIds)
  //        stageIds.foreach(stageId => {
  //          statusTracker.getStageInfo(stageId) match {
  //            case Some(stageInfo) => logger.info(s"${(System.currentTimeMillis() % 1000).toString} Query=$queryId/$instanceId "
  //              + "stageId=" + stageId + "; name = " + stageInfo.name()
  //              + "; completed tasks:" + stageInfo.numCompletedTasks()
  //              + "; active tasks: " + stageInfo.numActiveTasks()
  //              + "; all tasks: " + stageInfo.numTasks()
  //              + "; submission time: " + stageInfo.submissionTime())
  //              numPartsDetected = stageInfo.numTasks()
  //              ///TODO: Instead, try to infer this number of partitions in the GreenplumDataSourceReader class
  //              /// via SparkListenerJobStart & friends event listener
  //            case None =>
  //          }
  //        })
  //      }
  //      case None =>
  //    }
  //  })

  Files.deleteIfExists(pipeFile)
  if (useFifo)
    s"mkfifo ${pipeFile.toString}".!
  logger.info(s"${(System.currentTimeMillis() % 1000).toString} gpfdist started on ${port}")
  private var postToAll: Method = null
  if (listenerBus != null) {
    //sparkContext.listenerBus
    val methods = listenerBus.getClass.getMethods
    methods.foreach(m => {
      if (m.getName.equals("post")) {
        val parameterTypes = m.getParameterTypes
        if (parameterTypes.length == 1) {
          logger.info(s"${(System.currentTimeMillis() % 1000).toString} Partition ${partitionId.toString}: listenerBus.${m.getName}(event) found")
          if (parameterTypes(0).isAssignableFrom(classOf[SparkListenerEvent]))
            postToAll = m
        }
      }
    })
  }

  val fieldDelimiter = '|'
  private var lines: Iterator[String] = Iterator.empty
  //val inputStream = new BufferedReader(new InputStreamReader(new FileInputStream(pipeFile.getFileName.toFile), "UTF-8"), bufferSize)
  private var dataLine: String = null
  logger.info(s"${(System.currentTimeMillis() % 1000).toString} Creating GreenplumInputPartitionReader instance on executor ${SparkEnv.get.executorId} for partition ${partitionId.toString}")
  private var source: BufferedSource = null //scala.io.Source.fromFile(pipeFile.toFile, "UTF-8", bufferSize) // Will block until server starts sending data!
  private val rowCount = new AtomicLong()
  rowCount.set(0)

  override def next(): Boolean = {
    //inputStream.ready()
    //dataLine = inputStream.readLine()
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} next called, rowCount=${rowCount.get().toString}")
    this.synchronized {
      if (lines != null && lines.hasNext) {
        dataLine = lines.next()
        return true
      }
      if (source == null) {
        if (postToAll != null) {
          val msg = GPInputPartitionReaderReadyEvent(queryId, instanceId, partitionId.toString, numPartsDetected, s"$gpfdistUrl")
          postToAll.invoke(listenerBus, msg)
        } else {
          //    throw new RuntimeException(s"GPreader partition ${partitionId.toString}: listenerBus.post unavailable :(")
          //    val dbConnection: Connection =
          var nRows = 0
          SparkSchemaUtil.using(GPClient.getConn(optionsFactory)) {
            conn => {
              conn.setAutoCommit(false)
//              GPClient.executeStatement(conn, s"insert into gp_spark_instance (query_id, instance_id, part_id, part_url) " +
//                s"values ('${queryId}', '${instanceId}', ${partitionId.toString}, '$gpfdistUrl')")
              nRows = GPClient.executeStatement(conn, s"update gp_spark_instance " +
                s"set part_id = ${partitionId.toString},  part_url = '$gpfdistUrl', dt_start = CURRENT_TIMESTAMP " +
                s"where query_id = '${queryId}' and instance_id = '${instanceId}' and status is null")
              conn.commit()
            }
          }
          if (nRows == 0) {
            logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} next returns false (task cancel), rowCount=${rowCount.get().toString}")
            dataLine = null
            lines = null
            return false
          }
        }
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} next will block until server starts sending data!, rowCount=${rowCount.get().toString}")
        source = scala.io.Source.fromFile(pipeFile.toFile, "UTF-8", bufferSize) // Will block until server starts sending data!
      }
      lines = source.getLines // Should block, if there is no data in the pipe. Should return an empty iterator on EOF
      if (lines == null || !lines.hasNext) {
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} next returns false, rowCount=${rowCount.get().toString}")
        dataLine = null
        lines = null
        return false
      }
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} next returns true, rowCount=${rowCount.get().toString}")
      dataLine = lines.next()
      true
    }
  }

  override def get(): InternalRow = {
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} get called, rowCount=${rowCount.get().toString}")
    this.synchronized {
      if (dataLine == null) {
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} get returns null, rowCount=${rowCount.get().toString}")
        return null
      }
      rowCount.incrementAndGet()
      val fields = if (schema.nonEmpty) dataLine.split(fieldDelimiter) else "".split(fieldDelimiter)
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} get returns '${dataLine}', rowCount=${rowCount.get().toString}")
      SparkSchemaUtil(optionsFactory.dbTimezone).textToInternalRow(schemaOrPlaceholder, fields)
    }
  }

  override def close(): Unit = synchronized {
    if (postToAll != null && listenerBus != null) {
      val msg = GPInputPartitionReaderCloseEvent(queryId, instanceId, partitionId.toString)
      postToAll.invoke(listenerBus, msg)
    } else {
      SparkSchemaUtil.using(GPClient.getConn(optionsFactory)) {
        conn => {
          conn.setAutoCommit(false)
          GPClient.executeStatement(conn, s"update gp_spark_instance set dt_end = now(), row_count = ${rowCount.get().toString} " +
            s"where  query_id = '${queryId}' and  instance_id = '${instanceId}' ")
          conn.commit()
        }
      }
    }
    if (source != null)
      source.close()
    source = null
    lines = Iterator.empty
    gpfdistProcess.destroy()
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl}, rowCount=${rowCount.get().toString}")
    Files.deleteIfExists(pipeFile)
    try {
      Files.deleteIfExists(tempDir)
    } catch { case _: Throwable => }
  }
}

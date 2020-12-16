package com.itsumma.gpconnector.reader

import java.lang.reflect.Method
import java.net.InetAddress
import java.nio.file.{Files, Path, Paths}

import com.itsumma.gpconnector.ExternalProcessUtil
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

import scala.language.postfixOps
import scala.sys.process._

// scalafix:off DisableSyntax.null
// scalastyle:off null

class GreenplumInputPartitionReader(optionsFactory: GPOptionsFactory,
                                    queryId: String,
                                    instanceId: String,
                                    schema: StructType,
                                    partitionNo: Int,
                                    bufferSize: Int = 1024*1024,
                                    useFifo: Boolean = true) extends InputPartitionReader[InternalRow] {
  val start: Long = System.nanoTime()
  val partitionId: Int = TaskContext.getPartitionId()
  val rowEncoder: ExpressionEncoder[Row] = RowEncoder.apply(schema).resolveAndBind()
  val port: Int = 1808 + partitionNo
  val exId: String = SparkEnv.get.executorId
  private val sparkContext = SparkContext.getOrCreate()
  private val listenerBus: AnyRef = sparkContext.getClass.getMethod("listenerBus").invoke(sparkContext)
  private val tempDir: Path = Files.createDirectories(Paths.get("/tmp", "gp", "reader", exId))
  val pipeFile: Path =  Paths.get(tempDir.toString, s"$instanceId.pipe")
  val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
  val gpfdistUrl: String = s"gpfdist://${localIpAddress}:${port}/${pipeFile.getFileName.toString}"
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val gpfdistProcess = ExternalProcessUtil(tempDir).gpfdistProcess(port).start()
  private var numPartsDetected = 0

  private val statusTracker = sparkContext.statusTracker
  //private var jobInfo: Option[SparkJobInfo] = None

  private val activeStageIds = statusTracker.getActiveStageIds
  statusTracker.getJobIdsForGroup(queryId).foreach( jobId => {
    val jobInfo = statusTracker.getJobInfo(jobId)
    jobInfo match {
      case Some(jobInfoData) => {
        val stageIds = jobInfoData.stageIds().intersect(activeStageIds)
        stageIds.foreach(stageId => {
          statusTracker.getStageInfo(stageId) match {
            case Some(stageInfo) => logger.info(s"Query=$queryId/$instanceId "
              + "stageId=" + stageId + "; name = " + stageInfo.name()
              + "; completed tasks:" + stageInfo.numCompletedTasks()
              + "; active tasks: " + stageInfo.numActiveTasks()
              + "; all tasks: " + stageInfo.numTasks()
              + "; submission time: " + stageInfo.submissionTime())
              numPartsDetected = stageInfo.numTasks()
              ///TODO: Instead, try to infer this number of partitions in the GreenplumDataSourceReader class
              /// via SparkListenerJobStart & friends event listener
            case None =>
          }
        })
      }
      case None =>
    }
  })

  Files.deleteIfExists(pipeFile)
  if (useFifo)
    s"mkfifo ${pipeFile.toString}".!
  logger.info(s"gpfdist started on ${port}")
  private var postToAll: Method = null
  if (listenerBus != null) {
    //sparkContext.listenerBus
    val methods = listenerBus.getClass.getMethods
    methods.foreach(m => {
      if (m.getName.equals("post")) {
        val parameterTypes = m.getParameterTypes
        if (parameterTypes.length == 1) {
          logger.info(s"Partition ${partitionId.toString}: listenerBus.${m.getName}(event) found")
          if (parameterTypes(0).isAssignableFrom(classOf[SparkListenerEvent]))
            postToAll = m
        }
      }
    })
  }
  if (postToAll != null) {
    val msg = GPInputPartitionReaderReadyEvent(queryId, instanceId, partitionId.toString, numPartsDetected, s"$gpfdistUrl")
    postToAll.invoke(listenerBus, msg)
  } else {
    throw new RuntimeException(s"GPreader partition ${partitionId.toString}: listenerBus.post unavailable :(")
  }
  val fieldDelimiter = '|'
  private var lines: Iterator[String] = Iterator.empty
  //val inputStream = new BufferedReader(new InputStreamReader(new FileInputStream(pipeFile.getFileName.toFile), "UTF-8"), bufferSize)
  private var dataLine: String = ""
  logger.info(s"Creating GreenplumInputPartitionReader instance on ${SparkEnv.get.executorId} for partition ${partitionId.toString}")
  private val source = scala.io.Source.fromFile(pipeFile.toFile, "UTF-8", bufferSize) // Will block until server starts sending data!

  override def next(): Boolean = {
    //inputStream.ready()
    //dataLine = inputStream.readLine()
    if (lines != null && lines.hasNext) {
      dataLine = lines.next()
      return true
    }
    lines = source.getLines // Should block, if there is no data in the pipe. Should return an empty iterator on EOF
    if (lines == null || !lines.hasNext) {
      dataLine = ""
      return false
    }
    dataLine = lines.next()
    true
  }

  override def get(): InternalRow = {
    if (dataLine == null || dataLine.length == 0) return null
    val fields = dataLine.split(fieldDelimiter)
    /*val row = org.apache.spark.sql.Row.fromSeq(fields)
    rowEncoder.toRow(row)*/
    SparkSchemaUtil(optionsFactory.dbTimezone).textToInternalRow(schema, fields)
  }

  override def close(): Unit = {
    if (postToAll != null && listenerBus != null) {
      val msg = GPInputPartitionReaderCloseEvent(queryId, instanceId, partitionId.toString)
      postToAll.invoke(listenerBus, msg)
    }
    source.close()
    lines = Iterator.empty
    gpfdistProcess.destroy()
    logger.info(s"gpfdist destroyed on ${port}")
    Files.deleteIfExists(pipeFile)
  }
}

package com.itsumma.gpconnector.rmi

import com.itsumma.gpconnector.GPClient
import com.itsumma.gpconnector.gpfdist.WebServer
import com.itsumma.gpconnector.rmi.RMISlave.{clientSocketFactory, serverSocketFactory}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
//import java.rmi.Naming
import java.rmi.server.{UnicastRemoteObject, Unreferenced}
import scala.collection.mutable.{Queue => MutableQueue, Set => MutableSet}
import com.typesafe.scalalogging.Logger
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import java.rmi.registry.{LocateRegistry, Registry}
import java.util.concurrent.atomic.AtomicBoolean

/**
 *   Provides data exchange facility within its interconnect area which corresponds to
 * particular GP segment, so Spark executors serving this segment can communicate data
 * to each other. This is helpful in the situation when number of gpfdist service instances
 * is less then the number of Spark partitions scheduled.
 *   <p> Also provides data buffering capabilities.
 *   Communication is thread-safe.
 */
@remote trait ITSIpcClient {
  /**
   * UUID unique for this instance
   * @return String
   */
  def name: String

  /**
   * IP of this instance
   * @return
   */
  def hostAddress: String

  /**
   * GP segment served by this instance
   * @return
   */
  def segId: String

  /**
   * Does this instance hold a gpfdist service?
   * @return Boolean
   */
  def segService: Boolean

  /**
   * Called by ITSIpcServer implementation to communicate participant lists within segment.
   * @param participants Set[ITSIpcClient]
   */
  //def participantsListUpdate(participants: Set[ITSIpcClient]): Unit

  /**
   * Below are the data communication functions:
   */

  /**
   * Retrieve a portion of data from the segment data stream, or null
   * if the stream is terminated and there is no more data buffered.
   * Blocks until data available.
   * Throws exception if timeout elapsed.
   * @param msMax Int maximum wait time in ms
   * @return Array[Byte]
   */
  def get(msMax: Long = 60000): Array[Byte]

  /**
   * Closes the segment data stream on behalf of sender.
   * When all senders close the stream, it is considered terminated.
   * @param sender ITSIpcClient
   * @return Boolean, true if the stream is terminated
   */
  def close(sender: ITSIpcClient): Boolean

  /**
   * Registers the sender for this instance data stream and put data into the buffer.
   * The stream is considered open when at least one sender is registered.
   * @param sender ITSIpcClient
   * @param data String
   * @return true
   */
  def put(sender: ITSIpcClient, data: Array[Byte]): Boolean

  /**
   * Number of characters left in the buffer.
   * @return Int
   */
  def size: Int
}

/**
 * Provides RMI interface for transaction coordinator to gain this instance control
 */
@remote trait TaskHandler {
  def coordinatorAsks(pcb: PartitionControlBlock, func: String, msMax: Long = 60000): PartitionControlBlock
}

object RMISlave {
  val (localHotName: String, localIpAddress: String) = NetUtils().getLocalHostNameAndIp
  //InetAddress.getLocalHost.getHostAddress
  private val inetAddress: InetAddress = InetAddress.getByName(
    InetAddress.getByName(NetUtils().resolveHost2Ip(localIpAddress)).getHostName)
  val serverSocketFactory: ServerSocketFactory = ServerSocketFactory(inetAddress)
  val clientSocketFactory: ClientSocketFactory = ClientSocketFactory(inetAddress)
}

/**
 * Implements {@link ITSIpcClient} and {@link TaskHandler} RMI interfaces.
 * @param optionsFactory GPOptionsFactory instance
 * @param serverAddress String in the form IP:port of the RMI registry instance
 * @param queryId String UUID
 * @param readOrWrite Boolean - true for read, false for write transaction
 * @param executorId String from SparkEnv.get.executorId
 * @param partitionId Int as passed to DataWriter/DataReader interfaces
 * @param taskId Int as passed to DataWriter/DataReader interfaces
 * @param epochId Int as passed to DataWriter/DataReader interfaces
 */
class RMISlave(optionsFactory: GPOptionsFactory, serverAddress: String, queryId: String,
               readOrWrite: Boolean,
               executorId: String, partitionId: Int, taskId: Long, epochId: Long)
  extends UnicastRemoteObject(0, clientSocketFactory, serverSocketFactory)
    with ITSIpcClient
    with TaskHandler
    with Unreferenced
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val streamingBatchId = TaskContext.get.getLocalProperty("streaming.sql.batchId")
  private val isContinuousProcessing = TaskContext.get.getLocalProperty("__is_continuous_processing")
  private val instanceId: String = s"$partitionId:$taskId:$epochId"
  private val writerReady = new AtomicBoolean(readOrWrite)
  private val (instanceHostName: String, instanceHostAddress: String) = NetUtils().getLocalHostNameAndIp
    //InetAddress.getLocalHost.getHostAddress
  // private val instanceId = UUID.randomUUID.toString
  logger.info(s"Starting executor instance for ${if (readOrWrite) "read" else "write"} " +
    s"${queryId}/${instanceId}/${executorId} on ${instanceHostName}/${instanceHostAddress}" +
    s", server=${serverAddress}, streamingBatchId=$streamingBatchId, isContinuousProcessing=$isContinuousProcessing")
  var server: ITSIpcServer = try {
    //Naming..lookup(s"rmi://${serverAddress}/com/itsumma/gpconnector/rmi/RMIMaster") match {
    val registry: Registry = LocateRegistry.getRegistry(serverAddress.split(":")(0), serverAddress.split(":")(1).toInt)
    registry.lookup(s"com/itsumma/gpconnector/${queryId}") match {
      case server: ITSIpcServer => server.asInstanceOf[ITSIpcServer]
      case server: TaskCoordinator => server.asInstanceOf[ITSIpcServer]
      case wrong => throw new Exception(s"Unknown remote class ${wrong.getClass.getCanonicalName}")
    }
  } catch {
    case e: Exception =>
      val msg = s"Unable connect from ${instanceHostAddress}/${instanceId} to RMIMaster at ${serverAddress}: " +
        s"${e.getClass.getCanonicalName} " +
        s"${e.getMessage}"
      if (!readOrWrite) {
        logger.error(msg)
        throw e
      }
      logger.info(msg)
      null
  }
  // private var partList: Set[ITSIpcClient] = Set[ITSIpcClient]() //server.participantsList(instanceSegmentId)
  //private val dataQueue: MutableQueue[String] = MutableQueue[String]()
  private var dataQueue: ByteBuffer = ByteBuffer.allocate(10000000)
  private val bufferSize = optionsFactory.bufferSize
  private var localWriteBuffer: ByteBuffer = ByteBuffer.allocate(bufferSize)
  private val dataProviders: MutableSet[ITSIpcClient] = MutableSet[ITSIpcClient]()
  private var nDataProvidersLeft: Int = 0
  var connected: Boolean = false
  private var instanceSegmentId: String = null
  private var isServiceProvider: Boolean = false
  private var pcb = PartitionControlBlock(this.asInstanceOf[TaskHandler], queryId, partitionId, instanceId,
    instanceHostAddress, if (readOrWrite) "R" else "W", executorId)
  private var webServer: WebServer = null
  var gpfdistUrl: String = null
  var sqlTransferComplete: AtomicBoolean = new AtomicBoolean(false)
  val jobAbort: AtomicBoolean = new AtomicBoolean(false)

  try {
    if (server != null) {
      pcb = server.asInstanceOf[TaskCoordinator].handlerAsks(pcb, "checkIn", optionsFactory.networkTimeout)
      instanceSegmentId = pcb.gpSegmentId
      gpfdistUrl = pcb.gpfdistUrl
    }
    if (instanceSegmentId != null) {
      connected = true
    } else {
      sqlTransferComplete.set(true)
    }
  } catch {
    case ex: java.rmi.NoSuchObjectException => logger.debug(s"${ex.getClass.getName} ${ex.getMessage}")
  }

  def servicePort: Int = this.synchronized {
    if (webServer == null)
      return 0
    webServer.httpPort
  }

  override def coordinatorAsks(pcb: PartitionControlBlock, func: String, msMax: Long): PartitionControlBlock = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var retPcb: PartitionControlBlock = coordinatorAsksTry(pcb, func)
    while ((retPcb == null) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      Thread.sleep(rnd.nextInt(100) + 1)
      retPcb = coordinatorAsksTry(pcb, func)
    }
    if (isServiceProvider && (retPcb != null)) {
      while (!Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)
        && !GPClient.checkGpfdistIsUp(instanceHostAddress, webServer.httpPort)) {
        Thread.sleep(rnd.nextInt(100) + 1)
      }
    }
    retPcb
  }

  def coordinatorAsksTry(newPcb: PartitionControlBlock, func: String): PartitionControlBlock = this.synchronized {
    var retPcb: PartitionControlBlock = null
    var msg: String = ""
    func match {
      case "startService" => {
        isServiceProvider = true
        var port = optionsFactory.serverPort // By default port == 0 and WebServer assigns some available
        webServer = new WebServer(port, this, jobAbort, sqlTransferComplete)
        port = webServer.httpPort
        if (port == 0)
          throw new Exception(s"webServer.httpPort returns 0")
        gpfdistUrl = s"gpfdist://${instanceHostAddress}:${port}/output.pipe"
        retPcb = newPcb.copy(gpfdistUrl = gpfdistUrl)
        msg = s"GPFDIST service started at ${retPcb}"
      }
      case "sqlTransferComplete" => {
        sqlTransferComplete.set(true)
        retPcb = newPcb.copy()
        msg = s"${retPcb}"
      }
      case unknownFunc => throw new Exception(s"Unknown call: ${unknownFunc}")
    }
    logger.info(s"\ncoordinatorAsks: ${func}, ${msg}")
    retPcb
  }

  private var callNo: Long = 0
  private val rmiDataTargetGuard: Boolean = false
  private var rmiDataTarget: ITSIpcClient = null
  private var rmiThis: ITSIpcClient = this.asInstanceOf[ITSIpcClient]

  def write(dataBytes: Array[Byte]): Unit = localWriteBuffer.synchronized {
    callNo += 1
    if (localWriteBuffer.capacity() < dataBytes.length) {
      /*
            throw new Exception(s"Row size exceeds buffer size of ${bufferSize} bytes. " +
              s"Increase buffer.size connector parameter to at least ${dataBytes.length}.")
      */
      flushInternal()
      localWriteBuffer = ByteBuffer.allocate(dataBytes.length)
    }
    if (callNo == 1) {
      localWriteBuffer.put(dataBytes)
      flushInternal()
    } else {
      if (localWriteBuffer.remaining() < dataBytes.length)
        flushInternal()
      localWriteBuffer.put(dataBytes)
    }
  }

  def write(data: String): Unit = {
    val dataBytes = data.getBytes(StandardCharsets.UTF_8)
    write(dataBytes)
  }

  def flush(): Unit = localWriteBuffer.synchronized {
    flushInternal()
  }

  private def flushInternal(): Unit = {
    localWriteBuffer.flip()
    if (localWriteBuffer.remaining() > 0) {
      val dataBytes = new Array[Byte](localWriteBuffer.remaining())
      localWriteBuffer.get(dataBytes)
      //val data = new String(dataBytes, StandardCharsets.UTF_8)
      if (!jobAbort.get()) {
        rmiDataTargetGuard.synchronized {
          if (rmiDataTarget == null)
            rmiDataTarget = getSegServiceProvider
          if (rmiThis == rmiDataTarget) {
            putLocal(this.asInstanceOf[ITSIpcClient], dataBytes)
          } else {
            rmiDataTarget.put(this.asInstanceOf[ITSIpcClient], dataBytes)
          }
        }
      }
    }
    localWriteBuffer.clear()
  }

  def commit(rowCount: Long, msMax: Long = 60000): Unit = localWriteBuffer.synchronized {
    if (!connected) {
      logger.debug(s"Commit called, but client has never been connected")
      return
    }
    writerReady.set(true)
    pcb = pcb.copy(rowCount = rowCount)
    logger.info(s"Calling commit on ${pcb}")
    try {
      pcb = server.asInstanceOf[TaskCoordinator].handlerAsks(pcb, "commit", msMax)
    } catch {
      case e: java.rmi.NoSuchObjectException =>
        logger.info(s"handlerAsks('commit',pcb) called with server that is already shut down: pcb=${pcb}")
    }
    flushInternal()
    val closed: Boolean = if (rmiDataTarget != null) {
      rmiDataTarget.close(this.asInstanceOf[ITSIpcClient])
    } else false
    if (!closed)
      logger.debug(s"rmiDataTarget.close = false, ${pcb}")
    // Locks until GPFDIST transfer complete
    if (!NetUtils().waitForCompletion(msMax) {
      sqlTransferComplete.get()
    })
      throw new Exception(s"GPFDIST transfer incomplete")
    rmiDataTargetGuard.synchronized {
      rmiDataTarget = null
    }
  }

  def abort(rowCount: Long, msMax: Long = 60000): Unit = { //localWriteBuffer.synchronized {
    if (!connected) {
      logger.debug(s"Abort called, but client has never been connected")
      return
    }
    pcb = pcb.copy(rowCount = rowCount)
    try {
      pcb = server.asInstanceOf[TaskCoordinator].handlerAsks(pcb, "abort")
    } catch {
      case ex: java.rmi.NoSuchObjectException => logger.debug(s"${ex.getClass.getName} ${ex.getMessage}")
    }
    jobAbort.set(true)
    val rnd = new scala.util.Random
    val start = System.currentTimeMillis()
    while (!sqlTransferComplete.get() && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      Thread.sleep(rnd.nextInt(100) + 1)
    }
    rmiDataTargetGuard.synchronized {
      if (rmiDataTarget != null) {
        rmiDataTarget.close(this.asInstanceOf[ITSIpcClient])
        rmiDataTarget = null
      }
    }
  }

  def stop: Long = this.synchronized {
    try {
      if (connected && server != null) {
        server.disconnect(pcb)
        connected = false
      }
    } catch {
      case e: java.rmi.NoSuchObjectException =>
        logger.info(s"server.disconnect(pcb) called with server that is already shut down: pcb=${pcb}")
      case e: Exception => logger.warn(s"server.disconnect(pcb) failed: pcb=${pcb}, ${e}")
    }
    server = null
    rmiThis = null
    rmiDataTarget = null
    connected = false
    var rmiLoopMs: Long = 0
    if (webServer != null) {
      rmiLoopMs = webServer.rmiLoopMs.get()
      //webServer.server.stop(1)
      webServer.stop
      webServer = null
      logger.debug(s"Web server instance terminated (${gpfdistUrl})")
    }
    try {
      val success = UnicastRemoteObject.unexportObject(this, true)
      logger.info(s"unexportObject(pcb=${pcb})=${success}")
    } catch {
      case e: Exception => logger.warn(s"unexportObject failed: pcb=${pcb}, ${e.getClass.getCanonicalName}")
    }
    dataProviders.clear()
    rmiLoopMs
  }

  override def name: String = instanceId

  override def hostAddress: String = instanceHostAddress

  override def segId: String = instanceSegmentId

  override def segService: Boolean = isServiceProvider

/*
  override def participantsListUpdate(participants: Set[ITSIpcClient]): Unit = this.synchronized {
    partList = participants
  }
*/

  private var getSucceededOnce: Boolean = false

  private def tryGet: Array[Byte] = this.synchronized {
    var ret: Array[Byte] = null
    if (dataQueue.position() > 0) {
      dataQueue.flip()
      ret = new Array[Byte](dataQueue.remaining())
      dataQueue.get(ret)
      dataQueue.clear()
    }
    if (!sqlTransferComplete.get()) {
      if (ret == null) {
        if (dataProviders.nonEmpty || (readOrWrite && connected && !getSucceededOnce) || !writerReady.get())
          ret = new Array[Byte](0) // Will block
      }
    }
    if ((ret != null) && ret.nonEmpty && !getSucceededOnce) {
      getSucceededOnce = true
      logger.debug(s"getSucceededOnce=true pcb=${pcb}")
    }
    ret
  }

  val getWillBlock: AtomicBoolean = new AtomicBoolean(false)

  override def get(msMax: Long = 60000): Array[Byte] = {
    val rnd = new scala.util.Random
    val start = System.currentTimeMillis()
    var ret: Array[Byte] = tryGet
    getWillBlock.set(false)
    while (!Thread.currentThread().isInterrupted
      && ((msMax < 0) || (System.currentTimeMillis() - start) < msMax)
      && (ret != null) && ret.isEmpty) {
      if (!getWillBlock.get()) {
        getWillBlock.set(true)
        logger.debug(s"Get will block, instanceSegmentId=${instanceSegmentId}")
      }
      Thread.sleep(rnd.nextInt(10) + 1)
      ret = tryGet
    }
    if ((ret != null) && ret.isEmpty)
      throw new Exception(s"Time limit elapsed (${msMax}) waiting data from instanceId=${instanceId}")
    ret
  }

  override def close(sender: ITSIpcClient): Boolean = this.synchronized {
    var ret: Boolean = false
    if (dataProviders.contains(sender))
      dataProviders -= sender
    if (dataProviders.isEmpty && (dataQueue.position() == 0)) {
      // if (sender == this.asInstanceOf[ITSIpcClient]) {
      if (sender.segService) {
        if (nDataProvidersLeft <= 1) {
          ret = true
          nDataProvidersLeft = 0
        }
      } else {
        ret = true
        nDataProvidersLeft -= 1
      }
    }
    ret
  }

  override def put(sender: ITSIpcClient, data: Array[Byte]): Boolean = {
    putLocal(sender, data)
  }

  private def putLocal(sender: ITSIpcClient, data: Array[Byte]): Boolean = this.synchronized {
    if (!dataProviders.contains(sender)) {
      dataProviders += sender
      nDataProvidersLeft += 1
    }
    if (dataQueue.remaining() < data.length) {
      val newDataQueue = ByteBuffer.allocate(dataQueue.position() + data.length)
      newDataQueue.put(dataQueue.array(), 0, dataQueue.position())
      dataQueue = newDataQueue
    }
    dataQueue.put(data)
    writerReady.set(true)
    true
  }

  override def size: Int = this.synchronized {
    dataQueue.position()
  }

  /**
   * Returns gpfdist instance holder serving this instance segment
   * @return ITSIpcClient
   */
  def getSegServiceProvider: ITSIpcClient = this.synchronized {
    rmiDataTargetGuard.synchronized {
      if (!connected) {
        logger.debug(s"getSegServiceProvider called, but client has never been connected")
        return null
      }
      rmiDataTarget = server.getSegServiceProvider(instanceSegmentId)
      if (rmiDataTarget == null)
        throw new Exception(s"Unable to find GPFDIST service provider for GP segment ${instanceSegmentId}, pcb=${pcb}")
      rmiDataTarget
    }
  }

  override def unreferenced(): Unit = {
    stop
  }
}

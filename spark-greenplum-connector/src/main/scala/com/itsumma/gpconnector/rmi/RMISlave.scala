package com.itsumma.gpconnector.rmi

import com.itsumma.gpconnector.GPClient
import com.itsumma.gpconnector.gpfdist.{WebServer, WebServerMetrics}
import com.itsumma.gpconnector.rmi.RMISlave.{clientSocketFactory, serverSocketFactory}
import com.itsumma.gpconnector.utils.ProgressTracker
import org.apache.spark.internal.Logging
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory

import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
//import java.rmi.Naming
import java.rmi.server.{UnicastRemoteObject, Unreferenced}
import scala.collection.mutable.{Queue => MutableQueue, Set => MutableSet}
import org.apache.spark.TaskContext

import java.rmi.registry.{LocateRegistry, Registry}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Provides data exchange facility within its interconnect area which corresponds to
 * particular GP segment, so Spark executors serving this segment can communicate data
 * to each other. This is helpful in the situation when number of gpfdist service instances
 * is less then the number of Spark partitions scheduled.
 * <p> Also provides data buffering capabilities.
 * Communication is thread-safe.
 */
@remote
trait ITSIpcClient extends java.rmi.Remote {
  /**
   * UUID unique for this instance
   *
   * @return String
   */
  // @throws[java.rmi.RemoteException]
  def name: String

  /**
   * IP of this instance
   *
   * @return
   */
  // @throws[java.rmi.RemoteException]
  def hostAddress: String

  /**
   * GP segment served by this instance
   *
   * @return
   */
  // @throws[java.rmi.RemoteException]
  def segId: String

  /**
   * Does this instance hold a gpfdist service?
   *
   * @return Boolean
   */
  // @throws[java.rmi.RemoteException]
  def segService: Boolean

  /**
   * Called by ITSIpcServer implementation to communicate participant lists within segment.
   *
   * @param participants Set[ITSIpcClient]
   */
  //def participantsListUpdate(participants: Set[ITSIpcClient]): Unit

  /**
   * Below are the data communication functions:
   */

  /**
   * Closes the segment data stream on behalf of sender.
   * When all senders close the stream, it is considered terminated.
   *
   * @param sender ITSIpcClient
   * @return Boolean, true if the stream is terminated
   */
  // @throws[java.rmi.RemoteException]
  def close(sender: ITSIpcClient): Boolean

  /**
   * Registers the sender for this instance data stream and put data into the buffer.
   * The stream is considered open when at least one sender is registered.
   *
   * @param sender ITSIpcClient
   * @param data   String
   * @return true
   */
  // @throws[java.rmi.RemoteException]
  def put(sender: ITSIpcClient, data: Array[Byte]): Boolean

  /**
   * Flush the output buffer on this target.
   */
  def flush(): Unit

  /**
   * Number of characters left in the buffer.
   *
   * @return Int
   */
  // @throws[java.rmi.RemoteException]
  def size: Int
}

/**
 * Provides RMI interface for transaction coordinator to gain this instance control
 */
@remote
trait TaskHandler extends java.rmi.Remote {
  // @throws[java.rmi.RemoteException]
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
 *
 * @param optionsFactory GPOptionsFactory instance
 * @param serverAddress  String in the form IP:port of the RMI registry instance
 * @param queryId        String UUID
 * @param readOrWrite    Boolean - true for read, false for write transaction
 * @param executorId     String from SparkEnv.get.executorId
 * @param partitionId    Int as passed to DataWriter/DataReader interfaces
 * @param taskId         Int as passed to DataWriter/DataReader interfaces
 * @param epochId        Int as passed to DataWriter/DataReader interfaces
 */
class RMISlave(optionsFactory: GPOptionsFactory, serverAddress: String, queryId: String,
               readOrWrite: Boolean,
               executorId: String, partitionId: Int, taskId: Long, epochId: Long)
  extends UnicastRemoteObject(0, clientSocketFactory, serverSocketFactory)
    with ITSIpcClient
    with TaskHandler
    with Unreferenced
    with Logging
{
  private val progressTracker: ProgressTracker = new ProgressTracker()
  private val streamingBatchId = TaskContext.get.getLocalProperty("streaming.sql.batchId")
  private val isContinuousProcessing = TaskContext.get.getLocalProperty("__is_continuous_processing")
  private val instanceId: String = s"$partitionId:$taskId:$epochId"
  private val writerReady = new AtomicBoolean(readOrWrite)
  private val (instanceHostName: String, instanceHostAddress: String) = NetUtils().getLocalHostNameAndIp
  //InetAddress.getLocalHost.getHostAddress
  // private val instanceId = UUID.randomUUID.toString
  logInfo(s"Starting executor instance for ${if (readOrWrite) "read" else "write"} " +
    s"${queryId}/${instanceId}/${executorId} on ${instanceHostName}/${instanceHostAddress}" +
    s", server=${serverAddress}, streamingBatchId=$streamingBatchId, isContinuousProcessing=$isContinuousProcessing")
  var server: ITSIpcServer = try {
    //Naming..lookup(s"rmi://${serverAddress}/com/itsumma/gpconnector/rmi/RMIMaster") match {
    val registry: Registry = LocateRegistry.getRegistry(serverAddress.split(":")(0), serverAddress.split(":")(1).toInt)
    registry.lookup(s"com/itsumma/gpconnector/${queryId}") match {
      case server: ITSIpcServer => server
      case server: TaskCoordinator => server.asInstanceOf[ITSIpcServer]
      case wrong => throw new Exception(s"Unknown remote class ${wrong.getClass.getCanonicalName}")
    }
  } catch {
    case e: Exception =>
      val msg = s"Unable connect from ${instanceHostAddress}/${instanceId} to RMIMaster at ${serverAddress}: " +
        s"${e.getClass.getCanonicalName} " +
        s"${e.getMessage}"
      if (!readOrWrite) {
        logError(msg)
        throw e
      }
      logInfo(msg)
      null
  }
  // private var partList: Set[ITSIpcClient] = Set[ITSIpcClient]() //server.participantsList(instanceSegmentId)
  private val localWriteBufferGuard: Boolean = false
  private var buffExchange: BufferExchange = new BufferExchange(optionsFactory.bufferSize)
  private val dataProviders: MutableSet[ITSIpcClient] = MutableSet[ITSIpcClient]()
  private val transactionStarted: LocalEvent = new LocalEvent(true, name = "trnStart")
  private val providersDisconnected: LocalEvent = new LocalEvent(true, "srcDone")
  private val nSourcesPlan: AtomicInteger = new AtomicInteger(0)
  private val nSourcesDone: AtomicInteger = new AtomicInteger(0)
  var connected: Boolean = false
  private var instanceSegmentId: String = null
  private var isServiceProvider: Boolean = false
  private var pcb = PartitionControlBlock(this.asInstanceOf[TaskHandler], queryId, partitionId, instanceId,
    instanceHostAddress, if (readOrWrite) "R" else "W", executorId)
  private val webServerMetrics: WebServerMetrics = new WebServerMetrics()
  private var webServer: WebServer = null
  var gpfdistUrl: String = null
  private val coordinatorSqlComplete: AtomicBoolean = new AtomicBoolean(false)
  private val atLeastOnePostComplete: AtomicBoolean = new AtomicBoolean(false)
  val sqlTransferComplete: AtomicBoolean = new AtomicBoolean(false)
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
    case ex: java.rmi.NoSuchObjectException => logDebug(s"${ex.getClass.getName} ${ex.getMessage}")
  }

  def webLoopMs: Long = {
    webServerMetrics.webLoopMs.get()
  }

  def transferBytes: Long = {
    webServerMetrics.totalBytes.get()
  }

  def transferMs: Long = {
    webServerMetrics.processingMs.get()
  }

  def gpfReport: String = {
    if (webServer == null)
      return ""
    webServer.progressTracker.reportTimeTaken()
  }

  def servicePort: Int = this.synchronized {
    if (webServer == null)
      return 0
    webServer.httpPort
  }

  // @throws[java.rmi.RemoteException]
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

  private def coordinatorAsksTry(newPcb: PartitionControlBlock, func: String): PartitionControlBlock = this.synchronized {
    var retPcb: PartitionControlBlock = null
    var msg: String = ""
    func match {
      case "startService" => {
        isServiceProvider = true
        var port = optionsFactory.serverPort // By default port == 0 and WebServer assigns some available
        webServer = new WebServer(port, this, jobAbort, atLeastOnePostComplete, webServerMetrics)
        port = webServer.httpPort
        if (port == 0)
          throw new Exception(s"webServer.httpPort returns 0")
        gpfdistUrl = s"gpfdist://${instanceHostAddress}:${port}/output.pipe"
        retPcb = newPcb.copy(gpfdistUrl = gpfdistUrl)
        msg = s"GPFDIST service started at ${retPcb}"
      }
      case "sqlTransferComplete" => {
        if (isServiceProvider && readOrWrite) {
          flush()
        }
        if (!readOrWrite) {
          sqlTransferComplete.set(true)
        } else {
          coordinatorSqlComplete.set(true)
        }
        retPcb = newPcb.copy()
        msg = s"${retPcb}"
      }
      case "sqlTransferAbort" => {
        jobAbort.set(true)
        coordinatorSqlComplete.set(true)
        retPcb = newPcb.copy()
        msg = s"${retPcb}"
      }
      case unknownFunc => throw new Exception(s"Unknown call: ${unknownFunc}")
    }
    logInfo(s"\ncoordinatorAsks: ${func}, ${msg}")
    retPcb
  }

  private val firstRow: AtomicBoolean = new AtomicBoolean(true)
  private val rmiDataTargetGuard: Boolean = false
  private var rmiDataTarget: ITSIpcClient = null
  private var rmiDataTargetInstId: String = null
  private var rmiThis: ITSIpcClient = this.asInstanceOf[ITSIpcClient]

  def write(dataBytes: Array[Byte]): Unit = {
    logTrace(s"write enter, length=${dataBytes.length}")
    localWriteBufferGuard.synchronized {
      putLocal(rmiThis, dataBytes)
      if (firstRow.get()) {
        if (readOrWrite) { ///TODO: for read only?
          buffExchange.flush()
        } else {
          transactionStarted.reset()
        }
        firstRow.set(false)
      } else {
        if (buffExchange.available > 0) {
          toRmiTarget()
        }
      }
    }
    logTrace("write exit")
  }

  def write(data: String): Unit = {
    val dataBytes = data.getBytes(StandardCharsets.UTF_8)
    write(dataBytes)
  }

  override def flush(): Unit = localWriteBufferGuard.synchronized {
    buffExchange.flush()
    toRmiTarget()
  }

  private def toRmiTarget(): Unit = {
    val available = buffExchange.available
    logTrace(s"toRmiTarget enter, instanceId=${instanceId} available=${available}")
    if (available > 0) {
      if (!jobAbort.get()) {
        rmiDataTargetGuard.synchronized {
          if (rmiDataTarget == null) throw new Exception("Call rmiSlave.getSegServiceProvider method somewhere")
            //rmiDataTarget = getSegServiceProvider
          if (instanceId != rmiDataTargetInstId) {
          //if (rmiThis != rmiDataTarget) {
            var buff = buffExchange.get()
            while (buff != null) {
              if (buff.position() > 0)
                rmiDataTarget.put(this.asInstanceOf[ITSIpcClient], util.Arrays.copyOfRange(buff.array(), 0, buff.position()))
              buffExchange.addBuffer(buff)
              buff = buffExchange.get()
            }
            rmiDataTarget.flush()
          }
        }
      } else {
        buffExchange.clear()
      }
    }
    logTrace(s"toRmiTarget exit, instanceId=${instanceId}")
  }

  def commit(rowCount: Long, msMax: Long = 60000): Unit = {
    logTrace(s"commit enter, instanceId=${instanceId}")
    var closed: Boolean = false
    if (!connected) {
      logDebug(s"Commit called, but client has never been connected")
      return
    }
    if (!readOrWrite) {
      if (isServiceProvider) {
        rmiDataTargetGuard.synchronized {
          closed = close(this.asInstanceOf[ITSIpcClient])
        }
        if (!transactionStarted.waitForEvent(msMax) || !providersDisconnected.waitForEvent(msMax)) {
          jobAbort.set(true)
          throw new Exception(s"Time limit elapsed (${msMax}) waiting uplink data end for instanceId=${instanceId}")
        }
      }
    }
    if (!NetUtils().waitForCompletion(msMax) {
      if (buffExchange.available == 0)
        flush()
      buffExchange.totalDrain.get() == buffExchange.totalFill.get()
    }) {
      jobAbort.set(true)
      throw new Exception(s"Buffer transfer incomplete for instanceId=${instanceId}: " +
        s"fillBytes=${buffExchange.totalFill.get()}, drainBytes=${buffExchange.totalDrain.get()}")
    }
    pcb = pcb.copy(rowCount = rowCount)
    logInfo(s"Calling commit on ${pcb}")
    try {
      pcb = server.asInstanceOf[TaskCoordinator].handlerAsks(pcb, "commit", msMax)
    } catch {
      case e: java.rmi.NoSuchObjectException =>
        logInfo(s"handlerAsks('commit',pcb) called with server that is already shut down: pcb=${pcb}")
    }
    writerReady.set(true)
    rmiDataTargetGuard.synchronized {
      closed = if ((rmiDataTarget != null) && (rmiDataTarget != rmiThis)) {
        rmiDataTarget.close(this.asInstanceOf[ITSIpcClient])
      } else closed //close(this.asInstanceOf[ITSIpcClient])
    }
    if (!closed)
      logDebug(s"rmiDataTarget.close = false, ${pcb}")
    // Locks until GPFDIST transfer complete
    logTrace(s"commit: sqlTransferComplete=${sqlTransferComplete.get()}")
    if (!NetUtils().waitForCompletion(msMax) {
      sqlTransferComplete.get()
    })
      throw new Exception(s"GPFDIST transfer incomplete")
    rmiDataTargetGuard.synchronized {
      rmiDataTarget = null
    }
    rmiThis = null
    buffExchange.clear()
    buffExchange = null
    logTrace(s"commit exit, instanceId=${instanceId}")
  }

  def abort(rowCount: Long, msMax: Long = 60000): Unit = { //localWriteBuffer.synchronized {
    logTrace(s"commit abort, instanceId=${instanceId}")
    if (!connected) {
      logDebug(s"Abort called, but client has never been connected")
      return
    }
    pcb = pcb.copy(rowCount = rowCount)
    try {
      pcb = server.asInstanceOf[TaskCoordinator].handlerAsks(pcb, "abort")
    } catch {
      case ex: java.rmi.NoSuchObjectException => logDebug(s"${ex.getClass.getName} ${ex.getMessage}")
    }
    jobAbort.set(true)
    NetUtils().waitForCompletion(msMax) {
      sqlTransferComplete.get() || coordinatorSqlComplete.get()
    }
    rmiDataTargetGuard.synchronized {
      if ((rmiDataTarget != null) && (rmiDataTarget != rmiThis)) {
        rmiDataTarget.close(this.asInstanceOf[ITSIpcClient])
      } else {
        close(this.asInstanceOf[ITSIpcClient])
      }
      rmiDataTarget = null
    }
    rmiThis = null
    buffExchange.clear()
    buffExchange = null
  }

  def stop: Long = this.synchronized {
    try {
      if (connected && server != null) {
        progressTracker.trackProgress("rmiDisconnect") {
          server.disconnect(pcb)
        }
        connected = false
      }
    } catch {
      case e: java.rmi.NoSuchObjectException =>
        logInfo(s"server.disconnect(pcb) called with server that is already shut down: pcb=${pcb}")
      case e: Exception => logWarning(s"server.disconnect(pcb) failed: pcb=${pcb}, ${e}")
    }
    server = null
    rmiThis = null
    rmiDataTarget = null
    connected = false
    var rmiLoopMs: Long = 0
    progressTracker.trackProgress("webServerStop") {
      if (webServer != null) {
        rmiLoopMs = webServerMetrics.rmiLoopMs.get()
        //webServer.server.stop(1)
        webServer.stop
        webServer = null
        logDebug(s"Web server instance terminated (${gpfdistUrl})")
      }
    }
    progressTracker.trackProgress("rmiUnexport") {
      try {
        val success = UnicastRemoteObject.unexportObject(this, true)
        logInfo(s"unexportObject(pcb=${pcb})=${success}")
      } catch {
        case e: Exception => logWarning(s"unexportObject failed: pcb=${pcb}, ${e.getClass.getCanonicalName}")
      }
    }
    dataProviders.clear()
    logInfo(s"stop took: ${progressTracker.reportTimeTaken()}")
    rmiLoopMs
  }

  // @throws[java.rmi.RemoteException]
  override def name: String = instanceId

  // @throws[java.rmi.RemoteException]
  override def hostAddress: String = instanceHostAddress

  // @throws[java.rmi.RemoteException]
  override def segId: String = instanceSegmentId

  // @throws[java.rmi.RemoteException]
  override def segService: Boolean = isServiceProvider

  private var waitForPost: Long = 0L
  private val getSucceededOnce: AtomicBoolean = new AtomicBoolean(false)

  private def webTransferComplete: Boolean = {
    if (!readOrWrite || (webServer == null)) return sqlTransferComplete.get()
    if (readOrWrite && coordinatorSqlComplete.get() && !sqlTransferComplete.get()) {
      if (atLeastOnePostComplete.get()) {
        sqlTransferComplete.set(true)
      } else {
        if (waitForPost == 0) waitForPost = System.currentTimeMillis()
        if (System.currentTimeMillis() - waitForPost > 2000) sqlTransferComplete.set(true)
      }
    }
    val ret = ((webServer.nSegInProgress == 0)
      && sqlTransferComplete.get()
      && (buffExchange.totalDrain.get() >= buffExchange.totalFill.get())
      && (buffExchange.available == 0)
      )
    if (sqlTransferComplete.get() && (buffExchange.available != 0))
      logDebug(s"webTransfer(${ret}): waiting for byte queue drain completion, ${buffExchange.available} bytes left..")
    ret
  }

  private def tryGet(msMax: Long = 10): ByteBuffer = {
    var ret: ByteBuffer = buffExchange.get(msMax)
    if ((ret != null) && (ret.position() > 0) && !getSucceededOnce.get()) {
      getSucceededOnce.set(true)
      logDebug(s"getSucceededOnce=true pcb=${pcb}")
    }
    if (!readOrWrite || getSucceededOnce.get()) {
      if (nSourcesPlan.get() == 0) providersDisconnected.reset()
      transactionStarted.signalEvent()
      if (!readOrWrite) {
        if (nSourcesPlan.get() == 0) {
          nSourcesPlan.set(server.participantsList(instanceSegmentId).size)
        }
        if ((nSourcesPlan.get() > 0) && (nSourcesDone.get() >= nSourcesPlan.get())) {
          providersDisconnected.signalEvent()
        }
      }
    }
    this.synchronized {
      if ((ret == null) && !webTransferComplete) {
        if ((nSourcesPlan.get() > nSourcesDone.get())
          || (readOrWrite && connected && (!getSucceededOnce.get() || dataProviders.nonEmpty))
          || (buffExchange.totalDrain.get() < buffExchange.totalFill.get())
          || !writerReady.get())
          ret = ByteBuffer.allocate(0) // Will block
      }
    }
    ret
  }

  private val getWillBlock: AtomicBoolean = new AtomicBoolean(false)


  /**
   * Retrieve a portion of data from the segment data stream, or null
   * if the stream is terminated and there is no more data buffered.
   * Blocks until data available.
   * Throws exception if timeout elapsed.
   *
   * @param msMax Int maximum wait time in ms
   * @return ByteBuffer
   */
  def read(msMax: Long = 60000): ByteBuffer = {
    val rnd = new scala.util.Random
    val start = System.currentTimeMillis()
    logTrace(s"read enter: threadId=${Thread.currentThread().getId}, instanceId=${instanceId}")
    getWillBlock.set(false)
    var ret: ByteBuffer = tryGet(rnd.nextInt(10) + 1)
    while (!Thread.currentThread().isInterrupted
      && !jobAbort.get()
      && ((msMax < 0) || (System.currentTimeMillis() - start) < msMax)
      && (ret != null) && (ret.position() == 0)) {
      if (!getWillBlock.get()) {
        getWillBlock.set(true)
        logDebug(s"Get will block, threadId=${Thread.currentThread().getId}, instanceId=${instanceId}")
      }
      ret = tryGet(rnd.nextInt(10) + 1)
    }
    if (!jobAbort.get() && sqlTransferComplete.get() && (ret == null)) ret = tryGet(1)
    if (ret != null) {
      if (ret.position() == 0)
        throw new Exception(s"Time limit elapsed (${msMax}) " +
          s"waiting data from threadId=${Thread.currentThread().getId}, instanceId=${instanceId}")
      logTrace(s"read success: threadId=${Thread.currentThread().getId}, " +
        s"instanceId=${instanceId}, nBytes=${ret.position()}")
    } else {
      logDebug(s"End of stream: threadId=${Thread.currentThread().getId}, instanceId=${instanceId}")
    }
    ret
  }

  def recycleBuffer(buff: ByteBuffer): Unit = {
    buffExchange.addBuffer(buff)
  }

  // @throws[java.rmi.RemoteException]
  override def close(sender: ITSIpcClient): Boolean = this.synchronized {
    var ret: Boolean = false
    logTrace(s"close() enter: sources plan ${nSourcesPlan.get()}, done ${nSourcesDone.get()}, " +
      s"\nthis ${rmiThis}," +
      s"\narg provider=${sender}," +
      s"\nlist (size ${dataProviders.size})")
    if (dataProviders.contains(sender)) {
      dataProviders -= sender
      //      if (sender != rmiThis) {
      nSourcesDone.incrementAndGet()
      //      }
      ret = true
    }
    logTrace(s"close() exit: sources plan ${nSourcesPlan.get()}, done ${nSourcesDone.get()}, " +
      s"list (size ${dataProviders.size}):\n${dataProviders}")
    //if ((nSourcesPlan.get() > 0) && (nSourcesDone.get() >= nSourcesPlan.get() - 1)) {
    if ((nSourcesPlan.get() > 0) && (nSourcesDone.get() >= nSourcesPlan.get())) {
      // Ourself local provider should be the one that remains
      providersDisconnected.signalEvent()
      //if (ret) rmiDataTarget = null
    }
    ret
  }

  // @throws[java.rmi.RemoteException]
  override def put(sender: ITSIpcClient, data: Array[Byte]): Boolean = {
    putLocal(sender, data)
  }

  private def putLocal(sender: ITSIpcClient, data: Array[Byte]): Boolean = this.synchronized {
    if (!dataProviders.contains(sender)) {
      logTrace(s"putLocal adding provider ${sender}")
      dataProviders += sender
      if (rmiThis != sender) {
        providersDisconnected.reset()
      }
    }
    buffExchange.put(data)
    writerReady.set(true)
    true
  }

  // @throws[java.rmi.RemoteException]
  override def size: Int = this.synchronized {
    buffExchange.available.toInt
  }

  /**
   * Returns gpfdist instance holder serving this instance segment
   *
   * @return ITSIpcClient
   */
  def getSegServiceProvider: ITSIpcClient = this.synchronized {
    rmiDataTargetGuard.synchronized {
      if (!connected) {
        logDebug(s"getSegServiceProvider called, but client has never been connected")
        return null
      }
      if (rmiDataTarget == null) {
        if (!isServiceProvider) {
          rmiDataTarget = server.getSegServiceProvider(instanceSegmentId)
        } else {
          rmiDataTarget = rmiThis
        }
      }
      if (rmiDataTarget == null)
        throw new Exception(s"Unable to find GPFDIST service provider for GP segment ${instanceSegmentId}, pcb=${pcb}")
      rmiDataTargetInstId = rmiDataTarget.name
      rmiDataTarget
    }
  }

  override def unreferenced(): Unit = {
    stop
  }
}

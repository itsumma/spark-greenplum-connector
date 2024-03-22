package com.itsumma.gpconnector.rmi

import com.itsumma.gpconnector.rmi.GPConnectorModes.GPConnectorMode
import com.itsumma.gpconnector.rmi.RMIMaster.{clientSocketFactory, createRegistry, localHotName, localIpAddress, serverSocketFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.itsumma.gpconnector.SparkSchemaUtil.guessMaxParallelTasks

import java.net.{InetAddress, ServerSocket, Socket}
//import java.rmi.{Naming, RemoteException}
import java.rmi.registry.{LocateRegistry, Registry}
import java.rmi.server.{RMIClientSocketFactory, RMIServerSocketFactory, UnicastRemoteObject, Unreferenced}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable.{HashMap => MutableHashMap, Map => MutableMap}
import scala.collection.immutable.{ListMap, Set}
import scala.util.Random

//import com.typesafe.scalalogging.Logger
//import org.slf4j.LoggerFactory

/**
 * Central store for metadata forming GP-segment related interconnect area
 * allowing Spark executor instances communicate data to each other.
 */
@remote
trait ITSIpcServer extends java.rmi.Remote {
  //def connect(client: ITSIpcClient): String

  /**
   * Unregisters colling executor from the interconnect facility.
   *
   * @param pcb : client's Partition Control Block
   */
  // @throws[java.rmi.RemoteException]
  def disconnect(pcb: PartitionControlBlock): Unit

  /**
   * List all clients participating in the specified GP segment interconnect.
   *
   * @param segmentId (String)
   * @return the set of corresponding client instances
   */
  // @throws[java.rmi.RemoteException]
  def participantsList(segmentId: String): Set[ITSIpcClient]

  /**
   * Get ITSIpcClient instance where gpfdist service for the specified GP segment is running.
   *
   * @param segmentId (String)
   * @return corresponding client instance
   */
  // @throws[java.rmi.RemoteException]
  def getSegServiceProvider(segmentId: String): ITSIpcClient
}

/**
 * Provides RMI entry point for partition handlers
 */
@remote
trait TaskCoordinator extends java.rmi.Remote {
  // @throws(classOf[java.rmi.RemoteException])
  def handlerAsks(pcb: PartitionControlBlock, func: String, msMax: Long = 60000): PartitionControlBlock
}

case class ServerSocketFactory(address: InetAddress)
  extends RMIServerSocketFactory {
  override def createServerSocket(port: Int): ServerSocket = new ServerSocket(port, 0, address)
}

case class ClientSocketFactory(address: InetAddress) extends RMIClientSocketFactory with Logging {
  override def createSocket(host: String, port: Int): Socket = {
    logInfo(s"Passed in host is ${host}, actually using ${address.getHostName}/${address.getHostAddress}")
    new Socket(address, port)
  }
}

object GPConnectorModes extends Enumeration {
  type GPConnectorMode = Value
  val Batch, MicroBatch, Continuous = Value
}

object RMIMaster {
  val (localHotName: String, localIpAddress: String) = NetUtils().getLocalHostNameAndIp
  //InetAddress.getLocalHost.getHostAddress
  private val inetAddress: InetAddress = InetAddress.getByName(
    InetAddress.getByName(NetUtils().resolveHost2Ip(localIpAddress)).getHostName)
  val serverSocketFactory: ServerSocketFactory = ServerSocketFactory(inetAddress)
  val clientSocketFactory: ClientSocketFactory = ClientSocketFactory(inetAddress)

  def createRegistry: (Registry, Int) = {
    var registry: Registry = null
    var registryPort: Int = -1
    var att = 5
    do {
      att = att - 1
      try {
        val spareSocket = new ServerSocket(0)
        registryPort = spareSocket.getLocalPort
        spareSocket.close()
        registry = LocateRegistry.createRegistry(registryPort, clientSocketFactory, serverSocketFactory)
      } catch {
        case e: java.rmi.server.ExportException
          if e.getCause != null && e.getCause.isInstanceOf[java.net.BindException] && att > 0 =>
        /*if (att < 1) throw e*/
        /*case t: Exception => throw t*/
      }
    } while (att > 0 && registry == null)
    (registry, registryPort)
  }
}

/**
 * Implements {@link ITSIpcServer} and {@link TaskCoordinator} RMI interfaces.
 * <p>Creates RMI registry instance bound to the random anonymous TCP port.
 *
 * @param optionsFactory  GPOptionsFactory instance
 * @param queryId         String UUID
 * @param nGpSegments     number of segments in the GP database we expect to serve
 * @param jobDone         reference to atomic boolean where we return or get Done condition
 * @param jobAbort        reference to atomic boolean where we return Abort condition
 * @param address2PrefSeg preferred set of served GP segments for every executor IP
 */
class RMIMaster(optionsFactory: GPOptionsFactory,
                queryId: String,
                var nGpSegments: Int,
                jobDone: AtomicBoolean, jobAbort: AtomicBoolean,
                var address2PrefSeg: Map[String, Set[String]] = Map[String, Set[String]](),
                mode: GPConnectorMode = GPConnectorModes.Batch,
                isReadTransaction: Boolean = true
               )
  extends UnicastRemoteObject(0, clientSocketFactory, serverSocketFactory)
    with ITSIpcServer
    with TaskCoordinator
    with Unreferenced
    with Logging
{
  private val nParallelTasks = guessMaxParallelTasks()
  private val client2Segment: MutableMap[ITSIpcClient, String] = MutableMap[ITSIpcClient, String]()
  private val address2Seg = MutableMap[String, Set[String]]()
  private val seg2ServiceProvider = MutableMap[String, ITSIpcClient]()
  private val pcbByInstanceId = MutableMap[String, PartitionControlBlock]()
  val batchNo = new AtomicInteger(0)
  private val nBatchSize = new AtomicInteger(0)
  private val nFails = new AtomicInteger(0)
  private val nSuccess = new AtomicInteger(0)
  private val nSuccessBatch = new AtomicInteger(0)
  private val nSuccessTotal = new AtomicInteger(0)
  private var abortMsg: String = null
  private val lockPass = new AtomicLong(0)
  private var localBatchPrepared: Boolean = false
  private val lastEpoch = new AtomicLong(0)

  /** GPFDIST service URLs indexed by executor instance ID */
  private val partUrlByInstId: MutableHashMap[String, String] = MutableHashMap()

  logInfo(s"Starting driver instance ${queryId} on ${localHotName}/${localIpAddress}," +
    s"parallel tasks=$nParallelTasks")
  // private val registry: Registry = LocateRegistry.createRegistry(RMIRegPort)
  // Naming.rebind(s"//localhost:${RMIRegPort}/com/itsumma/gpconnector/rmi/RMIMaster", this)
  private var (registry: Registry, registryPort: Int) = createRegistry
  private val bindString = s"com/itsumma/gpconnector/${queryId}"
  registry.rebind(bindString, this)
  logInfo(s"RMI registry is listening on ${localIpAddress}:${registryPort}")

  def rmiRegistryAddress = s"${localIpAddress}:${registryPort}"

  def getCurrentEpoch: Long = lastEpoch.get()

  def partUrls: Set[String] = this.synchronized {
    if (isReadTransaction /*TODO: can use pcbByInstanceId too*/ ) {
      partUrlByInstId.values.toSet
    } else {
      pcbByInstanceId.
        map { case (_, pcb) => pcb.gpfdistUrl }.toSet
    }
  }

  def numActiveTasks: Int = this.synchronized {
    if (isReadTransaction) {
      return currentBatchSize
    }
    pcbByInstanceId.keySet.size
  }

  def currentBatchSize: Int = nBatchSize.get()

  def totalTasks: Int = {
    nSuccessTotal.get() + nFails.get()
  }

  def successTasks: Int = nSuccessTotal.get()

  def successTasksBatch: Int = nSuccessBatch.get()

  def failedTasks: Int = nFails.get()

  def setGpSegmentsNum(newVal: Int): Unit = this.synchronized {
    nGpSegments = newVal
  }

  private var transactionStartMs: Long = System.currentTimeMillis()
  private var partInitMs: Long = 0

  private def waitBatchTry(): Int = this.synchronized {
    if (jobDone.get() || jobAbort.get())
      return -1
    if (nFails.get() > 0)
      throw new Exception(s"Abort on ${abortMsg}")
    if (currentBatchSize == 0)
      return -2
    if (!isReadTransaction) {
      if ((currentBatchSize < nParallelTasks)
        && ((System.currentTimeMillis() - transactionStartMs) < partInitMs))
        return -2
    } else {
      val minWait = if (partInitMs > 0) partInitMs else 1000
      if ((currentBatchSize < nGpSegments)
        && (((System.currentTimeMillis() - transactionStartMs) < (minWait * 2)) ||
        (mode != GPConnectorModes.Batch) // wait for all scheduled partitions hardly while streaming, fail job if unmet
        )
      )
        return -2
    }
    if (isReadTransaction && (mode == GPConnectorModes.Batch))
      jobDone.set(true) // Prevent further partition handler registration
    logInfo(
      s"""New batch: ${batchNo.get()},\n""" +
        s"""seg2ServiceProvider.keySet=${seg2ServiceProvider.keySet.mkString("{", ",", "}")}\n""" +
        s"""address2Seg=${address2Seg.mkString("{", ",", "}")}\n""" +
        s"""pcbByInstanceId=\n${pcbByInstanceId.mkString("{", "\n", "}")}"""
    )
    localBatchPrepared = true
    batchNo.get()
  }

  def waitBatch(msMax: Long = 60000): Int = {
    transactionStartMs = System.currentTimeMillis()
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    logDebug(s"\nwaitBatch: allocating batch ${batchNo.get()}")
    var curBatchNo: Int = waitBatchTry()
    lockPass.set(0)
    while ((curBatchNo < -1) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      if (lockPass.incrementAndGet() == 1) {
        logDebug(s"\nLock in waitBatch, batch=${batchNo.get()}, nSuccess=${nSuccess.get()}, nFails=${nFails.get()}")
      }
      Thread.sleep(rnd.nextInt(100) + 1)
      curBatchNo = waitBatchTry()
    }
    if (curBatchNo == -2) {
      if (batchNo.get() == 0) {
        logDebug(s"\nTimeout elapsed while allocating batch ${batchNo.get()}")
      } else {
        // logError(s"\nTimeout elapsed while allocating batch ${batchNo.get()}")
        throw new Exception(s"Timeout elapsed while allocating batch ${batchNo.get()}\n" +
          s"""seg2ServiceProvider.keySet=${seg2ServiceProvider.keySet.mkString("{", ",", "}")}\n""" +
          s"""address2Seg=${address2Seg.mkString("{", ",", "}")}\n""" +
          s"""pcbByInstanceId=\n${pcbByInstanceId.mkString("{", "\n", "}")}"""
        )
      }
    } else {
      logDebug(s"\nwaitBatch success, localBatchNo=${curBatchNo}, batchSize=${currentBatchSize}")
    }
    curBatchNo
  }

  private def commitBatchTry(): Int = {
    if (jobDone.get() || jobAbort.get())
      return -1
    if (nFails.get() > 0)
      throw new Exception(s"Abort on ${abortMsg}")
    if ((currentBatchSize == 0) || (nSuccess.get() < currentBatchSize))
      return -2
    this.synchronized {
      if (isReadTransaction && (mode == GPConnectorModes.Batch))
        jobDone.set(true)
      logInfo(
        s"""commitBatch success, batchNo=${batchNo.get()}, batchSize=${currentBatchSize}, nSuccess=${nSuccess.get()}\n""" +
          s"""seg2ServiceProvider.keySet=${seg2ServiceProvider.keySet.mkString("{", "\n", "}")}\n""" +
          s"""address2Seg=${address2Seg.mkString("{", "\n", "}")}\n""" +
          s"""pcbByInstanceId=${pcbByInstanceId.mkString("{", "\n", "}")}"""
      )
      pcbByInstanceId.clear()
      partUrlByInstId.clear()
      address2Seg.clear()
      seg2ServiceProvider.clear()
      client2Segment.clear()
      nSuccessBatch.set(nSuccess.get())
      nSuccess.set(0)
      localBatchPrepared = false
      //nFails.set(0)
      nBatchSize.set(0)
      if (batchNo.incrementAndGet() < 0)
        batchNo.set(0)
    }
    batchNo.get()
  }

  def commitBatch(msMax: Long = 60000): Int = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    this.synchronized {
//      if (!isReadTransaction || jobAbort.get()) {
//      }
      var cmd = "sqlTransferComplete"
      if (jobAbort.get()) {
        cmd = "sqlTransferAbort"
      }
      pcbByInstanceId.foreach { instance => {
          logDebug(s"Sending ${cmd} to the slave ${instance._1}")
          val pcb:  PartitionControlBlock = instance._2.handler.coordinatorAsks(instance._2, cmd)
          if (pcb != null) {
            logTrace(s"commitBatch: sent ${cmd} to partition reader instances ${instance._1}")
          } else {
            logTrace(s"commitBatch: can't send ${cmd} to partition reader instances ${instance._1}")
          }
        }
      }
    }
    var newBatchNo: Int = commitBatchTry()
    lockPass.set(0)
    while ((newBatchNo < -1) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      if (lockPass.incrementAndGet() == 1)
        logDebug(s"Lock in commitBatch, nSuccess=${nSuccess.get()}, nFails=${nFails.get()}")
      Thread.sleep(rnd.nextInt(10) + 1)
      newBatchNo = commitBatchTry()
    }
    if (newBatchNo == -2)
      throw new Exception(s"Unable to commit batch ${batchNo.get()}")
    //logDebug(s"commitBatch success, newBatchNo=${newBatchNo}")
    newBatchNo
  }

  def stop(): Unit = this.synchronized {
    doStop()
  }

  private def doStop(): Unit = {
    if (registry == null)
      return
    try {
      registry.unbind(bindString)
    } catch {
      case e: Exception => logWarning(s"${e.getClass.getCanonicalName}")
    }
    val registryUnexport: Boolean =
      try {
        UnicastRemoteObject.unexportObject(registry, true)
      } catch {
        case e: Exception => logWarning(s"${e.getClass.getCanonicalName}")
          false
      }
    val selfUnexport: Boolean =
      try {
        UnicastRemoteObject.unexportObject(this, true)
      } catch {
        case e: Exception => logWarning(s"${e.getClass.getCanonicalName}")
          false
      }
    logInfo(s"${queryId}: unexportObject(self)=${selfUnexport}, unexportObject(registry)=${registryUnexport}, " +
      s"batchNo=${batchNo.get()}, nGpSegments=${nGpSegments}, " +
      s"successTasksBatch=${nSuccess.get()}, totalSuccessTasks=${nSuccessTotal.get()}, nFails=${nFails}, " +
      s"nPCBs=${pcbByInstanceId.size}")
    seg2ServiceProvider.clear()
    client2Segment.clear()
    pcbByInstanceId.clear()
    nBatchSize.set(0)
    registry = null
  }

  class GpSegmentAdhereException(s: String) extends Exception(s) {}

  override def handlerAsks(pcb: PartitionControlBlock, func: String, msMax: Long = 60000): PartitionControlBlock = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var retPcb: PartitionControlBlock = null //handlerAsksTry(pcb, func)
    while ((retPcb == null) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      retPcb = handlerAsksTry(pcb, func)
      if (retPcb == null)
        Thread.sleep(rnd.nextInt(100) + 1)
    }
    if (retPcb == null) {
      throw new Exception(s"Timeout ${msMax} elapsed for func=$func, $pcb")
    }
    retPcb
  }

  private def handlerAsksTry(pcb: PartitionControlBlock, func: String): PartitionControlBlock = this.synchronized {
    var newPcb: PartitionControlBlock = null //pcbByInstanceId.getOrElse(pcb.instanceId, null)
    var msg: String = ""
    if ((nFails.get() > 0) && (func != "abort"))
      throw new Exception(s"Abort on ${func}")
    var isServiceProvider: Boolean = true
    if (!isReadTransaction) {
      isServiceProvider = !address2Seg.contains(pcb.nodeIp) ||
        (seg2ServiceProvider.getOrElse(pcb.gpSegmentId, null) == pcb.handler.asInstanceOf[ITSIpcClient])
      if (isServiceProvider && (pcb.gpSegmentId == null) && (func == "checkIn")) {
        if (seg2ServiceProvider.keySet.size >= nGpSegments) {
          // We have more worker hosts than GP segments number
          isServiceProvider = false
        }
      }
    }
    func match {
      case "checkIn" =>
        if (jobDone.get() || jobAbort.get()) {
          return pcb.copy()
        }
        try {
          if (localBatchPrepared) //(nSuccess.get() > 0)
            return newPcb // Locks until current batch commit
          lastEpoch.set(pcb.instanceId.split(":").last.toInt)
          newPcb = pcb.copy(batchNo = batchNo.get())
          if (isServiceProvider) {
            newPcb = newPcb.handler.coordinatorAsks(newPcb, "startService", optionsFactory.networkTimeout)
            partUrlByInstId.put(newPcb.instanceId, newPcb.gpfdistUrl)
          } else {
            newPcb = newPcb.copy(dir = "w")
          }
          val instanceSegmentId = connect(newPcb)
          newPcb = newPcb.copy(gpSegmentId = instanceSegmentId, status = "i")
          if (isServiceProvider) {
            msg = s"New gpfdist instance started on ${newPcb}"
          } else {
            val spMap = pcbByInstanceId.filter(entry => entry._2.gpSegmentId == instanceSegmentId && entry._2.dir == "W")
            if (spMap.isEmpty
              || !seg2ServiceProvider.contains(instanceSegmentId)
              || (seg2ServiceProvider.getOrElse(instanceSegmentId, null).asInstanceOf[TaskHandler] != spMap.head._2.handler)
            ) {
              val msgErr = s"Unable to adhere to the GP segment " +
                s"${instanceSegmentId} as it has no gpfdist host assigned. pcb=${newPcb}"
              logError(
                s"""${msgErr},\n""" +
                  s"""seg2ServiceProvider.keySet=${seg2ServiceProvider.keySet.mkString("{", "\n", "}")}\n""" +
                  s"""address2Seg=${address2Seg.mkString("{", "\n", "}")}\n""" +
                  s"""pcbByInstanceId=${pcbByInstanceId.mkString("{", "\n", "}")}"""
              )
              throw new GpSegmentAdhereException(msgErr)
            }
            newPcb = newPcb.copy(gpfdistUrl = spMap.head._2.gpfdistUrl)
            msg = s"Adhere to available gpfdist instance from ${newPcb}"
          }
          pcbByInstanceId.put(newPcb.instanceId, newPcb) match {
            case Some(_) =>
            case None => nBatchSize.incrementAndGet()
          }
          if (partInitMs < 200) {
            partInitMs = System.currentTimeMillis() - transactionStartMs
            if (partInitMs < 200)
              partInitMs = 200
          }
          transactionStartMs = System.currentTimeMillis()
        } catch {
          //case ex: GpSegmentAdhereException => throw ex
          case e: Exception =>
            jobAbort.set(true)
            throw e
        }
      case "commit" => {
        if (pcbByInstanceId.contains(pcb.instanceId)) {
          newPcb = pcb.copy(status = "c")
          pcbByInstanceId.update(pcb.instanceId, newPcb)
          nSuccess.incrementAndGet()
          nSuccessTotal.incrementAndGet()
        } else {
          if (isReadTransaction)
            logInfo(s"Commit from extra or outdated slave, pcb=${pcb}")
          else
            logError(s"Commit from extra or outdated slave, pcb=${pcb}")
          newPcb = pcb.copy()
        }
        msg = s"Commit from ${newPcb}"
      }
      case "abort" => {
        newPcb = pcb.copy(status = "a")
        msg = s"Abort from ${newPcb}"
        abortMsg = msg
        nFails.incrementAndGet()
        jobAbort.set(true)
      }
      case unknownFunc => throw new Exception(s"Unknown call: ${unknownFunc}")
    }
    logDebug(s"\n${(System.currentTimeMillis() % 1000).toString} handlerAsks: ${func}, ${msg}")
    if ((nFails.get() > 0) && (func != "abort"))
      throw new Exception(s"Abort on ${func}: ${msg}")
    newPcb
  }

  // @throws[java.rmi.RemoteException]
  override def participantsList(segmentId: String): Set[ITSIpcClient] = this.synchronized {
    client2Segment.filter { t => t._2 == segmentId }.keySet.toSet
  }

  /**
   * Called as part of ITSIpcClient instance checkIn procedure.
   * Assigns corresponding executor instance to serve particular GP segment
   * and registers it for the interconnect facility of its segment.
   * Clients running on the application driver (not serving any GP segment itself)
   * should not call this method.
   *
   * @param pcb : structure containing a client handle and other parameters
   * @return GP segment ID as String
   */
  private def connect(pcb: PartitionControlBlock): String = {
    val client: ITSIpcClient = pcb.handler.asInstanceOf[ITSIpcClient]
    var segId = pcb.gpSegmentId // null //client.segId
    val isServiceHolder = (pcb.dir == "W") || (pcb.dir == "R")
    val hostAddress = pcb.nodeIp //client.hostAddress
    if ((segId != null) || client2Segment.contains(client))
      throw new Exception(s"Second attempt to connect ${pcb}")
    val segmentServiceCounts = client2Segment groupBy (_._2) mapValues (_.size)
    var candidateSegments: Set[String] = Set[String]()
    if (isServiceHolder) {
      if ((address2PrefSeg != null) && address2PrefSeg.contains(hostAddress)) {
        candidateSegments = address2PrefSeg(hostAddress)
        candidateSegments --= seg2ServiceProvider.keySet
      } else {
        candidateSegments = Set[String]()
      }
      if (candidateSegments.isEmpty) {
        //val nSeg = math.max(if (!isReadTransaction) nParallelTasks else nGpSegments, nGpSegments)
        candidateSegments = (for (id <- 0 until nGpSegments) yield id.toString).toSet
          .filter(id => !segmentServiceCounts.contains(id))
      }
      candidateSegments --= seg2ServiceProvider.keySet
    } else {
      candidateSegments = address2Seg.getOrElse(hostAddress, Set[String]())
      if (candidateSegments.isEmpty) {
        // Will make a satellite writer situated on the separate worker host (workers number > segments number)
        candidateSegments ++= seg2ServiceProvider.keySet // adhere to any master available
        logDebug(s"Making satellite writer on the separate worker: pcb=${pcb} candidateSegments=${candidateSegments}")
      }
    }
    if (candidateSegments.isEmpty)
      throw new Exception(s"Unable to assign GP segment to the ITSIpcClient instance ${pcb}")
    val candidateSegmentsShuffle = Random.shuffle(candidateSegments.toSeq) // For the case of all the counts are equal
    val candidateUseCounts = (for (id <- candidateSegmentsShuffle)
      yield (id, segmentServiceCounts.getOrElse(id, 0))).toMap
    val countsAsc = ListMap(candidateUseCounts.toSeq.sortBy(_._2): _*)
    segId = countsAsc.head._1

    if ((segId == null) || segId.isEmpty)
      throw new Exception(s"Unable to assign GP segment to the ITSIpcClient instance ${client}")
    if (isServiceHolder) {
      if (seg2ServiceProvider.contains(segId))
        throw new Exception(s"Segment ${segId} already has a gpfdist instance holder")
      seg2ServiceProvider.put(segId, client)
      var segSet = candidateSegments
      if (!segSet.contains(segId)) segSet = segSet + segId
      address2Seg.put(hostAddress, segSet)
    }
    client2Segment.put(client, segId)
    segId
  }

  override def disconnect(pcb: PartitionControlBlock): Unit = this.synchronized {
    val client: ITSIpcClient = pcb.handler.asInstanceOf[ITSIpcClient]
    val segId = pcb.gpSegmentId
    val isServer = (pcb.dir == "W") || (pcb.dir == "R")
    if (isServer) {
      if (seg2ServiceProvider.getOrElse(segId, null) == client) {
        //seg2ServiceProvider.retain((k, v) => k != segId || v != client)
        seg2ServiceProvider.remove(segId)
        address2Seg -= pcb.nodeIp
      }
    }
    //client2Segment -= client // make it remain here until the batch end
    pcbByInstanceId -= pcb.instanceId
    //participantsListUpdate()
    logInfo(s"Batch ${batchNo.get()}: disconnected ${pcb}")
  }

  // @throws[java.rmi.RemoteException]
  override def getSegServiceProvider(segmentId: String): ITSIpcClient = this.synchronized {
    val ret: ITSIpcClient = seg2ServiceProvider.getOrElse(segmentId, null)
    if (ret == null)
      logError(s"Unable to find GPFDIST service provider for GP segment ${segmentId},\n" +
        s"""seg2ServiceProvider.keySet=${seg2ServiceProvider.keySet.mkString("{", "\n", "}")}\n""" +
        s"""address2Seg=${address2Seg.mkString("{", "\n", "}")}\n""" +
        s"""pcbByInstanceId=${pcbByInstanceId.mkString("{", "\n", "}")}"""
      )
    ret
  }

  override def unreferenced(): Unit = {
    logInfo(s"unreferenced called")
  }
}

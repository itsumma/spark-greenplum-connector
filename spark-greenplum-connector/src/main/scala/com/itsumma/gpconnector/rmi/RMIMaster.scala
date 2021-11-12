package com.itsumma.gpconnector.rmi

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.rmi.{Naming, RemoteException}
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable.{HashMap => MutableHashMap, Map => MutableMap}
import scala.collection.immutable.{ListMap, Set}

/**
 * Central store for metadata forming GP-segment related interconnect area
 * allowing Spark executor instances communicate data to each other.
 */
@remote trait ITSIpcServer {
  //def connect(client: ITSIpcClient): String

  /**
   * Unregisters colling executor from the interconnect facility.
   * @param client instance handle
   */
  def disconnect(pcb: PartitionControlBlock): Unit

  /**
   * List all clients participating in the specified GP segment interconnect.
   * @param segmentId (String)
   * @return the set of corresponding client instances
   */
  def participantsList(segmentId: String): Set[ITSIpcClient]

  /**
   * Get ITSIpcClient instance where gpfdist service for the specified GP segment is running.
   * @param segmentId (String)
   * @return corresponding client instance
   */
  def getSegServiceProvider(segmentId: String): ITSIpcClient
}

/**
 * Provides RMI entry point for partition handlers
 */
@remote trait TaskCoordinator {
  def handlerAsks(pcb: PartitionControlBlock, func: String, msMax: Long = 60000): PartitionControlBlock
}

/**
 * Implements {@link ITSIpcServer} and {@link TaskCoordinator} RMI interfaces.
 * <p>Creates RMI registry instance bound to the specified TCP port.
 * In current implementation the caller must specify non-zero registry port.
 * @param RMIRegPort Int
 * @param nGpSegments number of segments in the GP database we expect to serve
 */
class RMIMaster(RMIRegPort: Int, nGpSegments: Int, jobDone: AtomicBoolean, jobAbort: AtomicBoolean) extends UnicastRemoteObject with ITSIpcServer with TaskCoordinator {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  //val serverAddress: String = InetAddress.getLocalHost.getHostAddress
  private val client2Segment: MutableMap[ITSIpcClient, String] = MutableMap[ITSIpcClient, String]()
  private val address2Seg = MutableMap[String, Set[String]]()
  private val rnd = new scala.util.Random
  private val seg2ServiceProvider = MutableMap[String, ITSIpcClient]()
  private val pcbByInstanceId = MutableMap[String, PartitionControlBlock]()
  private val batchNo = new AtomicInteger(0)
  private val nFails = new AtomicInteger(0)
  private val nSuccess = new AtomicInteger(0)
  private var abortMsg: String = null
  private val lockPass = new AtomicLong(0)
  private var isReadTransaction: Boolean = true

  /** GPFDIST service URLs indexed by executor instance ID */
  private val partUrlByInstId: MutableHashMap[String, String] = MutableHashMap()

  def partUrls: Set[String] = this.synchronized {
    partUrlByInstId.values.toSet
  }

  def numActiveTasks: Int = this.synchronized {
    if (isReadTransaction) {
      return pcbByInstanceId.size
    }
    nSuccess.get()
  }

  private val registry = LocateRegistry.createRegistry(RMIRegPort)
  Naming.rebind(s"//localhost:${RMIRegPort}/com/itsumma/gpconnector/rmi/RMIMaster", this)

  private val transactionStartMs: Long = System.currentTimeMillis()
  private var partInitMs: Long = 0

  private def waitBatchTry(): Int = this.synchronized {
    if (jobDone.get() || jobAbort.get())
      return -1
    if (nFails.get() > 0)
      throw new Exception(s"Abort on ${abortMsg}")
    if (pcbByInstanceId.isEmpty)
      return -2
    if (!isReadTransaction) {
      if (nSuccess.get() < pcbByInstanceId.size)
        return -2
    } else {
      if ((pcbByInstanceId.size == 1) && (partInitMs == 0)) {
        partInitMs = System.currentTimeMillis() - transactionStartMs
        return -2
      } else {
        if ((pcbByInstanceId.size < nGpSegments)
          && (System.currentTimeMillis() - transactionStartMs < (partInitMs * (pcbByInstanceId.size + 1))))
          return -2
      }
    }
    batchNo.get()
  }

  def waitBatch(msMax: Long = 60000): Int = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var curBatchNo: Int = waitBatchTry()
    lockPass.set(0)
    while ((curBatchNo < -1) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      if (lockPass.incrementAndGet() == 1) {
        logger.info(s"\nLock in waitBatch, nSuccess=${nSuccess.get()}, nFails=${nFails.get()}")
      }
      Thread.sleep(rnd.nextInt(100) + 1)
      curBatchNo = waitBatchTry()
    }
    if (curBatchNo == -2) {
      logger.error(s"\nTimeout elapsed while allocating batch")
      throw new Exception(s"Timeout elapsed while allocating batch ${batchNo.get()}")
    }
    logger.info(s"\nwaitBatch success, waitBatch=${curBatchNo}")
    curBatchNo
  }

  private def commitBatchTry(): Int = this.synchronized {
    if (jobDone.get() || jobAbort.get())
      return -1
    if (nFails.get() > 0)
      throw new Exception(s"Abort on ${abortMsg}")
    if (pcbByInstanceId.isEmpty || (nSuccess.get() < pcbByInstanceId.size))
      return -2
    if (isReadTransaction)
      jobDone.set(true)
    pcbByInstanceId.clear()
    partUrlByInstId.clear()
    address2Seg.clear()
    seg2ServiceProvider.clear()
    client2Segment.clear()
    nSuccess.set(0)
    //nFails.set(0)
    batchNo.incrementAndGet()
  }

  def commitBatch(msMax: Long = 60000): Int = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    this.synchronized {
      if (!isReadTransaction || !jobAbort.get())
      pcbByInstanceId.foreach { instance => {
        instance._2.handler.coordinatorAsks(instance._2, "sqlTransferComplete")
      }}
    }
    var newBatchNo: Int = commitBatchTry()
    lockPass.set(0)
    while ((newBatchNo < -1) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      if (lockPass.incrementAndGet() == 1)
        logger.info(s"Lock in commitBatch, nSuccess=${nSuccess.get()}, nFails=${nFails.get()}")
      Thread.sleep(rnd.nextInt(100) + 1)
      newBatchNo = commitBatchTry()
    }
    if (newBatchNo == -2)
      throw new Exception(s"Unable to commit batch ${batchNo.get()}")
    logger.info(s"commitBatch success, newBatchNo=${newBatchNo}")
    newBatchNo
  }

  def stop: Unit = this.synchronized {
    val success = UnicastRemoteObject.unexportObject(this, true)
    logger.info(s"unexportObject called, success=${success}")
  }

  override def handlerAsks(pcb: PartitionControlBlock, func: String, msMax: Long = 60000): PartitionControlBlock = {
    val start = System.currentTimeMillis()
    val rnd = new scala.util.Random
    var retPcb: PartitionControlBlock = handlerAsksTry(pcb, func)
    while ((retPcb == null) && !Thread.currentThread().isInterrupted && ((System.currentTimeMillis() - start) < msMax)) {
      Thread.sleep(rnd.nextInt(100) + 1)
      retPcb = handlerAsksTry(pcb, func)
    }
    if (retPcb == null)
      throw new Exception(s"Timeout ${msMax} elapsed for func=$func, $pcb")
    retPcb
  }

  def handlerAsksTry(pcb: PartitionControlBlock, func: String): PartitionControlBlock = this.synchronized {
    var newPcb: PartitionControlBlock = null //pcbByInstanceId.getOrElse(pcb.instanceId, null)
    var msg: String = ""
    if ((nFails.get() > 0) && (func != "abort"))
      throw new Exception(s"Abort on ${func}")
    func match {
      case "checkIn" => {
        if (jobDone.get() || jobAbort.get())
          return pcb.copy()
        try {
          if (nSuccess.get() > 0)
            return newPcb // Locks until current batch commit
          var isServiceProvider: Boolean = true
          if (pcb.dir != "R") {
            isReadTransaction = false
            isServiceProvider = !address2Seg.contains(pcb.nodeIp)
          }
          if (isServiceProvider) {
            newPcb = pcb.copy(batchNo = batchNo.get())
            newPcb = pcb.handler.coordinatorAsks(newPcb, "startService")
            partUrlByInstId.put(newPcb.instanceId, newPcb.gpfdistUrl)
          } else {
            newPcb = pcb.copy(dir = "w", batchNo = batchNo.get())
          }
          val instanceSegmentId = connect(newPcb)
          newPcb = newPcb.copy(gpSegmentId = instanceSegmentId, status = "i")
          if (isServiceProvider) {
            msg = s"New gpfdist instance started on ${newPcb}"
          } else {
            val spMap = pcbByInstanceId.filter(entry => (entry._2.gpSegmentId == instanceSegmentId && entry._2.dir == "W"))
            if (spMap.isEmpty
              || !seg2ServiceProvider.contains(instanceSegmentId)
              || (seg2ServiceProvider.getOrElse(instanceSegmentId, null).asInstanceOf[TaskHandler] != spMap.head._2.handler)
            )
              throw new Exception(s"Unable to adhere to the GP segment ${instanceSegmentId} as it has no gpfdist host assigned")
            newPcb = newPcb.copy(gpfdistUrl = spMap.head._2.gpfdistUrl)
            msg = s"Adhere to available gpfdist instance from ${newPcb}"
          }
          pcbByInstanceId.put(newPcb.instanceId, newPcb)
        } catch {
          case e: Exception => {
            jobAbort.set(true)
            throw e
          }
        }
      }
      case "commit" => {
        newPcb = pcb.copy(status = "c")
        msg = s"Commit from ${newPcb}"
        nSuccess.incrementAndGet()
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
    logger.info(s"\n${(System.currentTimeMillis() % 1000).toString} handlerAsks: ${func}, ${msg}")
    if ((nFails.get() > 0) && (func != "abort"))
      throw new Exception(s"Abort on ${func}: ${msg}")
    newPcb
  }

  override def participantsList(segmentId: String): Set[ITSIpcClient] = this.synchronized {
    client2Segment.filter{t => t._2 == segmentId}.keySet.toSet
  }

  private def participantsListUpdate(): Unit = {
    val deadClients = client2Segment.filter{ client =>
      try {
        val segId = client._2
        val segmentParticipants = client2Segment.filter{ c => c._2 == segId }
        if (segmentParticipants.isEmpty)
          seg2ServiceProvider.remove(segId)
        // client._1.participantsListUpdate(segmentParticipants.keySet.toSet)
        false
      } catch {
        case ex: RemoteException => true
      }}
    if (deadClients.nonEmpty)
      client2Segment --= deadClients.keySet
  }

  /**
   * Called by a regular ITSIpcClient instance during creation.
   * Assigns corresponding executor instance to serve particular GP segment
   * if not assigned explicitly, and registers it for the interconnect facility of its segment.
   * Clients running on the application driver (not serving any GP segment itself)
   * should be assigned to the special segment ID 'master', so they doesn't call this method.
   * @param client: executors client handle
   * @return executors serving GP segment ID as String
   */
  private def connect(pcb: PartitionControlBlock): String = {
    val client: ITSIpcClient = pcb.handler.asInstanceOf[ITSIpcClient]
    var segId = pcb.gpSegmentId // null //client.segId
    val isServiceHolder = (pcb.dir == "W") || (pcb.dir == "R")
    val hostAddress = pcb.nodeIp //client.hostAddress
    if (!client2Segment.contains(client)) {
      var segSet = address2Seg.getOrElse(hostAddress, Set())
      if (segId == null) {
        if (segSet.nonEmpty) {
          // If clients GPF host already has any GP segments to serve then choose one of them randomly
          val arr = segSet.toArray
          val ix = rnd.nextInt(arr.length)
          segId = arr(ix)
        } else {
          // segId = rnd.nextInt(nGpSegments).toString
          // If clients GPF host doesn't yet serve any GP segment then assign him the least served one
          val zeroCounts = (for (id <- 0 until nGpSegments) yield (id.toString, 0)).toMap
          val counts = client2Segment groupBy(_._2) mapValues(_.size)
          val toAppend = zeroCounts.filter(entry => !counts.contains(entry._1))
          val allCounts = counts.++(toAppend)
          val countsAsc = ListMap(allCounts.toSeq.sortBy(_._2):_*)
          segId = countsAsc.head._1
        }
      } else {
        throw new Exception(s"Second attempt to connect ${pcb}")
      }
      if ((segId == null) || segId.isEmpty)
        throw new Exception(s"Unable to assign GP segment to the ITSIpcClient instance ${client}")
      if (isServiceHolder) {
        if (seg2ServiceProvider.contains(segId))
          throw new Exception(s"Segment ${segId} already has a gpfdist instance holder")
        seg2ServiceProvider.put(segId, client)
      }
      client2Segment.put(client, segId)
      if (!segSet.contains(segId))
        segSet = segSet + segId
      address2Seg.put(hostAddress, segSet)
      participantsListUpdate()
    } else {
      throw new Exception(s"Second attempt to connect ${pcb}")
    }
    segId
  }

  override def disconnect(pcb: PartitionControlBlock): Unit = this.synchronized {
    if (pcb.batchNo == batchNo.get()) {
      val client: ITSIpcClient = pcb.handler.asInstanceOf[ITSIpcClient]
      val segId = pcb.gpSegmentId
      val isServer = (pcb.dir == "W") || (pcb.dir == "R")
      if (isServer)
        seg2ServiceProvider.remove(segId)
      client2Segment -= client
      participantsListUpdate()
    }
  }

  override def getSegServiceProvider(segmentId: String): ITSIpcClient = this.synchronized {
    seg2ServiceProvider.getOrElse(segmentId, null)
  }

}

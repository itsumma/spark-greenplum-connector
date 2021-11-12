package com.itsumma.gpconnector.gpfdist

import com.itsumma.gpconnector.rmi.{ITSIpcClient, RMISlave}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable.{HashMap => MutableHashMap, Queue => MutableQueue, Set => MutableSet}
import scala.collection.JavaConversions._

/**
 * Implements GPFDIST protocol natively in scala.
 *
 * @param port     TCP port number to bind the service
 * @param rmiSlave to communicate data with Spark
 */
class WebServer(port: Int, rmiSlave: RMISlave, jobAbort: AtomicBoolean, transferComplete: AtomicBoolean) {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val executor = Executors.newFixedThreadPool(10)
  val server: HttpServer = HttpServer.create(new InetSocketAddress(port), 0)

  def httpPort: Int = server.getAddress.getPort

  var rmiLoopMs = new AtomicLong(0)

  private val activeWriters = MutableHashMap[String, Int]()
  private val segmentQueues = MutableHashMap[String, Array[Byte]]()
  private val segmentCount: AtomicInteger = new AtomicInteger(0)
  private val allSegmentsDone: AtomicBoolean = new AtomicBoolean(false)
  private val segPass = MutableHashMap[String, Int]()

  private val me = this

  if (!rmiSlave.segService)
    throw new Exception(s"WebServer instance holder should be marked as service provider in its segment")

  def stop: Unit = me.synchronized {
    server.stop(1)
    segmentQueues.clear()
    activeWriters.clear()
    allSegmentsDone.set(true)
  }

  server.createContext("/", new HttpHandler() {

    def convertBytesToHex(bytes: Seq[Byte]): String = {
      val sb = new StringBuilder
      for (b <- bytes) {
        sb.append(String.format("%02x ", Byte.box(b)))
      }
      sb.toString
    }

    override def handle(exchange: HttpExchange): Unit = {
      val uri = exchange.getRequestURI
      val scheme = uri.getScheme
      val headers = exchange.getRequestHeaders
      val targetName = uri.getPath
      val method = exchange.getRequestMethod.toLowerCase
      val segmentId = headers.get("X-gp-segment-id").toList match {
        case values: List[String] => values.head
        case _ => throw new Exception(s"No X-GP-SEGMENT-ID")
      }
      var contentLength: Int = 0
      if (headers.containsKey("Content-length"))
        contentLength = headers.get("Content-length").toList match {
          case values: List[String] => values.head.toInt
          case _ => 100000
        }
      logger.info(s"Uri: ${uri.toString}, scheme: ${scheme}, method: ${method}, " +
        s"caller: ${exchange.getRemoteAddress}, segmentId: ${segmentId}, contentLength=${contentLength}")
      if (method == "get") {
        val responseHeaders = exchange.getResponseHeaders
        responseHeaders.remove("Transfer-encoding")
        responseHeaders.put("Content-type", List("text/plain"))
        responseHeaders.put("Expires", List("0"))
        responseHeaders.put("X-GPFDIST-VERSION", List("6.8.1 build commit:7118e8aca825b743dd9477d19406fcc06fa53852"))
        responseHeaders.put("X-GP-PROTO", List("1"))
        responseHeaders.put("Cache-Control", List("no-cache"))
        responseHeaders.put("Connection", List("close"))
        exchange.sendResponseHeaders(200, 0)
        val os = exchange.getResponseBody
        //val nBytes = rmiSlave.size
        //logger.info(s"segmentId=${segmentId} writing ${nBytes}")
        val start = System.currentTimeMillis() // System.nanoTime() //
        var data = rmiSlave.get(10000)
        var nBytes: Long = 0
        while (data != null) {
          //os.write(data.getBytes(StandardCharsets.UTF_8))
          nBytes += data.length
          os.write(data)
          data = rmiSlave.get(10000)
        }
        val ms = System.currentTimeMillis() - start
        rmiLoopMs.addAndGet(ms)
        os.flush()
        os.close()
        logger.info(s"GET for GP segment ${segmentId} complete in ${ms}/${System.currentTimeMillis() - start}ms, nBytes=${nBytes}")
      } else if (method == "post") me.synchronized {
        val passStart = System.currentTimeMillis()
        var rmiMsTotal: Long = 0
        var bytesReadTotal: Int = 0
        if (activeWriters.isEmpty) {
          segmentCount.set(headers.get("X-gp-segment-count").toList match {
            case values: List[String] => values.head.toInt
            case _ => throw new Exception(s"No X-GP-SEGMENT-COUNT")
          })
          logger.info(s"Starting download for ${targetName} with segmentId=${segmentId}")
        }
        var gpSeq: Int = 0
        if (headers.containsKey("X-gp-seq"))
          gpSeq = headers.get("X-gp-seq").toList match {
            case values: List[String] => values.head.toInt
            case _ => 0
          }
        var segmentDone: Int = 0
        if (headers.containsKey("X-gp-done"))
          segmentDone = headers.get("X-gp-done").toList match {
            case values: List[String] => values.head.toInt
            case _ => 0
          }
        if (!activeWriters.containsKey(segmentId)) {
          activeWriters.put(segmentId, segmentDone)
          segmentQueues.put(segmentId, Array.empty[Byte])
        }
        segPass.put(segmentId, segPass.getOrElse(segmentId, 0) + 1)
        var nSlices: Int = 0
        val is = exchange.getRequestBody
        if (!jobAbort.get()) {
          val prevLen = segmentQueues(segmentId).length
          val buffLen: Int = prevLen + contentLength
          val buff = new Array[Byte](buffLen)
          if (prevLen > 0)
            Array.copy(segmentQueues(segmentId), 0, buff, 0, prevLen)
          /*
                    var avBreak: Boolean = false
                    while (is.available() < contentLength && !avBreak) {
                      if (System.currentTimeMillis() - passStart > 1000) {
                        logger.warn(s"caller ${exchange.getRemoteAddress}: body stream hanged, available so far ${is.available()}")
                        avBreak = true
                      }
                      Thread.sleep(10)
                    }
          */
          var nRead = is.read(buff, prevLen, contentLength)
          while ((nRead > 0) && !jobAbort.get()) {
            nSlices += 1
/*
            logger.info(s"read: segment ${segmentId} gpSeq=${gpSeq} pass=${segPass.getOrElse(segmentId, 0)}" +
              s" slice=${nSlices} nRead=${nRead} " +
              //s"${convertBytesToHex(buff.slice(0, Math.min(nRead, 10)))} " +
              s".. ")
*/
            bytesReadTotal += nRead
            if ((bytesReadTotal < contentLength) && !jobAbort.get()) {
              //logger.info(s"caller: ${exchange.getRemoteAddress} will block, bytesReadTotal=${bytesReadTotal}")
              nRead = is.read(buff, prevLen + bytesReadTotal, contentLength - bytesReadTotal)
            } else {
              nRead = -1
            }
          }

          if (!jobAbort.get()) {
            val eolIx = buff.lastIndexOf(0x0a)
            var tail: Array[Byte] = Array.empty[Byte]
            if (eolIx >= 0) {
              if (eolIx < buffLen - 1) {
                val len = buffLen - eolIx - 1
                tail = Array.ofDim[Byte](len)
                Array.copy(buff, eolIx + 1, tail, 0, len)
              }
              //val data: String = new String(buff.slice(0, eolIx + 1), StandardCharsets.UTF_8)
              val data = buff.slice(0, eolIx + 1)
              val start = System.currentTimeMillis() // System.nanoTime() //
              logger.info(s"write: segment ${segmentId} gpSeq=${gpSeq} pass=${segPass.getOrElse(segmentId, 0)}" +
                s" nSlices=${nSlices} bytes=${data.length} " +
                //s"${convertBytesToHex(data.slice(0, Math.min(data.length, 10)))} " +
                s".. ")
              rmiSlave.write(data)
              //logger.info(s"write: segment ${segmentId} ok")
              rmiLoopMs.addAndGet(System.currentTimeMillis() - start)
              rmiMsTotal += System.currentTimeMillis() - start
            } else {
              tail = buff
            }

            if (tail.nonEmpty) {
              if (segmentDone != 0) {
                logger.error(s"Truncated row received in GP segment ${segmentId}")
                jobAbort.set(true)
              }
            }
            segmentQueues.put(segmentId, tail)
          }
        }
        val nWritersDone = activeWriters.values.sum
        if (nWritersDone == segmentCount.get()) {
          segmentQueues.clear()
          activeWriters.clear()
          allSegmentsDone.set(true)
        }
        val responseHeaders = exchange.getResponseHeaders
        val rCod = if (!jobAbort.get()) 200 else 500
        responseHeaders.remove("Transfer-encoding")
        responseHeaders.put("Content-type", List("text/plain"))
        responseHeaders.put("Expires", List("0"))
        responseHeaders.put("X-GPFDIST-VERSION", List("6.8.1 build commit:7118e8aca825b743dd9477d19406fcc06fa53852"))
        responseHeaders.put("X-GP-PROTO", List("0"))
        responseHeaders.put("Cache-Control", List("no-cache"))
        responseHeaders.put("Connection", List("close"))
        is.close()
        exchange.sendResponseHeaders(rCod, 0)
        val os = exchange.getResponseBody
        os.flush()
        os.close()
        if (segmentDone == 0) {
          logger.info(s"caller: ${exchange.getRemoteAddress} segment ${segmentId} gpSeq=${gpSeq} " +
            s"pass=${segPass.getOrElse(segmentId, 0)} bytesReadPass=${bytesReadTotal} in " +
            s"timeMs=${System.currentTimeMillis() - passStart}, rmiMs=${rmiMsTotal}, nSlices=${nSlices}")
        } else {
          if (!transferComplete.get() && !jobAbort.get())
            rmiSlave.flush
          logger.info(s"caller transfer complete: ${exchange.getRemoteAddress} segment ${segmentId} gpSeq=${gpSeq} " +
            s"pass=${segPass.getOrElse(segmentId, 0)} bytesReadPass=${bytesReadTotal}, nSlices=${nSlices}")
        }
        if (jobAbort.get() || allSegmentsDone.get()) {
          if (jobAbort.get()) {
            logger.info(s"Query aborted, sent ${rCod} response code")
          } else {
            logger.info(s"All segments done, sent ${rCod} response code")
          }
          transferComplete.set(true)
        }
      }
    }
  })
  server.setExecutor(executor)
  server.start()
}

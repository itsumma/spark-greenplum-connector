package com.itsumma.gpconnector.gpfdist

import com.itsumma.gpconnector.rmi.RMISlave
import com.itsumma.gpconnector.utils.ProgressTracker
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.internal.Logging

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
//import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.collection.JavaConverters._

class WebServerMetrics {
  val rmiLoopMs = new AtomicLong(0)
  val webLoopMs = new AtomicLong(0)
  val processingMs = new AtomicLong(0)
  val totalBytes = new AtomicLong(0)
}

/**
 * Implements GPFDIST protocol natively in scala.
 *
 * @param port     TCP port number to bind the service
 * @param rmiSlave to communicate data with Spark
 */
class WebServer(port: Int, rmiSlave: RMISlave, jobAbort: AtomicBoolean, transferComplete: AtomicBoolean, metrics: WebServerMetrics)
  extends Logging
{

  private val executor = Executors.newFixedThreadPool(10)
  val server: HttpServer = HttpServer.create(new InetSocketAddress(port), 0)

  def httpPort: Int = server.getAddress.getPort

  val progressTracker: ProgressTracker = new ProgressTracker()
  private val rmiLoopMs = metrics.webLoopMs
  private val webLoopMs = metrics.webLoopMs
  private val processingMs = metrics.processingMs
  private val totalBytes = metrics.totalBytes

  private val segmentCount: AtomicInteger = new AtomicInteger(0)
  private val segmentDoneCount: AtomicInteger = new AtomicInteger(0)

  def nSegInProgress: Int = segmentCount.get() - segmentDoneCount.get()

  private val me = new ReentrantLock()

  // Guarded vars:
  private val activeWriters = MutableHashMap[String, Int]()
  private val segmentQueues = MutableHashMap[String, Array[Byte]]()
  private val segPass = MutableHashMap[String, Int]()

  if (!rmiSlave.segService)
    throw new Exception(s"WebServer instance holder should be marked as service provider in its segment")

  def stop(): Unit = me.synchronized {
    server.stop(0)
    executor.shutdown()
    segmentQueues.clear()
    activeWriters.clear()
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
      val segmentId = headers.get("X-gp-segment-id").asScala match {
        case values: mutable.Buffer[String] => values.head
        case _ => throw new Exception(s"No X-GP-SEGMENT-ID")
      }
      var contentLength: Int = 0
      if (headers.containsKey("Content-length"))
        contentLength = headers.get("Content-length").asScala match {
          case values: mutable.Buffer[String] => values.head.toInt
          case _ => 100000
        }
      logDebug(s"Uri: ${uri.toString}, scheme: ${scheme}, method: ${method}, " +
        s"caller: ${exchange.getRemoteAddress}, segmentId: ${segmentId}, contentLength=${contentLength}")
      if (method == "get") {
        val responseHeaders = exchange.getResponseHeaders
        responseHeaders.remove("Transfer-encoding")
        responseHeaders.put("Content-type", List("text/plain").asJava)
        responseHeaders.put("Expires", List("0").asJava)
        responseHeaders.put("X-GPFDIST-VERSION", List("6.8.1 build commit:7118e8aca825b743dd9477d19406fcc06fa53852").asJava)
        responseHeaders.put("X-GP-PROTO", List("1").asJava)
        responseHeaders.put("Cache-Control", List("no-cache").asJava)
        responseHeaders.put("Connection", List("close").asJava)
        exchange.sendResponseHeaders(200, 0)
        logInfo(s"GET for GP segment ${segmentId} enter")
        val os = exchange.getResponseBody
        val start = System.currentTimeMillis() // System.nanoTime() //
        var data = progressTracker.trackProgress("rmiGetMs") {
          rmiSlave.read(10000)
        }
        var nBytes: Long = 0
        var nChunks: Long = 0
        while ((data != null) && (data.position() > 0)) {
          //os.write(data.getBytes(StandardCharsets.UTF_8))
          nChunks += 1
          nBytes += data.position()
          progressTracker.trackProgress("webWriteMs") {
            os.write(data.array(), 0, data.position())
          }
          progressTracker.trackProgress("rmiGetMs") {
            rmiSlave.recycleBuffer(data)
            data = rmiSlave.read(10000)
          }
        }
        progressTracker.trackProgress("webWriteMs") {
          os.flush()
          os.close()
        }
        val ms = System.currentTimeMillis() - start
        rmiLoopMs.addAndGet(ms)
        processingMs.addAndGet(ms)
        totalBytes.addAndGet(nBytes)
        webLoopMs.set(progressTracker.results.getOrElse("webWriteMs", 0))
        logInfo(s"GET for GP segment ${segmentId} complete in ${ms}ms, " +
          s"nBytes=${nBytes} in $nChunks pieces, ${progressTracker.reportTimeTaken()}")
      /***************************************************/
      /**********************-POST-***********************/
      /***************************************************/
      } else if (method == "post") me.synchronized {
        val passStart = System.currentTimeMillis()
        var rmiMsTotal: Long = 0
        var bytesReadTotal: Int = 0
        logTrace(s"""Headers:\n${headers.asScala.mkString("\n")}""")
        var gpSeq: Int = 0
        if (headers.containsKey("X-gp-seq"))
          gpSeq = headers.get("X-gp-seq").asScala match {
            case values: mutable.Buffer[String] => values.head.toInt
            case _ => 0
          }
        if (gpSeq == 1) {
          // Segment transfer initiation
          segmentCount.addAndGet(1)
          val nTotalSeg = headers.get("X-gp-segment-count").asScala match {
            case values: mutable.Buffer[String] => values.head.toInt
            case _ => 0 //throw new Exception(s"No X-GP-SEGMENT-COUNT")
          }
          logDebug(s"POST: starting download for segmentId=$segmentId ${targetName}, " +
            s"segmentCount=${segmentCount.get()}, X-gp-segment-count=$nTotalSeg")
        }
        var segmentDone: Int = 0
        if (headers.containsKey("X-gp-done"))
          segmentDone = headers.get("X-gp-done").asScala match {
            case values: mutable.Buffer[String] => values.head.toInt
            case _ => 0
          }
        if (!activeWriters.contains(segmentId)) {
          activeWriters.put(segmentId, segmentDone)
          segmentQueues.put(segmentId, Array.empty[Byte])
        }
        val thisSegPass = segPass.getOrElse(segmentId, 0) + 1
        segPass.put(segmentId, thisSegPass)
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
                        logWarning(s"caller ${exchange.getRemoteAddress}: body stream hanged, available so far ${is.available()}")
                        avBreak = true
                      }
                      Thread.sleep(10)
                    }
          */
          var nRead = progressTracker.trackProgress("webReadMs") {
            is.read(buff, prevLen, contentLength)
          }
          while ((nRead > 0) && !jobAbort.get()) {
            nSlices += 1
/*
            logDebug(s"read: segment ${segmentId} gpSeq=${gpSeq} pass=${segPass.getOrElse(segmentId, 0)}" +
              s" slice=${nSlices} nRead=${nRead} " +
              //s"${convertBytesToHex(buff.slice(0, Math.min(nRead, 10)))} " +
              s".. ")
*/
            bytesReadTotal += nRead
            if ((bytesReadTotal < contentLength) && !jobAbort.get()) {
              logTrace(s"caller: ${exchange.getRemoteAddress} will block, bytesReadTotal=${bytesReadTotal}")
              nRead = progressTracker.trackProgress("webReadMs") {
                is.read(buff, prevLen + bytesReadTotal, contentLength - bytesReadTotal)
              }
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
              logDebug(s"write: segment ${segmentId} gpSeq=${gpSeq} pass=${segPass.getOrElse(segmentId, 0)}" +
                s" nSlices=${nSlices} bytes=${data.length} " +
                //s"${convertBytesToHex(data.slice(0, Math.min(data.length, 10)))} " +
                s".. ")
              progressTracker.trackProgress("rmiWriteMs") {
                rmiSlave.write(data)
              }
              //logDebug(s"write: segment ${segmentId} ok")
              rmiLoopMs.addAndGet(System.currentTimeMillis() - start)
              rmiMsTotal += System.currentTimeMillis() - start
            } else {
              tail = buff
            }

            if (tail.nonEmpty) {
              if (segmentDone != 0) {
                logError(s"Truncated row received in GP segment ${segmentId}")
                jobAbort.set(true)
              }
            }
            segmentQueues.put(segmentId, tail)
          }
        }
      /*
        val nWritersDone = activeWriters.values.sum
        if (nWritersDone == segmentCount.get()) {
          segmentQueues.clear()
          activeWriters.clear()
          allSegmentsDone.set(true)
        }
      */
        val responseHeaders = exchange.getResponseHeaders
        val rCod = if (!jobAbort.get()) 200 else 500
        responseHeaders.remove("Transfer-encoding")
        responseHeaders.put("Content-type", List("text/plain").asJava)
        responseHeaders.put("Expires", List("0").asJava)
        responseHeaders.put("X-GPFDIST-VERSION", List("6.8.1 build commit:7118e8aca825b743dd9477d19406fcc06fa53852").asJava)
        responseHeaders.put("X-GP-PROTO", List("0").asJava)
        responseHeaders.put("Cache-Control", List("no-cache").asJava)
        responseHeaders.put("Connection", List("close").asJava)
        progressTracker.trackProgress("webReadMs") {
          is.close()
        }
        exchange.sendResponseHeaders(rCod, 0)
        val os = exchange.getResponseBody
        os.flush()
        os.close()
        processingMs.addAndGet(System.currentTimeMillis() - passStart)
        totalBytes.addAndGet(bytesReadTotal)
        if (segmentDone == 0) {
          logDebug(s"POST: ${exchange.getRemoteAddress} segmentId=${segmentId} gpSeq=${gpSeq} " +
            s"pass=${thisSegPass} bytesReadPass=${bytesReadTotal} in " +
            s"timeMs=${System.currentTimeMillis() - passStart}, rmiMs=${rmiMsTotal}, nSlices=${nSlices}")
        } else {
          segmentDoneCount.addAndGet(1)
          if (!jobAbort.get()) {
            progressTracker.trackProgress("rmiWriteMs") {
              rmiSlave.flush()
            }
          }
          if ((contentLength == 0) && (gpSeq > 1))
            transferComplete.set(true)
          logDebug(s"POST: segment transfer complete for segmentId=${segmentId} ${exchange.getRemoteAddress}, " +
            s"gpSeq=${gpSeq}, all segments: started=${segmentCount.get()}, complete=${segmentDoneCount.get()}, " +
            s"totalBytes=${totalBytes.get()}, processingMs=${processingMs.get()}, sqlComplete=${transferComplete.get()}"
            )
        }
        webLoopMs.set(progressTracker.results.getOrElse("webReadMs", 0))
        /*
        if (jobAbort.get() || allSegmentsDone.get()) {
          if (jobAbort.get()) {
            logInfo(s"POST for GP segment ${segmentId} aborted after ${processingMs.get()}ms, " +
              s"nBytes=${totalBytes.get()} in ${segPass.getOrElse(segmentId, 0)} pieces, " +
              s"rmiLoopMs=${rmiLoopMs.get()}, " +
              s"${progressTracker.reportTimeTaken()}")
          } else {
            progressTracker.trackProgress("rmiWriteMs") {
              rmiSlave.flush()
            }
            logInfo(s"POST for ${segmentCount.get()} GP segments complete in ${processingMs.get()}ms, " +
              s"nBytes=${totalBytes.get()} in ${segPass.getOrElse(segmentId, 0)} pieces, " +
              s"rmiLoopMs=${rmiLoopMs.get()}, " +
              s"${progressTracker.reportTimeTaken()}")
            Thread.sleep(20)
          }
          transferComplete.set(true)
        }
        */
      } //post
    }
  })
  server.setExecutor(executor)
  server.start()
}

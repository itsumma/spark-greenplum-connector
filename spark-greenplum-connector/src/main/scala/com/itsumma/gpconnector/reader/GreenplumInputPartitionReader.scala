package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.rmi.RMISlave
import com.itsumma.gpconnector.utils.ProgressTracker
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkEnv, TaskContext}

import java.io.{IOException, StringReader}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.language.postfixOps

//import scala.util.control.Breaks

// scalafix:off DisableSyntax.null
// scalastyle:off null

class GreenplumInputPartitionReader(optionsFactory: GPOptionsFactory,
                                    queryId: String,
                                    schema: StructType,
                                    partitionNo: Int,
                                    rmiRegistry: String
                                    )
  extends PartitionReader[InternalRow] with Logging
{
  val epochId: Long = Option(TaskContext.get().getLocalProperty("streaming.sql.batchId")).getOrElse("0").toLong
  private val partitionId: Int = TaskContext.getPartitionId()
  private val stageId: Int = TaskContext.get.stageId()
  private val exId: String = SparkEnv.get.executorId
  private val progressTracker: ProgressTracker = new ProgressTracker()

  private var schemaOrPlaceholder = schema
  if (schemaOrPlaceholder.isEmpty) {
    schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
  }
  private val fieldDelimiter = '\t'
  private val lines = new mutable.Queue[InternalRow]()
  private var dataLine: InternalRow = null
  private val rowCount = new AtomicLong(0)
  private val rmiGetMs = new AtomicLong(0)
  logDebug(s"Creating instance epoch=${epochId} on executor ${exId} for partition ${partitionId}")
  private var rmiSlave: RMISlave = new RMISlave(optionsFactory, rmiRegistry, queryId, true, exId, partitionNo, partitionId, stageId)
  private val gpfdistUrl: String = rmiSlave.gpfdistUrl
  if (rmiSlave.connected)
    rmiSlave.getSegServiceProvider

  logDebug(s"gpfdist started on ${gpfdistUrl}")

  private var eofReached: Boolean = false
  private var nextSliceNo: Int = 0

  override def next(): Boolean = this.synchronized {
    dataLine = null
    if (rmiSlave == null || !rmiSlave.connected)
      return false
    if (lines.nonEmpty) {
      dataLine = lines.dequeue()
      return true
    } else {
      val waitStart = System.currentTimeMillis()
      val buff = rmiSlave.read(optionsFactory.gpfdistTimeout) // Locks here
      var newData: Array[Byte] = null //rmiSlave.get(optionsFactory.gpfdistTimeout)
      if ((buff != null) && (buff.position() > 0))
        newData = buff.array() //util.Arrays.copyOf(buff.array(), buff.position())
      rmiGetMs.addAndGet(System.currentTimeMillis() - waitStart)
      if (newData == null || newData.isEmpty) { // End of stream
        eofReached = true
        logDebug(s"${gpfdistUrl} epoch=${epochId} end of stream, rowCount=${rowCount.get().toString}")
        closeInternal()
        return false
      }
      val newDataLen = buff.position() //newData.length
      nextSliceNo += 1
      logDebug(s"next newData.size=${newDataLen}, sliceNo=${nextSliceNo}")

      //val start = System.currentTimeMillis()
      var rowNum: Long = 0
      var enqueueNs: Long = 0
      var convNs: Long = 0
      var sliceStart: Int = 0
      progressTracker.trackProgress("parseMs") {
        for (ix <- 0 until newDataLen) {
          if (newData(ix) == 0x0a) {
            val startConv = System.nanoTime()
            val rowString = progressTracker.trackProgress("splitRows") {
              if (sliceStart < ix) new String(newData.slice(sliceStart, ix), StandardCharsets.UTF_8) else ""
            }
            val fields = progressTracker.trackProgress("splitFields") {
              if (schema.nonEmpty) rowString.split(fieldDelimiter) else "".split(fieldDelimiter)
            }
            val row = progressTracker.trackProgress("parseFields") {
              SparkSchemaUtil(optionsFactory.dbTimezone).textToInternalRow(schemaOrPlaceholder, fields)
            }
            val startEnq = System.nanoTime()
            convNs += System.nanoTime() - startConv
            lines.enqueue(row)
            enqueueNs += System.nanoTime() - startEnq
            rowNum += 1
            sliceStart = ix + 1
          }
        }
      }
      if (buff != null)
        rmiSlave.recycleBuffer(buff)
      if (sliceStart != newDataLen) {
        logError(s"Unterminated row received after ${rowNum} rows")
        throw new IOException(s"Unterminated row received after ${rowNum} rows")
      }

      logDebug(s"next slice ${nextSliceNo} parsed with ${rowNum} rows, enqueueNs=${enqueueNs}, convNs=${convNs}")
      dataLine = lines.dequeue()
      true
    }
  }

  override def get(): InternalRow = this.synchronized {
    if (dataLine == null) {
      logDebug(s"${gpfdistUrl} epoch=${epochId} get returns null, rowCount=${rowCount.get().toString}")
      return null
    }
    rowCount.incrementAndGet()
    dataLine
  }

  override def close(): Unit = synchronized {
    closeInternal()
  }

  private def closeInternal(): Unit = {
    if (rmiSlave == null)
      return
    var rmiPutMs: Long = 0
    var gpfReport: String = ""
    logTrace(s"closeInternal enter, eofReached=${eofReached}")
    try {
      progressTracker.trackProgress("gpfCommitMs") {
        if (rmiSlave.connected || rmiSlave.sqlTransferComplete.get()) {
          if (eofReached) {
            rmiSlave.commit(rowCount.get(), optionsFactory.networkTimeout) // Will block until GPFDIST transfer complete
          } else {
            rmiSlave.abort(rowCount.get(), optionsFactory.networkTimeout)
          }
        }
      }
      gpfReport = rmiSlave.gpfReport
    } catch {
      case e: Exception => logError(s"${e.getClass.getCanonicalName}:${e.getMessage} " +
        s"${e.getStackTrace.mkString("", "\n", "")}")
    } finally {
      progressTracker.trackProgress("gpfStopMs") {
        rmiPutMs = rmiSlave.stop
      }
      rmiSlave = null
    }
    logInfo(s"Destroyed ${gpfdistUrl}, epoch=${epochId}, " +
      s"rowCount=${rowCount.get()}, rmiPutMs=${rmiPutMs}, rmiGetMs=${rmiGetMs.get()}," +
      s"\n${progressTracker.reportTimeTaken()},\n${gpfReport}")
    lines.clear()
  }
}

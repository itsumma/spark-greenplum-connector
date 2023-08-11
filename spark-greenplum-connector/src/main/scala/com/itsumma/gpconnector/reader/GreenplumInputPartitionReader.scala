package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.rmi.RMISlave
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.charset.StandardCharsets
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
                                    ) extends InputPartitionReader[InternalRow] {
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val partitionId: Int = TaskContext.getPartitionId()
  private val stageId: Int = TaskContext.get.stageId()
  private val exId: String = SparkEnv.get.executorId
  //private val instanceId: String = s"$partitionNo:$partitionId:$stageId"
  //private val localIpAddress: String = InetAddress.getLocalHost.getHostAddress

  private var schemaOrPlaceholder = schema
  if (schemaOrPlaceholder.isEmpty) {
    schemaOrPlaceholder = SparkSchemaUtil.getGreenplumPlaceholderSchema(optionsFactory)
  }
  private val fieldDelimiter = '\t'
  private val lines = new mutable.Queue[InternalRow]()
  private var dataLine: InternalRow = null
  private val rowCount = new AtomicLong(0)
  private val rmiGetMs = new AtomicLong(0)
  logger.debug(s"Creating GreenplumInputPartitionReader instance on executor ${exId} for partition ${partitionId}")
  private var rmiSlave: RMISlave = new RMISlave(optionsFactory, rmiRegistry, queryId, true, exId, partitionNo, partitionId, stageId)
  private val gpfdistUrl: String = rmiSlave.gpfdistUrl
  if (rmiSlave.connected)
    rmiSlave.getSegServiceProvider

  logger.debug(s"gpfdist started on ${gpfdistUrl}")

  var eofReached: Boolean = false
  private var nextSliceNo: Int = 0

  override def next(): Boolean = this.synchronized {
    dataLine = null
    if (!rmiSlave.connected)
      return false
    if (lines.nonEmpty) {
      dataLine = lines.dequeue()
      return true
    } else {
      val waitStart = System.currentTimeMillis()
      val newData: Array[Byte] = rmiSlave.get(optionsFactory.gpfdistTimeout) // Locks here
      rmiGetMs.addAndGet(System.currentTimeMillis() - waitStart)
      if (newData == null || newData.isEmpty) { // End of stream
        eofReached = true
        logger.info(s"${gpfdistUrl} end of stream, rowCount=${rowCount.get().toString}")
        return false
      }
      val newDataLen = newData.length
      nextSliceNo += 1
      logger.debug(s"next newData.size=${newDataLen}, sliceNo=${nextSliceNo}")

      //val start = System.currentTimeMillis()
      var rowNum: Long = 0
      var enqueueUs: Long = 0
      var convMs: Long = 0
      var sliceStart: Int = 0
      for (ix <- 0 until newDataLen) {
        if (newData(ix) == 0x0a) {
          val startConv = System.nanoTime()
          val rowString = if (sliceStart < ix) new String(newData.slice(sliceStart, ix), StandardCharsets.UTF_8) else ""
          val fields = if (schema.nonEmpty) rowString.split(fieldDelimiter) else "".split(fieldDelimiter)
          val row = SparkSchemaUtil(optionsFactory.dbTimezone).textToInternalRow(schemaOrPlaceholder, fields)
          val startEnq = System.nanoTime()
          convMs += System.nanoTime() - startConv
          lines.enqueue(row)
          enqueueUs += System.nanoTime() - startEnq
          rowNum += 1
          sliceStart = ix + 1
        }
      }
      if (sliceStart != newDataLen) {
        logger.error(s"Unterminated row received after ${rowNum} rows")
        throw new IOException(s"Unterminated row received after ${rowNum} rows")
      }

      logger.debug(s"next slice ${nextSliceNo} parsed with ${rowNum} rows, enqueueNs=${enqueueUs}, convNs=${convMs}")
      dataLine = lines.dequeue()
      true
    }
  }

  override def get(): InternalRow = this.synchronized {
    if (dataLine == null) {
      logger.info(s"${gpfdistUrl} get returns null, rowCount=${rowCount.get().toString}")
      return null
    }
    rowCount.incrementAndGet()
    dataLine
  }

  override def close(): Unit = synchronized {
    if (rmiSlave == null)
      return
    var rmiPutMs: Long = 0
    try {
      if (rmiSlave.connected || rmiSlave.sqlTransferComplete.get()) {
        if (eofReached) {
          rmiSlave.commit(rowCount.get(), optionsFactory.networkTimeout) // Will block until GPFDIST transfer complete
        } else {
          rmiSlave.abort(rowCount.get(), optionsFactory.networkTimeout)
        }
      }
    } catch {
      case e: Exception => logger.error(s"${e.getClass.getCanonicalName}:${e.getMessage} " +
        s"${e.getStackTrace.mkString("", "\n", "")}")
    } finally {
      rmiPutMs = rmiSlave.stop
      rmiSlave = null
    }
    logger.info(s"Destroyed ${gpfdistUrl}, " +
      s"rowCount=${rowCount.get()}, rmiPutMs=${rmiPutMs}, rmiGetMs=${rmiGetMs.get()}")
    lines.clear()
  }
}

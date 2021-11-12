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
import scala.language.postfixOps
import scala.collection.mutable.{ArrayBuffer, Queue}
//import scala.util.control.Breaks

// scalafix:off DisableSyntax.null
// scalastyle:off null

class GreenplumInputPartitionReader(optionsFactory: GPOptionsFactory,
                                    queryId: String,
                                    instanceUUID: String,
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
  private val fieldDelimiter = '|'
  private val lines = new Queue[InternalRow]()
  private var dataLine: InternalRow = null
  private val rowCount = new AtomicLong(0)
  private val rmiGetMs = new AtomicLong(0)
  logger.info(s"${(System.currentTimeMillis() % 1000).toString} Creating GreenplumInputPartitionReader instance on executor ${exId} for partition ${partitionId}")
  private val rmiSlave: RMISlave = new RMISlave(optionsFactory, rmiRegistry, queryId, true, exId, partitionNo, partitionId, stageId)
  private val gpfdistUrl: String = rmiSlave.gpfdistUrl
  if (rmiSlave.connected)
    rmiSlave.getSegServiceProvider

  logger.info(s"${(System.currentTimeMillis() % 1000).toString} gpfdist started on ${gpfdistUrl}")

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
      val newData: Array[Byte] = rmiSlave.get() // Locks here
      rmiGetMs.addAndGet(System.currentTimeMillis() - waitStart)
      if (newData == null || newData.isEmpty) { // End of stream
        eofReached = true
        logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} end of stream, rowCount=${rowCount.get().toString}")
        return false
      }
      val newDataLen = newData.length
      nextSliceNo += 1
      logger.info(s"next newData.size=${newDataLen}, sliceNo=${nextSliceNo}")

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

      logger.info(s"next slice ${nextSliceNo} parsed with ${rowNum} rows, enqueueNs=${enqueueUs}, convNs=${convMs}")
      dataLine = lines.dequeue()
      true
    }
  }

  override def get(): InternalRow = this.synchronized {
    if (dataLine == null) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} ${gpfdistUrl} get returns null, rowCount=${rowCount.get().toString}")
      return null
    }
    rowCount.incrementAndGet()
    dataLine
  }

  override def close(): Unit = synchronized {
    var rmiPutMs: Long = 0
    try {
      if (rmiSlave.connected) {
        if (eofReached) {
          rmiSlave.commit(rowCount.get()) // Will block until GPFDIST transfer complete
        } else {
          rmiSlave.abort(rowCount.get())
        }
      }
    } catch {
      case e: Exception => logger.error(s"${e.getMessage} ${e.getStackTrace}")
    } finally {
      rmiPutMs = rmiSlave.stop
    }
    logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl}, " +
      s"rowCount=${rowCount.get()}, rmiPutMs=${rmiPutMs}, rmiGetMs=${rmiGetMs.get()}")
    lines.clear()
  }
}

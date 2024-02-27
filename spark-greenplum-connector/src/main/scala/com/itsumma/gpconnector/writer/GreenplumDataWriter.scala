package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.rmi.{NetUtils, RMISlave}
import com.itsumma.gpconnector.utils.ProgressTracker
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, TaskContext}
//import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps

final case class GreenplumConnectionExpired(private val message: String = "",
                                      private val cause: Throwable = None.orNull) extends Exception(message, cause)

case class GreenplumWriterCommitMessage(writeUUID : String, instanceId: String, gpfdistUrl: String,
                                        partitionId : Int, taskId : Long, epochId : Long,
                                        nRowsWritten : Long, rmiPutMs: Long, rmiGetMs: Long,
                                        nBytesWritten : Long, webTransferMs: Long
                                       )
  extends WriterCommitMessage
{}

class GreenplumDataWriter(writeUUID: String, schema: StructType, optionsFactory: GPOptionsFactory, rmiRegistry: String,
                          partitionId: Int, taskId: Long, batchId: Option[Long] = None)
  extends DataWriter[InternalRow] with Logging
{
  val epochId: Long = batchId.getOrElse(
    Option(TaskContext.get().getLocalProperty("streaming.sql.batchId")).getOrElse("0").toLong)
  private val exId: String = SparkEnv.get.executorId
  private val (_, localIpAddress: String) = NetUtils().getLocalHostNameAndIp
    //InetAddress.getLocalHost.getHostAddress
  private val fieldDelimiter = '\t'
  private val rowCount = new AtomicLong(0)
  private val rmiPutMs = new AtomicLong(0)

  private var serviceInstanceController: Boolean = true
  private var gpfdistUrl: String = null

  private val instanceId: String = s"$partitionId:$taskId:$epochId"
  private var rmiSlave: RMISlave = null
  private val progressTracker: ProgressTracker = new ProgressTracker()

  //init()

  private def init(): Unit = {
    rmiSlave = new RMISlave(optionsFactory, rmiRegistry, writeUUID, false, exId, partitionId, taskId, epochId)
    serviceInstanceController = rmiSlave.segService
    gpfdistUrl = rmiSlave.gpfdistUrl
    if (serviceInstanceController) {
      logDebug(s" " +
        s"GPFDIST controller instance started with port ${rmiSlave.servicePort}, " +
        s"writeUUID=$writeUUID, exId=$exId, partNo=$partitionId, instanceId=$instanceId")
    } else {
      logDebug(s"partitionNo=${partitionId}(instanceId=$instanceId): " +
        s"Slave instance started at: writeUUID=${writeUUID}, nodeIp=${localIpAddress}, gpfUrl='${gpfdistUrl}', " +
        s"executor_id=${exId}")
    }
    try {
    rmiSlave.getSegServiceProvider
    } catch {
      case e: java.rmi.NoSuchObjectException =>
        throw GreenplumConnectionExpired(s"Database connection expired after ${optionsFactory.networkTimeout} ms of inactivity." +
          s" Probably you should retry you action.")
    }
  }

  override def write(t: InternalRow): Unit = {
    this.synchronized {
      if (rmiSlave == null)
        init()
    }
    val txt = progressTracker.trackProgress("row2text") {
      SparkSchemaUtil(optionsFactory.dbTimezone).internalRowToText(schema, t, fieldDelimiter)
    }
    //logDebug(s"write numFields=${t.numFields}, row(${rowCount.get()})='${txt}'")
    val writeStart: Long = System.currentTimeMillis()
    rmiSlave.write(txt + '\n')
    rmiPutMs.addAndGet(System.currentTimeMillis() - writeStart)
    rowCount.incrementAndGet()
  }

  override def commit(): WriterCommitMessage = {
    var rmiGetMs: Long = 0
    var webTransferMs: Long = 0
    var nBytes: Long = 0
    var gpfReport: String = ""
    val valid: Boolean = this.synchronized{
      rmiSlave != null
    }
    if (valid) {
      val maxWait = if (optionsFactory.gpfdistTimeout > 0) optionsFactory.gpfdistTimeout else optionsFactory.networkTimeout
      try {
        progressTracker.trackProgress("commit") {
          rmiSlave.commit(rowCount.get(), maxWait) // Will block until GPFDIST transfer complete
        }
        gpfReport = rmiSlave.gpfReport
      } finally {
        webTransferMs = rmiSlave.transferMs
        nBytes = rmiSlave.transferBytes
        progressTracker.trackProgress("gpfStopMs") {
          rmiGetMs = rmiSlave.stop
        }
        rmiSlave = null
        }
    }
    if (serviceInstanceController) {
      logInfo(s"Destroyed ${gpfdistUrl} by commit, epochId=${epochId} " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}\n" +
        s"${progressTracker.reportTimeTaken()}\n" +
        s"${gpfReport}")
    }
    val ret = GreenplumWriterCommitMessage(writeUUID=writeUUID, instanceId=instanceId, gpfdistUrl=gpfdistUrl,
      partitionId=partitionId, taskId=taskId, epochId=epochId,
      nRowsWritten=rowCount.get(), rmiPutMs=rmiPutMs.get(), rmiGetMs=rmiGetMs,
      nBytesWritten=nBytes, webTransferMs=webTransferMs
    )
    rowCount.set(0)
    rmiPutMs.set(0)
    ret
  }

  override def abort(): Unit = {
    var rmiGetMs: Long = 0
    val valid: Boolean = this.synchronized {
      rmiSlave != null
    }
    if (valid) {
    try {
      rmiSlave.abort(rowCount.get(), optionsFactory.networkTimeout)
    } finally {
      rmiGetMs = rmiSlave.stop
        rmiSlave = null
      }
    }
    if (serviceInstanceController) {
      logDebug(s"Destroyed ${gpfdistUrl} by abort, " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}")
    }
  }

  override def close(): Unit = {}
}

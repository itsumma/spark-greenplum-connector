package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.rmi.{NetUtils, RMISlave}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps

final case class GreenplumConnectionExpired(private val message: String = "",
                                      private val cause: Throwable = None.orNull) extends Exception(message, cause)

case class GreenplumWriterCommitMessage(writeUUID : String, instanceId: String, gpfdistUrl: String,
                                        partitionId : Int, taskId : Long, epochId : Long,
                                        nRowsWritten : Long, rmiPutMs: Long, rmiGetMs: Long)
  extends WriterCommitMessage
{}

class GreenplumDataWriter(writeUUID: String, schema: StructType, optionsFactory: GPOptionsFactory, rmiRegistry: String,
                          partitionId: Int, taskId: Long, epochId: Long)
  extends DataWriter[InternalRow]
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
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

  //init()

  def init(): Unit = {
    rmiSlave = new RMISlave(optionsFactory, rmiRegistry, writeUUID, false, exId, partitionId, taskId, epochId)
    serviceInstanceController = rmiSlave.segService
    gpfdistUrl = rmiSlave.gpfdistUrl
    if (serviceInstanceController) {
      logger.debug(s" " +
        s"GPFDIST controller instance started with port ${rmiSlave.servicePort}, " +
        s"writeUUID=$writeUUID, exId=$exId, partNo=$partitionId, instanceId=$instanceId")
    } else {
      logger.debug(s" partitionNo=${partitionId}(instanceId=$instanceId): " +
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
    val txt = SparkSchemaUtil(optionsFactory.dbTimezone).internalRowToText(schema, t, fieldDelimiter)
    //logger.debug(s" write numFields=${t.numFields}, row(${rowCount.get()})='${txt}'")
    val writeStart: Long = System.currentTimeMillis()
    rmiSlave.write(txt + '\n')
    rmiPutMs.addAndGet(System.currentTimeMillis() - writeStart)
    rowCount.incrementAndGet()
  }

  override def commit(): WriterCommitMessage = {
    var rmiGetMs: Long = 0
    val valid: Boolean = this.synchronized{
      rmiSlave != null
    }
    if (valid) {
      val maxWait = if (optionsFactory.gpfdistTimeout > 0) optionsFactory.gpfdistTimeout else optionsFactory.networkTimeout
      try {
        rmiSlave.commit(rowCount.get(), maxWait) // Will block until GPFDIST transfer complete
      } finally {
        rmiGetMs = rmiSlave.stop
        rmiSlave = null
      }
    }
    if (serviceInstanceController) {
      logger.debug(s" Destroyed ${gpfdistUrl} by commit, " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}")
    }
    val ret = GreenplumWriterCommitMessage(writeUUID=writeUUID, instanceId=instanceId, gpfdistUrl=gpfdistUrl,
      partitionId=partitionId, taskId=taskId, epochId=epochId,
      nRowsWritten=rowCount.get(), rmiPutMs=rmiPutMs.get(), rmiGetMs=rmiGetMs)
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
      logger.debug(s" Destroyed ${gpfdistUrl} by abort, " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}")
    }
  }
}

package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.rmi.{RMISlave}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps

case class GreenplumWriterCommitMessage(writeUUID : String, instanceId: String, gpfdistUrl: String,
                                        partitionId : Int, taskId : Long, epochId : Long,
                                        nRowsWritten : Long, rmiPutMs: Long, rmiGetMs: Long)
  extends WriterCommitMessage
{}

class GreenplumDataWriter(writeUUID: String, schema: StructType, saveMode: SaveMode, optionsFactory: GPOptionsFactory,
                          partitionId: Int, taskId: Long, epochId: Long, nGpSegments: Int, rmiRegistry: String)
  extends DataWriter[InternalRow]
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val exId: String = SparkEnv.get.executorId
  private val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
  private val fieldDelimiter = '|'
  private val rowCount = new AtomicLong(0)
  private val rmiPutMs = new AtomicLong(0)

  private var serviceInstanceController: Boolean = true
  private var gpfdistUrl: String = null

  private val instanceId: String = s"$partitionId:$taskId:$epochId"
  private var rmiSlave: RMISlave = null

  init()

  def init(): Unit = {
    rmiSlave = new RMISlave(optionsFactory, rmiRegistry, writeUUID, false, exId, partitionId, taskId, epochId)
    serviceInstanceController = rmiSlave.segService
    gpfdistUrl = rmiSlave.gpfdistUrl
    if (serviceInstanceController) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} " +
        s"GPFDIST controller instance started with port ${rmiSlave.servicePort}, " +
        s"writeUUID=$writeUUID, exId=$exId, partNo=$partitionId, instanceId=$instanceId")
    } else {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} partitionNo=${partitionId}(instanceId=$instanceId): " +
        s"Slave instance started at: writeUUID=${writeUUID}, nodeIp=${localIpAddress}, gpfUrl='${gpfdistUrl}', " +
        s"executor_id=${exId}")
    }
    rmiSlave.getSegServiceProvider
  }

  override def write(t: InternalRow): Unit = {
    val txt = SparkSchemaUtil(optionsFactory.dbTimezone).internalRowToText(schema, t, fieldDelimiter)
    //logger.info(s"${(System.currentTimeMillis() % 1000).toString} write numFields=${t.numFields}, row(${rowCount.get()})='${txt}'")
    val writeStart: Long = System.currentTimeMillis()
    rmiSlave.write(txt + '\n')
    rmiPutMs.addAndGet(System.currentTimeMillis() - writeStart)
    rowCount.incrementAndGet()
  }

  override def commit(): WriterCommitMessage = {
    var rmiGetMs: Long = 0
    try {
      rmiSlave.commit(rowCount.get()) // Will block until GPFDIST transfer complete
    } finally {
      rmiGetMs = rmiSlave.stop
    }
    if (serviceInstanceController) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl} by commit, " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}")
    }
    GreenplumWriterCommitMessage(writeUUID=writeUUID, instanceId=instanceId, gpfdistUrl=gpfdistUrl,
      partitionId=partitionId, taskId=taskId, epochId=epochId,
      nRowsWritten=rowCount.get(), rmiPutMs=rmiPutMs.get(), rmiGetMs=rmiGetMs)
  }

  override def abort(): Unit = {
    var rmiGetMs: Long = 0
    try {
      rmiSlave.abort(rowCount.get())
    } finally {
      rmiGetMs = rmiSlave.stop
    }
    if (serviceInstanceController) {
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl} by abort, " +
        s"write rowCount=${rowCount.get().toString}, rmiPutMs=${rmiPutMs.get()}, rmiGetMs=${rmiGetMs}")
    }
  }
}

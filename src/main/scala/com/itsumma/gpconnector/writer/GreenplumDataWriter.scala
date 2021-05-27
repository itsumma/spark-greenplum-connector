package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.{ExternalProcessUtil, GPClient}
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.net.InetAddress
import java.nio.file.{Files, Path, Paths}
import java.io.{BufferedWriter, FileOutputStream, FileWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps
// import scala.util.control.Breaks
import scala.sys.process._

case class GreenplumWriterCommitMessage(writeUUID : String, instanceId: String, gpfdistUrl: String,
                                        partitionId : Int, taskId : Long, epochId : Long, nRowsWritten : Long)
  extends WriterCommitMessage
{}

class GreenplumDataWriter(writeUUID: String, instanceId: String, schema: StructType, saveMode: SaveMode, optionsFactory: GPOptionsFactory,
                          partitionId: Int, taskId: Long, epochId: Long)
  extends DataWriter[InternalRow]
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val exId: String = SparkEnv.get.executorId
  private val tempDir: Path = Paths.get("/tmp", "gp", "writer", writeUUID) //, exId)
    // Files.createDirectories(Paths.get("/tmp", "gp", "writer", writeUUID, instanceId, exId)) // port.toString
  private val pipeFile: Path = Paths.get(tempDir.toString, s"output.pipe")
  private val localIpAddress: String = InetAddress.getLocalHost.getHostAddress
  private val fieldDelimiter = '|'
  private val rowCount = new AtomicLong(0)
  private val dbConnection: Connection = GPClient.getConn(optionsFactory)
  dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
  dbConnection.setAutoCommit(false)

  private var serviceInstanceController: Boolean = true
  private var gpfdistUrl: String = null
  private var gpfdistProcess: java.lang.Process = null

  init()

  def init(): Unit = {
    var nInstances = 0
    var nInstancesUpdated = 0
    val rnd = new scala.util.Random
    while (!Thread.currentThread().isInterrupted && (nInstances == 0)) {
      try {
        nInstances = 0
        SparkSchemaUtil.using(dbConnection.prepareStatement(s"select node_ip, part_id, dir, part_url from gp_spark_instance where query_id = ? " +
          s"and status is null and dt_end is null " +
          s"for update")) {
          statement => {
            statement.setString(1, writeUUID)
            SparkSchemaUtil.using(statement.executeQuery()) {
              rs => {
                while (serviceInstanceController && rs.next()) {
                  nInstances += 1
                  val nodeIp = rs.getString(1)
                  val partId = rs.getInt(2)
                  val dir = rs.getString(3)
                  if ((nodeIp == localIpAddress) && (dir == "W")) {
                    serviceInstanceController = false
                    gpfdistUrl = rs.getString(4)
                    logger.info(s"${(System.currentTimeMillis() % 1000).toString} partition=${partitionId}: " +
                      s"Adhere to gpfdist instance at: writeUUID=${writeUUID}, nodeIp=${nodeIp}, gpfUrl='${gpfdistUrl}', controllerPart=${partId}")
                    val nRows = GPClient.executeStatement(dbConnection, s"update gp_spark_instance " +
                      s"set node_ip = '${localIpAddress}', dir = 'w', part_url = '${gpfdistUrl}', executor_id = ${exId} " +
                      s"where query_id = '${writeUUID}' and part_id = ${partitionId}")
                    if (nRows == 0)
                      throw new Exception("update gp_spark_instance returns 0")
                    nInstancesUpdated += nRows
                    // s"mkfifo ${pipeFile.toString}".!
                  }
                }
                if ((nInstances > 0) && serviceInstanceController) {
                  ExternalProcessUtil.deleteRecursively(tempDir)
                  Files.createDirectories(tempDir)
                  s"mkfifo ${pipeFile.toString}".!
                  val (proc, port) = ExternalProcessUtil(optionsFactory, tempDir, inheritStdIO = true).gpfdistProcess(true)
                  gpfdistProcess = proc
                  gpfdistUrl = s"gpfdist://${localIpAddress}:${port}/${pipeFile.getFileName.toString}"
                  // gpfdistUrl = s"gpfdist://${localIpAddress}:${port}/*"
                  while (!Thread.currentThread().isInterrupted && !GPClient.checkGpfdistIsUp(localIpAddress, port)) {
                    Thread.sleep(10)
                  }
                  logger.info(s"${(System.currentTimeMillis() % 1000).toString} gpfdist started on port ${port}, " +
                    s"writeUUID=$writeUUID, exId=$exId, partitionId=$partitionId, taskId=$taskId")
                  val nRows = GPClient.executeStatement(dbConnection, s"update gp_spark_instance " +
                    s"set node_ip = '${localIpAddress}', dir = 'W', part_url = '${gpfdistUrl}', executor_id = ${exId} " +
                    s"where query_id = '${writeUUID}' and part_id = ${partitionId}")
                  if (nRows == 0)
                    throw new Exception("update gp_spark_instance returns 0")
                  nInstancesUpdated += nRows
                }
              }
            }
          }
        }
        try {
          dbConnection.commit()
        } catch {
          case _: Throwable =>
        }
        if (nInstances == 0)
          Thread.sleep(rnd.nextInt( (100 - 10) + 1 ))
      } catch {
        case e: Exception => {
          if (e.getMessage.contains("deadlock detected")) {
            logger.info(s"Postgres spuriously reports deadlock condition at GreenplumDataWriter.init:61 - there is no deadlock possible here")
            try {
              dbConnection.rollback()
              Thread.sleep(rnd.nextInt( (100 - 10) + 1 ))
            } catch {
              case _: Throwable =>
            }
          } else {
            throw e
          }
        }
      }
    }
    //  if (nInstancesUpdated > 0)
    //    dbConnection.commit()
  }

  def countActiveParts: Int = {
    var cnt: Int = 0
    SparkSchemaUtil.using(dbConnection.prepareStatement(s"select count(1) from gp_spark_instance " +
      s"where query_id = ? and node_ip = ? and (dt_end is null or status is null)")) {
      statement => {
        statement.setString(1, writeUUID)
        statement.setString(2, localIpAddress)
        SparkSchemaUtil.using(statement.executeQuery()) {
          rs => {
            if (rs.next())
              cnt = rs.getInt(1)
          }
        }
      }
    }
    try {
      dbConnection.commit()
    } catch {
      case _ : Throwable =>
    }
    cnt
  }

  private var output: PrintWriter = null

  override def write(t: InternalRow): Unit = {
    if (rowCount.get() == 0) {
      //init()
      output = new PrintWriter(
        // new FileWriter(pipeFile.toString, true)
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pipeFile.toString, true), StandardCharsets.UTF_8))
        )
    }
    val txt = SparkSchemaUtil(optionsFactory.dbTimezone).internalRowToText(schema, t, fieldDelimiter)
    //logger.info(s"${(System.currentTimeMillis() % 1000).toString} write numFields=${t.numFields}, row(${rowCount.get()})='${txt}'")
    output.write(txt + "\n")
    output.flush()
    rowCount.incrementAndGet()
  }

  override def commit(): WriterCommitMessage = {
    if (output != null) {
      output.flush()
      output.close()
      output = null
      // Thread.sleep(10)
    }
    // Files.deleteIfExists(pipeFile)
    val rnd = new scala.util.Random
    dbConnection.setAutoCommit(true)
    while (!Thread.currentThread().isInterrupted && (GPClient.executeStatement(dbConnection,
      s"update gp_spark_instance set dt_end = now(), row_count = ${rowCount.get().toString} " +
        s"where query_id = '${writeUUID}' and part_id = ${partitionId}") == 0)) { // and status is not null
      try {
        dbConnection.commit()
      } catch {
        case _ : Throwable =>
      }
      Thread.sleep(rnd.nextInt( (100 - 10) + 1 ))
    }
    try {
      dbConnection.commit()
    } catch {
      case _ : Throwable =>
    }
    if (gpfdistProcess != null) {
      while (!Thread.currentThread().isInterrupted && (countActiveParts > 0))
        Thread.sleep(rnd.nextInt( (100 - 10) + 1 ))
      gpfdistProcess.destroy()
      gpfdistProcess = null
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl}, write rowCount=${rowCount.get().toString}")
      // Files.deleteIfExists(pipeFile)
      try {
        ExternalProcessUtil.deleteRecursively(tempDir)
      } catch {
        case _: Throwable =>
      }
    }
    dbConnection.close()
    GreenplumWriterCommitMessage(writeUUID, instanceId, gpfdistUrl, partitionId, taskId, epochId, rowCount.get())
  }

  override def abort(): Unit = {
    if (output != null) {
      output.close()
    }
    // Files.deleteIfExists(pipeFile)
    dbConnection.setAutoCommit(true)
    GPClient.executeStatement(dbConnection, s"update gp_spark_instance set dt_end = now(), row_count = ${rowCount.get().toString} " +
      s"where  query_id = '${writeUUID}' and part_id = ${partitionId}")
    try {
      dbConnection.commit()
    } catch {
      case _ : Throwable =>
    }
    dbConnection.close()
    if (gpfdistProcess != null) {
      gpfdistProcess.destroy()
      gpfdistProcess = null
      logger.info(s"${(System.currentTimeMillis() % 1000).toString} Destroyed ${gpfdistUrl}, write rowCount=${rowCount.get().toString}")
      // Files.deleteIfExists(pipeFile)
      try {
        ExternalProcessUtil.deleteRecursively(tempDir)
      } catch {
        case _: Throwable =>
      }
    }
  }
}

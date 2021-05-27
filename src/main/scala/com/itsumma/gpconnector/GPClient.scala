package com.itsumma.gpconnector

import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory

import java.nio.file.{Path, Paths}
import java.sql.{Connection, DriverManager}
import scala.language.postfixOps
import scala.util.Try

case object GPClient {

  import java.net.{InetAddress, InetSocketAddress, ServerSocket}
  import java.text.MessageFormat
  import java.util.UUID

  def getConn(optionsFactory: GPOptionsFactory) : Connection = {
    var conn = DriverManager.getConnection(optionsFactory.url, optionsFactory.user, optionsFactory.password)
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    conn
  }

  def getUniqueFileName(directory: String, prefix: String, extension: Option[String] = None): Path = {
    val fileName = MessageFormat.format("{0}-{1}", prefix, UUID.randomUUID)
    var fPath = Paths.get(directory, fileName)
    extension match {
      case Some(ext) => fPath = fPath.resolveSibling(fPath.getFileName + "." + ext.trim)
      case None =>
    }
    fPath
  }

  def checkGpfdistIsUp(localIp: String, port: Int): Boolean = using(new ServerSocket) {
    serverSocket =>
      try {
        //serverSocket.setReuseAddress(false) // this required only on OSX
        serverSocket.bind(new InetSocketAddress(InetAddress.getByName(localIp), port), 1)
        false
      } catch {
        case _: Exception =>
          true
      }
  }

  def executeStatement(conn: Connection, sql: String): Int = {
    using(conn.createStatement()) {
      statement =>
        statement.executeUpdate(sql)
    }
  }

  private def using[A, B <: {
    def close(): Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  def waitNRowsAndExecute(conn: Connection, sqlSelect: String, sqlExec: String,
                      desiredRows: Int, deadlineSec: Long): Int = {
    val start = System.nanoTime()
    var elapsedTime: Long = 0
    var nRowsAvailable: Int = 0
    using(conn.prepareStatement(sqlSelect)) {
      statement => {
        while (nRowsAvailable < desiredRows && elapsedTime < deadlineSec && !Thread.currentThread().isInterrupted) {
          using(statement.executeQuery()) {
            rs => {
              while (rs.next())
                nRowsAvailable = rs.getInt(1)
            }
          }
          elapsedTime = (System.nanoTime() - start) / 1000000000L //math.pow(10, 9).toLong
          if (nRowsAvailable < desiredRows && elapsedTime < deadlineSec && !Thread.currentThread().isInterrupted)
            Thread.sleep(100)
        }
      }
    }
    if (elapsedTime >= deadlineSec || nRowsAvailable < desiredRows) {
      println(s"----------- Aborted $sqlSelect | nRowsAvailable=$nRowsAvailable of $desiredRows, elapsedTime=$elapsedTime of $deadlineSec -----------")
      return nRowsAvailable
      throw new RuntimeException(s"----------- Aborted $sqlSelect | nRowsAvailable=$nRowsAvailable of $desiredRows, elapsedTime=$elapsedTime of $deadlineSec -----------")
    }
    if (sqlExec != null && sqlExec.nonEmpty)
      GPClient.using(conn.createStatement()) {
        statement => statement.executeUpdate(sqlExec)
      }
    println(s"----------- Success $sqlSelect | nRowsAvailable=$nRowsAvailable of $desiredRows, elapsedTime=$elapsedTime of $deadlineSec -----------")
    nRowsAvailable
  }

  def tableExists(conn: Connection, table: String): Boolean = {
    val query = s"SELECT * FROM $table WHERE 1=0"
    Try {
      val statement = conn.prepareStatement(query)
      try {
        statement.executeQuery()
      } finally {
        statement.close()
        if (!conn.getAutoCommit) {
          conn.clearWarnings()
          conn.rollback()
        }
      }
    }.isSuccess
  }
}

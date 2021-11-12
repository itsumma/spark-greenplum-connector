package org.apache.spark.sql.itsumma.gpconnector

import java.sql.Connection

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import GPOptionsFactory.{queryNSegments, using}
import org.apache.spark.util.Utils

case object GPOptionsFactory {

  def queryNSegments(conn: Connection): Int = {
    var nSeg: Int = 0
    using(conn.createStatement()) {
      statement =>
        using(statement.executeQuery("select count(1) from gp_toolkit.gp_workfile_usage_per_segment where segid >= 0")) {
          rs => {
            while (rs.next())
              nSeg = rs.getInt(1)
          }
        }
    }
    nSeg
  }

  private def using[A, B <: {
    def close() : Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }
}

case class GPOptionsFactory(params: Map[String, String]) {
  require(params.contains("url"))
  require(params.contains("dbtable"))
  private var nSeg: Int = params.getOrElse("partitions","0").toInt
  val gpfdistTimeout: Long = Utils.timeStringAsMs(params.getOrElse("server.timeout", "5m"))
  val tableOrQuery: String = params("dbtable")
  val driver: String = "org.postgresql.Driver"
  val partitionColumn: String = params.getOrElse("partitionColumn","") // gp_segment_id is the default
  val dbTimezone: String = params.getOrElse("dbTimezone", java.time.ZoneId.systemDefault.toString)
  val url: String = params("url")
  val user: String = params.getOrElse("user", null)
  val password: String = params.getOrElse("password", null)
  val serverPath: String = params.getOrElse("server.path", "/home/hadoop/hadoop/bin/gpfdist")
  val serverPort: Int = params.getOrElse("server.port", "0").toInt
  val truncate: Boolean = params.getOrElse("truncate", "false").toBoolean
  val distributedBy: String = params.getOrElse("distributedBy", null)
  val dbSchema: String = params.getOrElse("dbschema", null)
  val bufferSize: Int = params.getOrElse("buffer.size", "20000").toInt

  def getJDBCOptions(tableName: String = tableOrQuery): JDBCOptions = {
    val excludeParams = Map("driver" -> "", "dbtable" -> "", "partitions" -> "",
      "dbschema" -> "",
      "partitionColumn" -> "",
      "dbTimezone" -> "",
      "truncate" -> "",
      "distributedBy" -> "",
      "iteratorOptimization" -> "",
      "server.path" -> "",
      "server.port" -> "",
      "server.useHostname" -> "",
      "server.hostEnv" -> "",
      "server.nic" -> "",
      "server.timeout" -> "",
      "pool.maxSize" -> "",
      "pool.timeoutMs" -> "",
      "pool.minIdle" -> "",
      "buffer.size"-> ""
    )
    new JDBCOptions(CaseInsensitiveMap(params
      -- excludeParams.keySet
      + ("driver" -> driver,
      "dbtable" -> tableName)))
  }

  def numSegments: Int = {
    if (nSeg > 0)
      return nSeg
    using(JdbcUtils.createConnectionFactory(getJDBCOptions("--"))()) {
      conn => {
        nSeg = queryNSegments(conn)
      }
    }
    nSeg
  }

}

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
  val gpfdistTimeout: Long = Utils.timeStringAsMs(params.getOrElse("server.timeout", "5m"))
  val tableOrQuery: String = params("dbtable")
  val driver: String = "org.postgresql.Driver"
  val partitionColumn: String = params.getOrElse("partitionColumn","") // gp_segment_id is the default
  val dbTimezone: String = params.getOrElse("dbTimezone", java.time.ZoneId.systemDefault.toString)
  private var nSeg: Int = params.getOrElse("partitions","0").toInt

  def getJDBCOptions(tableName: String = tableOrQuery): JDBCOptions = {
    val excludeParams = Map("driver" -> "", "dbtable" -> "", "partitions" -> "", "partitionColumn"->"", "dbTimezone"->"")
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

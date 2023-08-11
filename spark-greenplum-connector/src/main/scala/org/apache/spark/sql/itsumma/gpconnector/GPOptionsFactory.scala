package org.apache.spark.sql.itsumma.gpconnector

import com.typesafe.scalalogging.Logger
//import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.Utils
import org.slf4j.LoggerFactory

case class GPTarget(tableOrQuery: String) {

  val (cleanDbTableName: String, tableSchema: String, isQuery: Boolean, isQuated: Boolean) = guessTargetFlags()

  private def guessTargetFlags(): (String, String, Boolean, Boolean) = {
    var cleanDbTableName: String = ""
    var tableSchema: String = ""
    var isQuery: Boolean = false
    var isQuated: Boolean = false
    val queryPattern = """\A\s*(?i)(select\s+\S.*)""".r
    val tablePattern = """\A(?i)(\w+[.])?(\w+)\z""".r
    val quotedTablePattern = """\A(?i)(["][^"]+["][.])?(["][^"]+["])\z""".r
    tableOrQuery match {
      case queryPattern(_) => isQuery = true
      case tablePattern(prefix, tblName) => cleanDbTableName = tblName
        if (prefix != null && prefix.nonEmpty) {
          tableSchema = prefix.dropRight(1)
        }
      case quotedTablePattern(prefix, tblName) => cleanDbTableName = tblName
        if (prefix != null && prefix.nonEmpty) {
          tableSchema = prefix.dropRight(1)
        }
        isQuated = true
      case _ =>
    }
    (cleanDbTableName, tableSchema, isQuery, isQuated)
  }

  /**
   * If this instance provides DB schema, then use it as is, otherwise substitutes defaultSchema prefix
   * @param defaultSchema default DB schema (object search path) of the current user session
   * @return DBschema.tableName
   */
  def getCanonicalName(defaultSchema: String): String = {
    if (tableSchema.nonEmpty) {
      return tableOrQuery
    } else if (cleanDbTableName.nonEmpty) {
      if (defaultSchema != null && defaultSchema.nonEmpty)
        return s"${defaultSchema}.${cleanDbTableName}"
      else return cleanDbTableName
    }
    ""
  }

  /**
   * If this instance provides DB schema, then use it as is, otherwise substitutes defaultSchema prefix
   *
   * @param defaultSchema default DB schema (object search path) of the current user session
   * @return DBschema
   */
  def getTableSchema(defaultSchema: String): String = {
    if (tableSchema.nonEmpty) {
      return tableSchema
    } else if (cleanDbTableName.nonEmpty) {
      return if (defaultSchema != null) defaultSchema else ""
    }
    ""
  }
}

/**
 *
 * @param params Map[String, String] - Note: in Spark 2.x.x DataSourceOptions option names
 *     are implicitly converted to the lower case
 */
case class GPOptionsFactory(params: Map[String, String]) {
  require(params.contains("url"))
  require(params.contains("dbtable") || params.contains("sqltransfer"))
  val gpfdistTimeout: Long = Utils.timeStringAsMs(params.getOrElse("server.timeout", "-1"))
  val tableOrQuery: String = params.getOrElse("dbtable", "").trim
  val driver: String = "org.postgresql.Driver"
  val dbTimezone: String = params.getOrElse("dbtimezone", java.time.ZoneId.systemDefault.toString).trim
  val url: String = params("url")
  val user: String = params.getOrElse("user", null)
  val password: String = params.getOrElse("password", null)
  val serverPort: Int = params.getOrElse("server.port", "0").toInt
  val truncate: Boolean = params.getOrElse("truncate", "false").toBoolean
  val distributedBy: String = params.getOrElse("distributedby", "").trim
  val partitionClause: String = params.getOrElse("partitionclause","").trim
  val sqlTransfer: String = params.getOrElse("sqltransfer", "").trim
  val dbSchema: String = params.getOrElse("dbschema", null)
  val bufferSize: Int = params.getOrElse("buffer.size", "20000").toInt
//  val heartbeatInterval: Long = Utils.timeStringAsMs(params.getOrElse("spark.executor.heartbeatinterval", "10s"))
//  val fetchTimeout: Long = Utils.timeStringAsMs(params.getOrElse("spark.files.fetchtimeout", "60s"))
  val networkTimeout: Long = Utils.timeStringAsMs(params.getOrElse("network.timeout",
    s"${Utils.timeStringAsMs(params.getOrElse("spark.network.timeout", "120s"))/2}ms"
  ))
  val dbMessageLogLevel: String = params.getOrElse("dbmessages", "OFF").trim
  val useTempExtTables: Boolean = params.getOrElse("tempexttables", "true").toBoolean
  val offsetStoreSql: String = params.getOrElse("offset.update", "").trim
  val offsetRestoreSql: String = params.getOrElse("offset.select", "").trim
  val undoSideEffectsSQL: String = params.getOrElse("undo.side.effects.sql", "").trim
  val readStreamAutoCommit: Boolean = params.getOrElse("stream.read.autocommit", "true").toBoolean

  /**
   * Dump passed in parameters for debug purposes
   * @return multiline String with key -> value pairs
   */
  def dumpParams(): String = {
    val ret: StringBuilder = new StringBuilder(1000)
    val excludeParams = Map("password"-> "")
    (params -- excludeParams.keySet).foreach(p => ret ++= s""""${p._1}" -> "${p._2}"\n""")
    if (ret.nonEmpty)
      ret.setLength(ret.length - 1)
    ret.toString()
  }

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  def getJDBCOptions(tableName: String = tableOrQuery): JDBCOptions = {
    val excludeParams = Map(
      "driver" -> "",
      "dbtable" -> "",
      "partitions" -> "",
      //"dbschema" -> "",
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
      "buffer.size"-> "",
      "network.timeout" -> "",
      "spark.network.timeout" -> "",
      "spark.files.fetchTimeout" -> "",
      "spark.executor.heartbeatInterval" -> "",
      "sqlTransfer" -> "",
      "dbmessages" -> "",
      "tempexttables" -> "",
      "offset.update" -> "",
      "offset.select" -> "",
      "undo.side.effects.sql" -> "",
      "applicationname" -> "", // Spark-2 case workaround
      "ApplicationName" -> ""
    )
    val excludeNullValues: Map[String, String] = params.filter{case (k, v) => v == null}
    var amendParams: Map[String, String] = (params --excludeParams.keySet --excludeNullValues.keySet) ++ Map("driver" -> driver,
          "dbtable" -> {if (tableName.nonEmpty) tableName else "-"})
    //import org.postgresql.PGProperty
    import org.apache.spark.SparkContext
    val appId: String = try {
      CaseInsensitiveMap(params).getOrElse("applicationname", SparkContext.getOrCreate.applicationId)
    } catch {
      // SparkContext can raise "SparkException: A master URL must be set in your configuration" when called in executor
      case x: Exception => ""
      //case x: SparkException => ""
    }
    if (appId != "")
      amendParams = amendParams ++ Map("ApplicationName" -> appId)
    logger.debug(s"${amendParams.mkString("\n{", "\n", "}")}")
    new JDBCOptions(CaseInsensitiveMap(amendParams))
  }
}

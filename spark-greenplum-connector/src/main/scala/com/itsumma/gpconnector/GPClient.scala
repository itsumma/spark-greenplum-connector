package com.itsumma.gpconnector

import com.itsumma.gpconnector.GPClient.makeConnProps
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, SQLWarning}
import java.util.Properties
import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Try
import scala.collection.mutable.{HashMap => MutableHashMap, Map => MutableMap}

case class GPClient(optionsFactory: GPOptionsFactory) {
  import org.apache.commons.dbcp2.BasicDataSource
  private var pool: BasicDataSource = null

  private def initPool(optionsFactory: GPOptionsFactory): Unit = this.synchronized {
    if (pool != null)
      return
    import scala.collection.JavaConverters._
    val props = makeConnProps(optionsFactory).asScala.map { case (key, value) => s"$key=$value" }.mkString("; ")
    pool = new BasicDataSource()
    pool.setUrl(optionsFactory.url)
    if (optionsFactory.user != null)
      pool.setUsername(optionsFactory.user)
    if (optionsFactory.password != null)
      pool.setPassword(optionsFactory.password)
    pool.setMaxWaitMillis(optionsFactory.networkTimeout)
    pool.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    pool.setAutoCommitOnReturn(false)
    pool.setDefaultAutoCommit(false)
    pool.setRollbackOnReturn(true)
    pool.setConnectionProperties(props)
  }

  def getConnection(): Connection = {
    initPool(optionsFactory)
    val conn = pool.getConnection
    conn
  }

}

case object GPClient {

  import java.net.{InetAddress, InetSocketAddress, ServerSocket}
//  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val logger = Logger("com.itsumma.gpconnector.GPClient")

  private def makeConnProps(optionsFactory: GPOptionsFactory): Properties = {
    val props = optionsFactory.getJDBCOptions().asProperties
    props.remove("url")
    if ((optionsFactory.dbSchema != null) && optionsFactory.dbSchema.nonEmpty) {
      var searchPathApply: String = optionsFactory.dbSchema
      val schemas = searchPathApply.split("[, ]+]")
      if (!schemas.contains("public"))
        searchPathApply += ",public"
      props.setProperty("currentSchema", searchPathApply)
    }
    props
  }

  def getConn(optionsFactory: GPOptionsFactory) : Connection = {
    val conn = DriverManager.getConnection(optionsFactory.url, makeConnProps(optionsFactory))
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    conn
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

  private def logDbWarnings(pWarning: SQLWarning, msgLogLevel: String): Unit = {
    var warning: SQLWarning = pWarning
    while (warning != null)
    {
      val msg = warning.getMessage
      val state = warning.getSQLState
      msgLogLevel match {
        case "DEBUG" => logger.debug(s"${state}: ${msg}")
        case "INFO" => logger.info(s"${state}: ${msg}")
        case "WARN" => logger.warn(s"${state}: ${msg}")
        case _ =>
      }
      warning = warning.getNextWarning
    }
  }

  def executeStatement(conn: Connection, sql: String, msgLogLevel: String = "OFF"): Int = {
    using(conn.createStatement()) {
      statement => {
        val ret = statement.executeUpdate(sql)
        if (msgLogLevel != "OFF") logDbWarnings(statement.getWarnings, msgLogLevel)
        ret
      }
    }
  }

  def using[A, B <: {
    def close(): Unit
  }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }

  private def createDbSchemeIfNotExists(conn: Connection, schemaNameParam: String): Boolean = {
    var schemaExists: Boolean = false
    val schemaName: String = if (!schemaNameParam.contains("\"")) schemaNameParam.toLowerCase() else schemaNameParam
    GPClient.using(conn.prepareStatement("select count(1) cnt " +
      "from information_schema.schemata where schema_name = ?")) {
      st => {
        st.setString(1, schemaName.replaceAll("\"", ""))
        using(st.executeQuery()) {
          rs => {
            while (rs.next()) {
              schemaExists = rs.getInt(1)==1
            }
          }
        }
      }
    }
    if (!schemaExists) {
      logger.info(s"Will create DB schema ${schemaName} because it doesn't exists..")
      GPClient.using(conn.createStatement()) {
        st => st.executeUpdate(s"create schema ${schemaName}")
      }
      if (!conn.getAutoCommit)
        conn.commit()
    } else {
      logger.info(s"DB schema ${schemaName} exists.")
    }
    !schemaExists
  }

  private def getDbObjSearchPath(conn: Connection): String = {
    var ret: String = ""
    GPClient.using(conn.createStatement()) {
      st => {
        using(st.executeQuery(s"show search_path")) {
          rs => if (rs.next()) ret = rs.getString(1).replaceAll("\\s+", "")
        }
      }
    }
    ret
  }

  /**
   *Validates & activates passed in database objects search path, which can contain single database schema name,
   * or comma separated list of database schema names. Creates primary (first in the list) database schema
   * if it doesn't exists. Returns the primary database schema name, or null if an input searchPath is null or empty.
   * @param conn Greenplum database JDBC connection
   * @param searchPath Value passed in via dbschema connector option. Can be null.
   * @return name of the database schema where destination table should be created (for write operations)
   */
  def checkDbObjSearchPath(conn: Connection, searchPath: String): String = {
    var ret: String = null
    if ((searchPath != null) && searchPath.nonEmpty) {
      var searchPathApply: String = searchPath.replaceAll("\\s+", "")
      val schemas = searchPath.split("[,]+]")
      if (schemas.nonEmpty) {
        ret = schemas(0)
        if (!schemas.contains("public"))
          searchPathApply += ",public"
        createDbSchemeIfNotExists(conn, schemas(0))
        val currSearchPath = getDbObjSearchPath(conn)
        if (currSearchPath != searchPathApply) {
          GPClient.using(conn.createStatement()) {
            st => st.executeUpdate(s"set search_path to ${searchPathApply}")
          }
        }
      }
    }
    logger.info(s"Current GP name space search path: ${getDbObjSearchPath(conn)}")
    ret
  }

  /**
   * Returns a map of node server name -> set of GP primary segment IDs it holds.
   * <p>Excludes master node.
   * <p>Also only segments in the UP status returned.
   * @param conn Greenplum connection
   * @return
   */
  def nodeNamesWithSegments(conn: Connection): Map[String, Set[String]] = {
    val ret: MutableMap[String, Set[String]] = MutableMap[String, Set[String]]()
    val select = """select  hostname, array_agg(content::text) segment_ids
                   |from    gp_segment_configuration
                   |where   content >= 0
                   |        and role = 'p'
                   |        and status = 'u'
                   |        /*and mode = 's'*/
                   |group by hostname""".stripMargin
    using(conn.prepareStatement(select)) {
      statement => {
       using(statement.executeQuery()) {
         rs => {
           while (rs.next()) {
             ret.put(rs.getString(1), rs.getArray(2).getArray().asInstanceOf[Array[String]].toSet)
           }
         }
       }
      }
    }
    ret.toMap
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

  def versionCompare(a: String, b: String): Int = {
    def nums(s: String) = s.split("\\.").map(_.toInt)
    val pairs = nums(a).zipAll(nums(b), 0, 0).toList
    def go(ps: List[(Int, Int)]): Int = ps match {
      case Nil => 0
      case (a, b) :: t =>
        if (a > b) 1 else if (a < b) -1 else go(t)
    }
    go(pairs)
  }

  def queryGPVersion(conn: Connection): String = {
    var ret = "0.0.0"
    using(conn.createStatement()) {
      st => {
        using(st.executeQuery("select substring( version() from '.*Greenplum Database ([0-9.]+).*')")) {
          rs => {
            if (rs.next())
              ret = rs.getString(1)
          }
        }
      }
    }
    ret
  }

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

  /**
   * Extracts distribution policy clause from the DML of given Greenplum DB table.
   * @param conn DB connection
   * @param tblSchema schema (namespace) of the table
   * @param tblName table name without namespace
   * @param tblKind table storage kind: h=heap, a=append-optimized, c=column-oriented, v=virtual, x=external table
   * @return distribution clause of DML
   */
  def getTableDistributionPolicy(conn: Connection, tblSchema: String, tblName: String, tblKind: String = "h"): String = {
    var ret: String = ""
    val schemas = if (tblSchema == null || tblSchema.isEmpty)
        """"$user",public""".split(",")
        else tblSchema.split(",")
    val sqlArr = conn.createArrayOf("varchar", schemas.toArray)
    val query: String =
      s"""select pg_get_table_distributedby(c.oid) distributed_by
         |from   pg_class c
         |       join pg_namespace n on (c.relnamespace = n.oid)
         |where  n.nspname = any (?)
         |       and c.relname = ?
         |       and c.relstorage = ?
         |order by n.nspname, c.relname limit 1""".stripMargin
    using(conn.prepareStatement(query)) {
      statement => {
        statement.setArray(1, sqlArr)
        statement.setString(2, tblName)
        statement.setString(3, tblKind)
        using(statement.executeQuery()) {
          rs => {
            while (rs.next())
              ret = rs.getString(1)
          }
        }
      }
    }
    ret
  }

  /**
   * Recognize & normalize table DML distribution clause.
   * Also outputs a column list suitable for primary key creation, if applicable.
   * <p> An empty strings returned in the case of invalid input.
   * <p>Allowed input examples:
   * <p>replicated
   * <p>randomly
   * <p>distributed  randomly
   * <p>distributed by (col1, col2)
   * <p>col1, col2
   * @param src
   * @return tuple: (normalized distribution clause, cleaned col name list or empty string in the case
   *         of <b>replicated</b> or <b>randomly</b> policy)
   */
  def formatDistributedByClause(src: String): (String, String) = {
    if (src == null || src.isEmpty)
      return ("", "")
    val replicatedPattern = """\A\s*(?i)(distributed\s+)?(replicated)\s*\z""".r
    val randomlyPattern = """\A\s*(?i)(distributed\s+)?(randomly)\s*\z""".r
    val byColListPattern = """\A\s*(?i)(distributed\s+by\s*\()?([^)]+)(\))?\s*\z""".r
    val colPattern = """\A(?i)\s*(\w+)(\s.*)?\z""".r
    val quotedColPattern = """\A(?i)\s*(["][^"]+["])(\s.*)?\z""".r
    src match {
      case replicatedPattern(_, arg) => ("distributed replicated", "")
      case randomlyPattern(_, arg) => ("distributed randomly", "")
      case byColListPattern(_, arg, _) => {
        val colNames = arg.trim.split(",").map {
          case colPattern(colName, _) => colName
          case quotedColPattern(colName, _) => colName
          case _ => arg.trim
        }.mkString(",")
        (s"distributed by ($arg)", colNames)
      }
      case _ => ("", "")
    }
  }
}

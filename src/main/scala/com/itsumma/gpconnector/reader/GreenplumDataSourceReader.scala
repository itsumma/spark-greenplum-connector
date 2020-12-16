package com.itsumma.gpconnector.reader

import java.sql.Connection
import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.itsumma.gpconnector.GPClient
import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.partitioning.{Distribution, Partitioning}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, HashMap => MutableHashMap}

// scalafix:off DisableSyntax.null
// scalastyle:off null

@DeveloperApi
case class GPInputPartitionReaderReadyEvent(
                                             queryId: String,
                                             instanceId: String,
                                             //hostName: String,
                                             executorId: String,
                                             numPartsDetected: Int,
                                             gpfdistUrl: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

@DeveloperApi
case class GPInputPartitionReaderCloseEvent(
                                             queryId: String,
                                             instanceId: String,
                                             //hostName: String,
                                             executorId: String) extends SparkListenerEvent {
  override def logEvent: Boolean = false
}

class GreenplumDataSourceReader(optionsFactory: GPOptionsFactory)
  extends DataSourceReader
    with SupportsReportPartitioning
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
{
  val tableOrQuery: String = optionsFactory.tableOrQuery
  val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
  val partUrls: MutableHashMap[String, String] = MutableHashMap()
  val dbTableName: String = if (tableOrQuery.contains(" ")) "" else tableOrQuery
  val externalTableName: String = s"ext$queryId${dbTableName}"
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  //val sparkSession = SparkSession.builder().getOrCreate()
  private val sc = SparkContext.getOrCreate
  private val fieldDelimiter = "|"
  private val dbConnection: Connection = SparkSchemaUtil.getConn(optionsFactory)
  private val notificationsTimeout: Int = sc.getConf.getOption("spark.batch.duration.seconds").orElse(Option("120")).get.toInt * 1000
  private val numParts: Int = optionsFactory.numSegments //math.max(sc.defaultMinPartitions, optionsFactory.numSegments)
  var whereClause = ""
  var pushedDownFilters: Array[Filter] = new Array[Filter](0)
  private var schema = SparkSchemaUtil.getGreenplumTableSchema(optionsFactory, tableOrQuery)
  //private val partIds = ListBuffer[String]() //new util.ArrayList[String]()
  private val stage: AtomicInteger = new AtomicInteger()
  private val partUrlsCount = new AtomicInteger()
  private val numPartsDetected = new AtomicInteger()
  private val numPartsClosed = new AtomicInteger()

  sc.setJobGroup(queryId, "greenplum connector reader jobs", true)
  //logger.warn(s"----master=${sc.master} applicationId=${sc.applicationId} appName=${sc.appName}---")

  private val partListener: SparkListener = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case partitionReaderReadyEvent: GPInputPartitionReaderReadyEvent => {
        if (partitionReaderReadyEvent.queryId.equals(queryId)) {
          logger.info(s"-------${partitionReaderReadyEvent.toString}-------")
          partUrls.put(partitionReaderReadyEvent.instanceId, partitionReaderReadyEvent.gpfdistUrl)
          partUrlsCount.set(partUrls.size)
          numPartsDetected.set(partitionReaderReadyEvent.numPartsDetected)
        }
      }
      case partitionCloseEvent: GPInputPartitionReaderCloseEvent => {
        if (partitionCloseEvent.queryId.equals(queryId))
          numPartsClosed.getAndIncrement()
      }
      case _ =>
    }
  }

  private val sqlThread: Thread = new Thread(new SqlThread(dbConnection), s"gpfdist-read$queryId")
  sqlThread.setDaemon(true)

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    if (stage.get() != 0)
      throw new UnsupportedOperationException(
        s"Attempt to re-run previously executed query"
      )
    sc.addSparkListener(partListener)
    sqlThread.start()
    var inputPartitions = ListBuffer[InputPartition[InternalRow]]()
    for (i <- 0 until numParts) {
      val partInstanceId = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
      val inpPart = new GreenplumInputPartition(optionsFactory, queryId, partInstanceId, schema, i)
      //partIds += partInstanceId
      inputPartitions += inpPart
    }
    //TODO: detect and drop GP external tables possibly left from previous failed operations
    stage.incrementAndGet()
    inputPartitions
  }

  override def pruneColumns(structType: StructType): Unit = {
    schema = structType
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val tuple3 = SparkSchemaUtil(optionsFactory.dbTimezone).pushFilters(filters)
    whereClause = tuple3._1
    pushedDownFilters = tuple3._3
    tuple3._2
  }

  override def pushedFilters: Array[Filter] = pushedDownFilters

  private class SqlThread(private val conn: Connection) extends Runnable {
    def run {
      val start = System.currentTimeMillis()
      try {
        while (!Thread.currentThread().isInterrupted
          && partUrlsCount.get() < (numPartsDetected.get() match {case 0 => numParts case otherValue => otherValue })) {
          if (System.currentTimeMillis() - start > notificationsTimeout)
            throw new RuntimeException("GP reader initialization timeout expired")
          Thread.sleep(100)
        }
        while (stage.get() == 0 && !Thread.currentThread().isInterrupted) {
          Thread.sleep(10)
        }
        stage.incrementAndGet()
        conn.setAutoCommit(true)
        val locationClause = new StringBuilder("")
        var i = 0
        partUrls.foreach {
          case (_, gpfdistUrl) => {
            if (i > 0) locationClause.append(", ")
            locationClause.append(s"'$gpfdistUrl'")
            i += 1
          }
        }
        if (GPClient.tableExists(conn, externalTableName))
          GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
        val columnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, withTypes = true)
        GPClient.executeStatement(conn, s"CREATE WRITABLE EXTERNAL TABLE $externalTableName" +
          //s" (LIKE ${table})" +
          s"($columnsClause)" +
          s" LOCATION (${locationClause.toString()})" +
          s" FORMAT 'TEXT' (DELIMITER '$fieldDelimiter' " +
          s"    NULL 'NULL' " +
          //s"    NEWLINE 'LF' " + // Specifying NEWLINE is not supported for GP writable external tables
          s"  ) " +
          s" ENCODING 'UTF8'"
          //+ s" DISTRIBUTED BY (exp_id)"
        )
        stage.incrementAndGet()
        //TODO: consider adding DISTRIBUTED clause here, if appropriate
        var insertSelectSql = s"insert into $externalTableName "
        val colList = SparkSchemaUtil.getGreenplumTableColumns(schema, withTypes = false)
        if (dbTableName.length > 0) {
          insertSelectSql += s"select $colList from $dbTableName"
        } else {
          insertSelectSql += s"select $colList from ($tableOrQuery) sqr"
        }
        if (whereClause.length > 0)
          insertSelectSql += s" where $whereClause"
        logger.info(s"SQL: $insertSelectSql")
        try {
          GPClient.executeStatement(conn, insertSelectSql)
        } catch {
          case e: Exception => {
            if (numPartsClosed.get() == 0) throw e
          }
        }
        if (!conn.getAutoCommit)
          conn.commit()
      } finally {
        sc.removeSparkListener(partListener)
        if (stage.get() >= 3) {
          try {
            GPClient.executeStatement(conn, s"DROP EXTERNAL TABLE $externalTableName")
          } catch {
            case e: Exception => logger.error(s"${e.getMessage}")
          }
        }
        conn.close()
      }
    }
  }

  override def outputPartitioning(): Partitioning = new Partitioning {
    override def numPartitions(): Int = numParts

    override def satisfy(distribution: Distribution): Boolean = false
  }
}

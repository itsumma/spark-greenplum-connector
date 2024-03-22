import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.net.URI
import java.time._
import scala.util.Random

/**
 * First of all substitute your Grennplam connection parameters here:
 */
val dbUrl = "jdbc:postgresql://greenplum-master-host:5432/db-name"
val dbUser = "your_gp_db_user"
val dbPassword = "gp_user_password"

val rowsPerBatch: Int = 2000
val payloadSize: Int = 100000

val cpDirName = "/tmp/rate-mb2db/checkpoint"

def cleanFS(sc: SparkContext, fsPath: String) = {
  val fs = org.apache.hadoop.fs.FileSystem.get(new URI(fsPath), sc.hadoopConfiguration)
  fs.delete(new Path(fsPath), true)
}

cleanFS(sc, cpDirName)


/**
* This PL/pgSQL script process records passed over the pipline to the Greenplum receiver side and prints some statistics.
*/
val aggregator = """do $$
declare
  v_cnt int;
begin
  drop table if exists rate2db_target;
  create table rate2db_target
  with ( appendoptimized=true, blocksize=2097152 )
  as select  *
  from    <ext_table> us
  DISTRIBUTED BY (id)
  ;
  GET DIAGNOSTICS v_cnt := ROW_COUNT;
  raise notice 'Read % rows in %s', v_cnt, extract(epoch from clock_timestamp()-now());
end
$$"""

val appId: String = SparkContext.getOrCreate.applicationId

com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
println(s"Connector version: ${com.itsumma.gpconnector.ItsMiscUDFs.getVersion}")

var stream = spark.readStream.format("rate-micro-batch").
  //option("numPartitions", 2).
  option("rowsPerBatch", rowsPerBatch).
  option("startTimestamp", OffsetDateTime.now().toInstant.toEpochMilli).
  option("advanceMillisPerBatch", 200).
  load().
  withColumn("spark_ts", com.itsumma.gpconnector.ItsMiscUDFs.getRowTimestamp()).
  withColumn("payload", lit(Random.alphanumeric.take(payloadSize).mkString(""))).
  selectExpr("value as id", "now() as gen_ts", "spark_ts", "(cast(spark_ts as double) - cast(now() as double)) as spark_delay").
writeStream.
  format("its-greenplum").
  option("url", dbUrl).
  option("user", dbUser).
  option("password", dbPassword).
  //option("ApplicationName",s"test-rate2db").
  option("sqlTransfer", aggregator).
  option("dbmessages", "WARN").
  option("checkpointLocation", cpDirName).
  option("asyncProgressTrackingEnabled", true).
  option("asyncProgressTrackingCheckpointIntervalMs", 120000).
  option("action.name", "console").
  //trigger(Trigger.ProcessingTime("1 seconds")).
start()


//stream.stop()

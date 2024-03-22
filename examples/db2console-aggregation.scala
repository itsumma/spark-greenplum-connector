import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import java.net.URI

/**
* This sample application intended to be run via spark-shell.
* We assume that you start spark-shell from the folder where this scrip and log4j.properties file reside:
*/
//spark-shell --files "./log4j.properties" --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.ui.showConsoleProgress=false"
/**
* On the spark-shell prompt do:
*  scala> :load db2console-aggregation.scala
*/

/**
* First of all substitute your Grennplam connection parameters here:
*/
val dbUrl = "jdbc:postgresql://greenplum-master-host:5432/db-name"
val dbUser = "your_gp_db_user"
val dbPassword = "gp_user_password"

val cpDirName = "/tmp/db2console-aggregation/checkpoint"
val secondsPerBatch: Double = 0.0
val rowsPerBatch: Int = 2000
val payloadSize: Int = 100

val offsetScale: Double = if (secondsPerBatch > 0.1) 1.0 / secondsPerBatch else 10.0

def cleanFS(sc: SparkContext, fsPath: String) = {
  val fs = org.apache.hadoop.fs.FileSystem.get(new URI(fsPath), sc.hadoopConfiguration)
  fs.delete(new Path(fsPath), true)
}
/**
* This function call will delete Spark checkpoint directory of our application on every run.
* If you want the stream to continue from the last complete offset after application restart,
* comment out this line:
*/
cleanFS(sc, cpDirName)

/**
* This PL/pgSQL script acts as records generator on the Greenplum DB sender side.
* Connector will substitute angle-bracket template arguments with actual values there.
*/
val generator = s"""do $$$$
declare
  v_start_offset bigint := ('<start_offset_json>'::json ->> 'offset_ts')::bigint;
  v_end_ofsset bigint := ('<end_offset_json>'::json ->> 'offset_ts')::bigint;
  v_batch_size bigint := v_end_ofsset - v_start_offset;
  v_sleep float := 0.0;
  v_counter bigint := ${rowsPerBatch};
  v_rec_per_offset bigint := 0;
  v_id bigint := 0;
  v_dur float := ${secondsPerBatch};
begin
  if v_batch_size = 0 then
    return;
  end if;
  v_rec_per_offset := v_counter / v_batch_size;
  if v_rec_per_offset = 0 then
    v_rec_per_offset := 1;
  end if;
  v_id := v_start_offset * v_rec_per_offset;
  if v_counter > 0 and v_counter <= 100 and v_dur > 0.0 then
    v_sleep := v_dur / v_counter::float;
  end if;
  insert into <ext_table>
  select  <select_colList>
  from  (
        select  seq_n::bigint + v_id id,
                seq_n::bigint,
                clock_timestamp() gen_ts,
                (seq_n::bigint + v_id) / v_rec_per_offset + 1 offset_id,
                repeat('0', ${payloadSize})::text payload,
                case when v_sleep >= 0.01 then pg_sleep(v_sleep) else null end sleep
        from    generate_series(1, v_counter) as seq_n(n)
        ) a;
  v_sleep := v_dur - extract(epoch from clock_timestamp()-now())::float;
  if v_sleep >= 0.01 then
    perform pg_sleep(v_sleep);
  end if;
  raise notice '% records generated for offsets % - %', v_counter, v_start_offset + 1, v_end_ofsset + 1;
end
$$$$"""

/**
 * Our connector provides some useful UDFs as bonus.
 * The following line makes them available for Spark SQL expressions:
 */
com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
println(s"Connector version: ${com.itsumma.gpconnector.ItsMiscUDFs.getVersion}")

var stream = (spark.readStream.format("its-greenplum").option("url", dbUrl).
  option("user", dbUser).
  option("password", dbPassword).
  /**
  * dbtable option here specifies Spark DataFrame columns name/type and must correspond to the generator script output.
  */
  option("dbtable","select 1::bigint id, 1::int seq_n, clock_timestamp() gen_ts, 1::bigint offset_id, '0'::text payload").
  option("sqlTransfer", generator).
  option("offset.select", s"select json_build_object('offset_ts', (extract(epoch from pg_catalog.clock_timestamp()) * ${offsetScale})::bigint)::text").
  option("dbmessages", "WARN").
  load().
  /**
   * getRowTimestamp() UDF returns idividual row timestamp containing a moment when this row come to Spark.
  */
  withColumn("spark_ts", com.itsumma.gpconnector.ItsMiscUDFs.getRowTimestamp()).
  /**
   * getBatchId() UDF returns current micro-batch number, also know as 'epoch' in Spark
  */
  withColumn("batch_id", com.itsumma.gpconnector.ItsMiscUDFs.getBatchId()).
  withWatermark("spark_ts", "10000 milliseconds").
  //groupBy(window(col("spark_ts"), "1000 milliseconds"), col("batch_id")).
  groupBy(col("batch_id")).
  agg(count(col("batch_id")), max(col("seq_n"))).
writeStream.
  format("console").
  outputMode("update").
  option("checkpointLocation", cpDirName).
  option("truncate", "false").
//   option("asyncProgressTrackingEnabled", true).
//   option("asyncProgressTrackingCheckpointIntervalMs", 20000).
  start())

/**
* In the production code you will probably need to lock application here and wait intil stream termination:
*/
//val ret = stream.awaitTermination()

/**
* When run in the spark-shell, we actually don't need a stream.awaitTermination() to let a stream to go.
*  One can call stream.stop() to terminate it.
*/
//stream.stop()


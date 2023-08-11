import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import java.net.URI

/**
* This sample application intended to be run via spark-shell.
* We assume that you start spark-shell from the folder where this scrip and log4j.properties file reside:
*/
//park-shell --files "./log4j.properties" --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" --conf "spark.ui.showConsoleProgress=false"
/**
* On the park-shell prompt do:
*  scala> :load streaming-example.scala
*/

/**
* First of all substitute your Grennplam connection parameters here:
*/
val dbUrl = "jdbc:postgresql://gp-master-host:5432/database"
val dbUser = "database_user"
val dbPassword = "yourpassword"

/**
 * We will run two instances of connector using the Structured Streaming micro-batch mode.
 * <p>The first instance will continiosuly generate data records and send it over Spark pipeline to the second instance.
 * <p>The second instance will receive these data and print some statistics to the console once per micro-batch.
 * <p><b>cpDirName</b> variable defines HDFS path where Spark will store checkpoint information and use it to
 * garantee an Exactlu-Once semantics over entire processing pipeline.
 * Look for more info about it in the Spark Structired Streaming Guide.
 * <p><b>secondsPerBatch</b> variable defines micro-batches duration in seconds. Possible values are from 0.1 and up.
 * Spark starts new micro-batch after the previous micro-batch finished and it received a new <b>offset</b>
 * via call of SQL query we provide using <b>offset.select</b> option.
 * The actual time interval between batches can be longer because Spark itself introduces
 * a substantial overhead time.
 * <p><b>rowsPerOffset</b> variable specifies how many records we want per single <b>offset</b>
 * Notice that a micro-batch can comprise several offsets depending on the resulting batch interval.
 */
val cpDirName = "/tmp/gp-streaming-example/checkpoint"
val secondsPerBatch = 1.0
val rowsPerOffset: Int = 10

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
  v_id bigint;
  v_counter bigint := 0;
  v_offset bigint;
  v_rec_per_offset bigint := ${rowsPerOffset};
begin
  if v_batch_size > 0 then
    v_sleep :=  ${secondsPerBatch} / v_batch_size::float / v_rec_per_offset::float;
  end if;
  v_id := (v_start_offset + 1) * v_rec_per_offset;
  loop
    v_id := v_id + 1;
    exit when v_id > v_end_ofsset * v_rec_per_offset;
    v_offset := v_id / v_rec_per_offset;
    v_counter := v_counter + 1;
    insert into <ext_table>
    select  <select_colList>
    from    (
      select  v_id id, v_counter seq_n, clock_timestamp() gen_ts, v_offset offset_id
        ) a;
    if v_sleep >= 0.001 then
      perform pg_sleep(v_sleep);
    end if;
  end loop;
end
$$$$"""

/**
* This PL/pgSQL script process records passed over the pipline to the Greenplum receiver side and prints some statistics.
*/
val aggregator = """do $$
declare
  v_str text;
begin
  select  format(e'\n\nmin_offset=%s\nmax_offset=%s\navg_delay_spark=%s\nmin_delay_spark=%s\nmax_delay_spark=%s\navg_delay_gp=%s\nmin_delay_gp=%s\nmax_delay_gp=%s\nmin_gen=%s\nmax_gen=%s',
            min(offset_id), max(offset_id),
            avg(delay_s), min(delay_s), max(delay_s),
            avg(extract('epoch' from clock_timestamp()-gen_ts)),
            min(extract('epoch' from clock_timestamp()-gen_ts)),
            max(extract('epoch' from clock_timestamp()-gen_ts))
            , min(gen_ts), max(gen_ts)
            )
  into    v_str
  from    <ext_table> us
  ;
  raise notice '%', v_str;
end
$$"""

/**
 * Our connector provides some useful UDFs as bonus.
 * The following line makes them available for Spark SQL expressions:
 */
com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
println(s"Connector version: ${com.itsumma.gpconnector.ItsMiscUDFs.getVersion}")

val stream = (spark.readStream.format("its-greenplum").option("url", dbUrl).
  option("user", dbUser).
  option("password", dbPassword).
  /**
  * dbtable option here specifies Spark DataFrame columns name/type and must correspond to the generator script output.
  */
  option("dbtable","select 1::bigint id, 1::int seq_n, clock_timestamp() gen_ts, 1::bigint offset_id").
  option("sqlTransfer", generator).
  option("offset.select", s"select json_build_object('offset_ts', (extract(epoch from pg_catalog.clock_timestamp()) * ${1.0/secondsPerBatch})::bigint)::text").
  load().
  /**
   * getRowTimestamp() UDF returns idividual row timestamp containing a moment when this row come to Spark.
  */
  withColumn("spark_ts", com.itsumma.gpconnector.ItsMiscUDFs.getRowTimestamp()).
  /**
   * getBatchId() UDF returns current micro-batch number, also know as 'epoch' in Spark
  */
  selectExpr("getBatchId() as batch_id", "id", "seq_n", "gen_ts", "spark_ts", "(cast(spark_ts as double) - cast(gen_ts as double)) as delay_s", "offset_id").
writeStream.
  format("its-greenplum").option("url", dbUrl).
  option("user", dbUser).
  option("password", dbPassword).
  option("sqlTransfer", aggregator).
  option("dbmessages", "WARN").
  option("checkpointLocation", cpDirName).
  outputMode("append").
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


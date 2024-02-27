import org.apache.spark.sql.SaveMode
import java.util.UUID.randomUUID
import java.time._
import spark.implicits._

/*
This script will create 4 db tables and fill it with some data.
Use the following query to view results:
select  '1: ' || count(1)::text uuid_seq from uuid_seq
union all
select  '2: ' || count(1)::text uuid_seq from uuid_seq_2
union all
select  '3: ' || count(1)::text uuid_seq from uuid_seq_3
union all
select  '4: ' || count(1)::text uuid_seq from uuid_seq_4
order by 1

*/

case class LoopRead(opts: Map[String, String]) {
    def run(): Unit = {
        var cnt = 0
        while (cnt < 5) {
            cnt += 1
            val cmd = s"SELECT * FROM uuid_seq limit ${cnt + 5}"
            val gpdf = spark.read.format("its-greenplum").options(opts.updated("dbtable", cmd)).load()
            println("\r\n*************************************************")
            println(s"Should display ${cnt + 5} rows from uuid_seq table")
            println("*************************************************")
            gpdf.show(false)
        }
    }
}

case class LoopWrite(opts: Map[String, String]) {
    def run(): Unit = {
        var cnt = 0
        while (cnt < 5) {
            cnt += 1
            println("\r\n*************************************************")
            println(s"Should append 1000 rows into uuid_seq_3 table (pass ${cnt})")
            println("*************************************************")
            sc.
                parallelize(Array.fill[String](1000){randomUUID().toString}.zipWithIndex.map({case (uid,id) => {(uid, id, java.sql.Timestamp.from(OffsetDateTime.now().toInstant), id % 2 == 1)}}), 2).
                toDF("id", "seq_no", "created_d", "mv").
            write.
                format("its-greenplum").
                options(opts.updated("dbtable","uuid_seq_3")).
                mode(SaveMode.Append).
                save()

        }
    }
}

//sc.setLogLevel("INFO")
val options = Map("url"->"jdbc:postgresql://greenplum-master-host:5432/db-name", "user"->"your_gp_db_user", "password"->"gp_user_password", "dbtable"->"uuid_seq")

var df = sc.
    parallelize(Array.fill[String](1000){randomUUID().toString}.zipWithIndex.map({case (uid,id) => {(uid, id, java.sql.Timestamp.from(OffsetDateTime.now().toInstant), id % 2 == 1)}}), 2).
    toDF("id", "seq_no", "created_d", "mv")
println("\r\n*************************************************")
println("Should append 1000 rows into uuid_seq table")
println("*************************************************")
df.write.
    format("its-greenplum").
    options(options).
    mode(SaveMode.Append).
    save()

var gpdf = spark.read.format("its-greenplum").options(options).load()
println("\r\n*************************************************")
println("Should display 20 rows from uuid_seq table")
println("*************************************************")
gpdf.show(false)
gpdf.count()

println("\r\n*************************************************")
println("Should copy uuid_seq table into uuid_seq_2 recreating the later")
println("*************************************************")
gpdf.write.format("its-greenplum").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

println("\r\n*************************************************")
println("Should copy uuid_seq table into uuid_seq_2 recreating the later")
println("*************************************************")
gpdf.repartition(7).write.format("its-greenplum").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

//gpdf.repartition(col("created_d")).write.format("its-greenplum").options(options.updated("dbtable","uuid_seq_2")).mode("overwrite").save()

gpdf = spark.read.format("its-greenplum").options(options.updated("dbtable","select * from uuid_seq where seq_no < 0")).load()
println("\r\n*************************************************")
println("Should display empty rowset und recreate uuid_seq_3 empty table")
println("*************************************************")
gpdf.show(false)
gpdf.count()
gpdf.write.format("its-greenplum").options(options.updated("dbtable","uuid_seq_3")).mode("overwrite").save()

gpdf = spark.read.format("its-greenplum").options(options.updated("dbtable","SELECT * FROM uuid_seq limit 1")).load()
println("\r\n*************************************************")
println("Should display 1 row and append it to uuid_seq_4 table")
println("*************************************************")
gpdf.show(false)
gpdf.count()
gpdf.write.format("its-greenplum").options(options.updated("dbtable","uuid_seq_4")).mode("append").save()


LoopRead(options).run()
LoopWrite(options).run()

println("\r\n*************************************************")
println("Done!")
println("*************************************************")

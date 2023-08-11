# Advanced usage of ITSumma Spark Greenplum Connector

In addition to the usual ability to read or write data from/to single database table
this connector afford means to freely manipulate data on the DB side
in the context of data processing transaction. 
This opens possibility to use Spark and DB facilities in co-operative parallel manner
giving gain in processing power and resource saving.

Via **dbtable** option in data-reading operation it is possible to pass
an arbitrary SQL select query which can tailor data to be put into Spark DataFrame 
on the fly, for example a call of any PostgreSQL/Greenplum function that can return a set of records.

With **sqltransfer** option one can gain even more capabilities:
 - execute arbitrary PL/pgSQL code before and/or after data transfer, in particular create permanent or temporary tables
 - make any transformations and joins with DB tables on data read from or written to the Spark without a need for intermediate storage
 - process micro-batches in the context of Structured Streaming pipeline
 - handle Spark checkpoint offsets to satisfy Exactly-Once semantics requirements

Within Structured Streaming pipeline in some situations data can be processed 
piecewise during transfer instead of been first collected in a whole
on the consumer side, thus saving resources.

## Read operations (transfer data from DB to Spark)
SQL statement passed via **sqltransfer** option could be a standalone DML
or PL/PpgSQL anonymous block containing the following operation:
~~~
INSERT INTO <ext_table> (optional_list_of_ext_table_colum_names)
SELECT list_of_expressions FROM SOURCE_TABLE
OPTIONAL_JOINED_TABLES_CLAUSE
WHERE_CLAUSE
~~~
or
~~~
DO $$
DECLARE
 data_record record;
BEGIN
 FOR data_record in (
      SELECT list_of_expressions FROM SOURCE_TABLE
      OPTIONAL_JOINED_TABLES_CLAUSE
      WHERE_CLAUSE
      )
 LOOP
   .. do wahtever you need with columns of data_record to prepare a list_of_values ..
   INSERT INTO <ext_table> (optional_list_of_ext_table_colum_names)
   VALUES (list_of_values);
 END LOOP;
END $$
~~~
, where `<ext_table>` is a string tag which will be substituted with the
name of writable external table used to transfer data between DB and Spark. 
``list_of_expressions`` can contain any valid SQL expressions,
user defined or inbuilt Greenplum functions; also, connector substitutes a default one 
matching schema of the output DataFrame via `<select_colList>` string tag.
Structure of the `<ext_table>` table corresponds to the schema of Spark DataFrame 
from where Spark will read data and should be specified using one of three methods:
 - pass in some existing DB table name via **dbtable** option; resulting output DataFrame will derive its structure from this table
 - pass in some SQL select statement via **dbtable** option (can produce 1 or 0 rows); resulting output DataFrame will derive its structure from the dataset columns metadata
 - build a custom schema of type org.apache.spark.sql.types.StructType and supply it via .schema(customSchema) in your pipeline

Spark can push some filtering conditions to the data source.  
In this case `<pushed_filters>` string tag will be substituted with a list
of expressions you must include into **WHERE_CLAUSE**.

### Structured Streaming read operation
In this mode Spark in essence poll your data source for new records. 
You should write SQL query to extract data for micro-batches and, preferably, 
pass it via **sqltransfer** option.
The poll interval is defined on the sink side of the pipeline (after writeStream clause), 
via .trigger(trigger_instance). Depending on type of Trigger the following variants available:
 - **unspecified** - (the default): poll interval = 100ms or processing time of micro-batch, whatever is larger
 - **Trigger.ProcessingTime** poll interval = specified interval or processing time of micro-batch, whatever is larger

See Spark documentation on [Triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers) for more.

### Using Spark checkpoints / offset to achieve Exactly-Once fault-tolerance guarantees
Spark will call SQL select you provide via **offset.select** option 
to know the **offset** associated 
with the latest available position in your data stream. **offset** is just a JSON string that 
characterise stream position somehow. You can include any timestamps or identifiers of you choice there.
 Spark never interprets **offset** anyhow, but checks equality between previously 
processed, stored into checkpoint **offset** with the latest available. Inequality means
there is new data arrived and new micro-batch will be formed. After application failure,
any batch that has not been stored into checkpoint yet will be processed again, so any
operation in the pipeline should be idempotent, i.e. able to be repeated without side effects.  

On every micro-batch, connector substitutes actual offset values into start and end 
string tags of SQL operator you passed via **sqltransfer** option:
 - **<start_offset_json>** - substituted with micro-batch start offset JSON string
 - **<end_offset_json>** - substituted with micro-batch end offset JSON string

To fulfill the Spark checkpoint and recovery mechanism requirements ypu should 
return only records that correspond the start/end offset range in every micro-batch.
Remember that start offset is always exclusive, as it is already processed by a previous batch,
and an end offset is always inclusive.

## Write operations (transfer data from Spark to DB)
SQL statement passed via **sqltransfer** option could be a standalone DML
or PL/PpgSQL anonymous block containing the following operation:
~~~
INSERT INTO DESTINATION_TABLE (optional_list_of_destination_table_colum_names)
SELECT list_of_expressions FROM <ext_table>
OPTIONAL_JOINED_TABLES_CLAUSE
OPTIONAL_WHERE_CLAUSE
~~~
or
~~~
DO $$
DECLARE
 data_record record;
BEGIN
 FOR data_record in (
      SELECT list_of_expressions FROM <ext_table>
      OPTIONAL_JOINED_TABLES_CLAUSE
      OPTIONAL_WHERE_CLAUSE
      )
 LOOP
   .. do wahtever you need with columns of data_record ..
 END LOOP;
END $$
~~~
, where `<ext_table>` is a string tag which will be substituted with the
name of readable external table used to transfer data between Spark and DB.  
``list_of_expressions`` can contain any valid SQL expressions,
user defined or inbuilt Greenplum functions and so on, transforming 
and combining data extracted from the `<ext_table>`. Structure of the `<ext_table>` table
corresponds to the schema of Spark DataFrame from where data are sourced.

In the case of a streaming operation the following additional parameters provided via string tag substitution:
 - **<current_epoch>** - sequential number of micro-batch assigned by Spark during stream processing, consistent across application restarts
 - **<stream_mode>** - Spark streaming OutputMode: one of Append, Update or Complete - see [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes)

### Achieve Exactly-Once fault-tolerance guarantees
There is nothing very spacial you should do for that on the data-sink side but make sure 
that your operation is idempotent, that is it avoids any side effects which cannot be 
undone using Greenplum transaction rollback, like autonomous transactions or 
sending data to remote systems.

But just in case there is **undo.side.effects.sql** option where you can pass SQL DML
to try undo those side effects, though success is not guaranteed. 

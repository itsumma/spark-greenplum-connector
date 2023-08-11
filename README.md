# ITSumma Spark Greenplum Connector
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Version strategy

Version number adheres to *major.minor.patch* semantics, however a *major* part is reserved to match
corresponding Spark version, and currently it can by 2 (for Spark 2) or 3 (for Spark 3).  
Releases for Spark 2 and 3 with the same minor and patch number 
provide (as much as possible) the same functionality, similar bug fixes and so on.  
Package (jar) files are named as follows:  
`spark-greenplum-connector_scalaMajor.scalaMinor-sparkMajor.connectorMinor.jar`  
Source code maintained in **spark2** branch for Spark v2, in **spark3** branch for Spark v3.

## Requirements

1. Spark v2.* or Spark v3.*
2. Greenplum database
3. Unrestricted network access from any Greenplum segment to any Spark cluster worker node on arbitrary TCP port.

## Features

You can use this connector with Spark DataSource API V2 to read and write data from/to Greenplum database.  
The batch mode as well as Structured Streaming using the micro-batch processing is supported.  
There are some [advanced features](Advanced-usage.md) included, for example it is possible to use PL/pgSQL anonymous block 
or function as data source or sink during read and write operations.
This technique allows to delegate some part of data processing onto the DB side.

### How to use

1. [Download](https://github.com/itsumma/spark-greenplum-connector/releases/download/v2.1.0/spark-greenplum-connector_2.11-2.1.jar) or build the library. See build instructions [here](BUILD.md).
2. Copy jar-file to `/path/to/spark-greenplum-connector_x.x-x.x.jar`, replace /path/to with your Spark **jars** folder path
3. Optionally, depending on your Spark installation, you may need to edit conf/spark-defaults.conf and place here:
```
spark.driver.extraClassPath     /path/to/spark-greenplum-connector_x.x-x.x.jar
spark.executor.extraClassPath     /path/to/spark-greenplum-connector_x.x-x.x.jar
```
NOTE: Take care to remove older versions of this connector from the **jars** folder to avoid conflicts.

4. Create Greenplum database user with appropriate privileges to read, write and execute objects. For example, in the **psql** or other Grenplum client do:
```
create user database_user with password 'yourpassword';
alter user database_user with superuser;
alter role database_user with createexttable ( type = 'readable', protocol = 'gpfdist' );
```

#### Verification:
Run the spark-shell.

To read from the database (provided a table with name source_table_name exists):
```
scala> val gpdf = spark.read.format("its-greenplum").
    option("url", "jdbc:postgresql://gp-master-host:5432/database").
    option("user", "database_user").
    option( "password", "yourpassword").
    option("dbtable","source_table_name").load()
scala> gpdf.show()
scala> gpdf.count()
```

You also may use arbitrary SQL queries instead of plain Greenplum table name: 
```
scala> val gpdf = spark.read.format("its-greenplum").
    option("url", "jdbc:postgresql://gp-master-host:5432/database").
    option("user", "database_user").
    option( "password", "yourpassword").
    option("dbtable","SELECT * FROM pg_stat_activity").load()
```

To write to the database:

Store previously initialized `gpdf` DataFrame object back into the database
```
scala> gpdf.write.format("its-greenplum").
    option("url", "jdbc:postgresql://gp-master-host:5432/database").
    option("user", "database_user").
    option( "password", "yourpassword").
    option("dbtable","dest_table_name").
    mode("append").save()
```
Table with the name **dest_table_name** will be created during this operation if it doesn't exist. 

See comments in the **examples/streaming-example.scala** file for more examples and instructions.

### Connector options

 - **url** - Greenplum database JDBC connection string
 - **dbschema** - Greenplum database schema (object name space) or comma separated list of schemas, where to create/search objects
 - **user** - gpdb user/role name
 - **password** - gpdb password for the user
 - **dbtimezone** - Useful when Greenplum database time zone is different from the Spark time zone
 - **dbtable** - Greenplum database table name. For read operation also can be arbitrary SQL query string.
 - **sqltransfer** - substitute custom query or PL/pgSQL anonymous block for Greenplum DB *GPFDIST* protocol `insert into .. select` operator. See [advanced features](Advanced-usage.md) for use cases. If **sqltransfer** is specified, **dbtable** is optional and can be used to tune the columns schema and type mapping.
 - **distributedby** - supply `DISTRIBUTED BY` clause value when creating the destination table in write operations (in conjunction with **dbtable** option), or an intermediate "writable external" table in read operations, see Greenplum documentation for details
 - **partitionclause** - for write operations only, used together with **dbtable** option, allows to append partitioning clause or any arbitrary text to the end of DB table creation statement
 - **tempexttables** - "true" to use temporary external tables for Greenplum DB *GPFDIST* protocol (default), or "false" for persistent  
 - **truncate** - for write operations only, use `truncate table dbtable` SQL operator instead of drop/create to preserve the output table structure when `overwrite` mode specified for write operation. "true" or "false", default "false"
 - **server.port** - overrides tcp port for GPFDIST protocol server to be used by each executor instance. By default, an ephemeral (random) port is used. 
 - **network.timeout** - limit time of internal driver/executor and executor/executor data communications, default 60s. Raises an exception if expired.
 - **server.timeout** - limit GPFDIST protocol transfer time, By default unlimited.
 - **dbmessages** - level of the log messages, generated by DB executable objects using **raise notice** SQL operator, can be INFO, WARN or OFF, default is OFF
 - **ApplicationName** - assigns a name to the DB cursors used by this connector in the Greenplum, value of SparkContext.applicationId is used by default. Note: the property name is case-sensitive!
 - **offset.select** - for the Structured Streaming read operation provide SQL select query returning a JSON you assign to represent the latest available position (or **"offset"**) in the input Greenplum DB data source. Could contain timestamps and/or any identifiers of your choice. Used by the Spark **checkpoint** mechanism to achieve Exactly Once semantics over application restart. See [advanced features](Advanced-usage.md) for details. This query will be called on start of every micro-batch to determine the last offset it will process. 
 - **offset.update** - optionally, one can put here a SQL DML operator with single question mark parameter, like that: `update some_table set last_commit_offset = ?`. Connector pass there a last completed offset when the stream data sink commits every micro-batch.
 - **stream.read.autocommit** - Default=true; when false, in the stream read mode interaction with Greenplum DB will go in a single transaction per micro-batch. Together with **offset.update** and other options can be useful for a custom offset handling. 


### Supported data types

|Spark/catalyst| Postgres/Greenplum           |Java/Scala          |
|--------------|------------------------------|--------------------|
|StringType    | TEXT or VARCHAR(long_enough) |String              |
|StringType    | UUID `*`                     |String              |
|IntegerType   | INTEGER                      |Int                 |
|LongType      | BIGINT                       |java.math.BigInteger|
|DoubleType    | DOUBLE PRECISION             |Double              |
|FloatType     | REAL                         |Double              |
|ShortType     | INTEGER                      |Int                 |
|ByteType      | BYTE                         |Int                 |
|BooleanType   | BIT(1)                       |Boolean             |
|BooleanType   | BOOLEAN                      |Boolean             |
|BinaryType    | BYTEA                        |Array[byte]         |
|TimestampType | TIMESTAMP                    |java.sql.Timestamp  |
|DateType      | DATE                         |java.sql.Date       |
|DecimalType   | DECIMAL(precision,scale)     |java.math.BigInteger|
|StringType    | BIT(n), VARBIT `**`          |String              |
|StringType    | GEOMETRY `***`               |String              |
|StringType    | JSON                         |String              |

 `*` For existing Greenplum table containing a column of the corresponding type
 
 `**`  Total row size including all fields up to 64k
 
 `***` PostGIS GEOMETRY. See http://postgis.net/workshops/postgis-intro/geometries.html

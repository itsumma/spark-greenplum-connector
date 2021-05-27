# DevOpsProdigy Spark Greenplum Connector
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Requirements

1. Spark v2.*
2. gpfdist executable from the greenplum distribution must be in the system path on every Spark executor node

## Features

This repo is still an alpha version of the connector. You can use the connecter via DataSource API V2 either to read or to write to Greenplum database.

### How to use

1. Compile the library `mvn clean package`
2. Copy jar-file to `/path/to/spark-greenplum-connector_2.11-1.1.jar` (replace /path/to to the installation path)
3. In spark installation folder edit conf/spark-defaults.conf and place here:
```
spark.driver.extraClassPath     /path/to/spark-greenplum-connector_2.11-1.1.jar
spark.executor.extraClassPath     /path/to/spark-greenplum-connector_2.11-1.1.jar
```
4. Run the spark-shell.

To read from a database:
```
scala> val gpdf = spark.read.format("its-greenplum").option("url", "jdbc:postgresql://hostname:5432/database").option("user", "yourDbAccount").option( "password", "yourpassword").option("dbtable","table_name").load()
scala> gpdf.show()
scala> gpdf.count()
```

To write to a database:

Use your existing `gpdf` as a DataFrame object
```
scala> gpdf.write.format("its-greenplum").option("url", "jdbc:postgresql://hostname:5432/database").option("user", "yourDbAccount").option( "password", "yourpassword").option("dbtable","table_name").mode(SaveMode.Append).save()
```

### Connector options

 - url - JDBC connection string URL
 - dbtable - gpdb table
 - user - gpdb user/role name
 - password - gpdb password for the user
 - server.path - path to gpfdist binary


### Supported data types

|Spark/catalyst|Postgres/Greenplum          |Java/Scala          |
|--------------|----------------------------|--------------------|
|StringType    |TEXT or VARCHAR(long_enough)|String              |
|StringType    |UUID `*`                    |String              |
|IntegerType   |INTEGER                     |Int                 |
|LongType      |BIGINT                      |java.math.BigInteger|
|DoubleType    |DOUBLE PRECISION            |Double              |
|FloatType     |REAL                        |Double              |
|ShortType     |INTEGER                     |Int                 |
|ByteType      |BYTE                        |Int                 |
|BooleanType   |BIT(1)                      |Boolean             |
|BooleanType   |BOOLEAN                     |Boolean             |
|BinaryType    |BYTEA                       |Array[byte]         |
|TimestampType |TIMESTAMP                   |java.sql.Timestamp  |
|DateType      |DATE                        |java.sql.Date       |
|DecimalType   |DECIMAL(precision,scale)    |java.math.BigInteger|
|StringType    |BIT(n), VARBIT `**`         |String              |
|StringType    |GEOMETRY `***`              |String              |

 `*` For existing Greenplum table containing a column of the corresponding type
 
 `**`  Total row size including all fields up to 64k
 
 `***` PostGIS GEOMETRY. See http://postgis.net/workshops/postgis-intro/geometries.html

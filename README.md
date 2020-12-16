# DevOpsProdigy Spark Greenplum Connector
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Requirements

1. Spark v2.*
2. gpfdist executable from the greenplum distribution must be in the system path on every Spark executor node

## Features

This is an alpha version of the connector. It could be used via DataSource API V2 only for reading from greenplum database. The content of the table is accessible through the dataframe interface of Spark.

### How to use

1. Compile the library `mvn clean package`
2. Copy jar-file to `/path/to/spark-greenplum-connector_2.11-1.0.jar` (replace /path/to to the installation path)
3. In spark installation folder edit conf/spark-defaults.conf and place here:
```
spark.driver.extraClassPath     /path/to/spark-greenplum-connector_2.11-1.0.jar
spark.executor.extraClassPath     /path/to/spark-greenplum-connector_2.11-1.0.jar
```
4. Run the spark-shell and select contents of entire table from the greenplum database:
```
scala> val gpdf = spark.read.format("greenplum").option("url", "jdbc:postgresql://hostname:5432/database").option("user", "yourDbAccount").option( "password", "yourpassword").option("dbtable","table_name").load()
scala> gpdf.show()
scala> gpdf.count()
```

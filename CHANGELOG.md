# 3.1.0 (2024-02-27)
### New Features
* Release for Spark 3.4.1 and up - the first version with support for Spark 3
* Data throughput boost to 10..20M byte/s per Greenplum segment
* Sub-second latency in the micro-batch streaming mode 

# 2.1.0 (2023-08-11)
### New Features
* Spark Structured Streaming micro-batch mode support for read and write operations
* Cooperative data processing on DB and Spark side
* Utilisation of Spark data locality facilities (experimental)

### Bug Fixes
* Fixed memory leaks in driver and executor code
* General stability improvements

# 2.0.0 (2021-11-12)

### New Features
* Removed dependency on the Greenplum`gpfdist` executable
* Better integration with Spark DSAPIv2 interfaces

### Bug Fixes
* Fixed problem with reading/writing very small datasets (from single record and up)
* Fixed some internal race conditions leading to hang-up previously

# 1.1 (2021-05-27)

### New Features
* Now it's ready to write using DSAPIv2!
* Added support for Geometry type of PostGIS

### Bug Fixes
* Fixed work in a cluster mode

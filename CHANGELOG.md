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

package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.{GPClient, GreenplumRowSet}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class GreenplumScanBuilder(optionsFactory: GPOptionsFactory, rowSet: GreenplumRowSet, var schema: StructType)
  extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns
    with Logging
{
  private var whereClause = ""
  private var pushedDownFilters: Array[Filter] = new Array[Filter](0)
  logDebug(s"""options=\n${optionsFactory.dumpParams()}""")
  private var greenplumScan: GreenplumScan = null

  override def build(): Scan = this.synchronized {
    if (greenplumScan == null)
      greenplumScan = new GreenplumScan(optionsFactory, rowSet, schema, whereClause)
    greenplumScan
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val tuple3 = SparkSchemaUtil(optionsFactory.dbTimezone).pushFilters(filters)
    whereClause = tuple3._1
    pushedDownFilters = tuple3._3
    tuple3._2
  }

  override def pushedFilters(): Array[Filter] = pushedDownFilters

  override def pruneColumns(requiredSchema: StructType): Unit = {
    schema = requiredSchema
  }
}

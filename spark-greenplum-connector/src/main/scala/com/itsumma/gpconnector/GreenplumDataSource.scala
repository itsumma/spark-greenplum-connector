package com.itsumma.gpconnector

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util
import org.apache.spark.sql.types.StructType

class GreenplumDataSource extends TableProvider
  with DataSourceRegister {

  override def shortName(): String = "its-greenplum"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val rowSet = new GreenplumRowSet(null, Array.empty[Transform], options.asCaseSensitiveMap())
    val ret: StructType = rowSet.schema()
    rowSet.clear()
    ret
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new GreenplumRowSet(schema, partitioning, properties)
  }
}

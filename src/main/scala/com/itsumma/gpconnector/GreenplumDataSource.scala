package com.itsumma.gpconnector

import com.itsumma.gpconnector.reader.GreenplumDataSourceReader
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

import scala.collection.JavaConversions.mapAsScalaMap

class GreenplumDataSource extends DataSourceV2
  with ReadSupport
  //with WriteSupport
  with DataSourceRegister {
  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    val opts = new GPOptionsFactory(dataSourceOptions.asMap().toMap)
    new GreenplumDataSourceReader(opts)
  }

  //override def createWriter(s: String, structType: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = ???

  override def shortName(): String = "greenplum"
}

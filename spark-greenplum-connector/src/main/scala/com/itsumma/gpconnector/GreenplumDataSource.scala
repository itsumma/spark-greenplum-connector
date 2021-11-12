package com.itsumma.gpconnector

import com.itsumma.gpconnector.reader.GreenplumDataSourceReader
import com.itsumma.gpconnector.writer.GreenplumDataSourceWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

import java.util.Optional
import scala.collection.JavaConversions.mapAsScalaMap

class GreenplumDataSource extends DataSourceV2
  with ReadSupport
  with WriteSupport
  with DataSourceRegister {

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    val opts = new GPOptionsFactory(dataSourceOptions.asMap().toMap)
    new GreenplumDataSourceReader(opts)
  }

  override def createWriter(writeUUID: String, schema: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    val opts = new GPOptionsFactory(dataSourceOptions.asMap().toMap)
    Optional.of( new GreenplumDataSourceWriter(writeUUID, schema, saveMode, opts ) )
  }

  override def shortName(): String = "its-greenplum"
}

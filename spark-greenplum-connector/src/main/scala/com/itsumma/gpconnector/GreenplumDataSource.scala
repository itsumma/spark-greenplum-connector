package com.itsumma.gpconnector

import com.itsumma.gpconnector.reader.GreenplumDataSourceReader
import com.itsumma.gpconnector.writer.GreenplumDataSourceWriter
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

import java.util.{Optional, UUID}
import scala.collection.JavaConversions.mapAsScalaMap

class GreenplumDataSource extends DataSourceV2
  with DataSourceRegister
  with ReadSupport
  with MicroBatchReadSupport
  with WriteSupport
  with StreamWriteSupport
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private var gpClient: GPClient = null
  logger.debug(s"GreenplumDataSource instance created")

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    val opts = GPOptionsFactory(dataSourceOptions.asMap().toMap)
    logger.debug(s"createReader(\n${opts.dumpParams()})")
    new GreenplumDataSourceReader(opts)
  }

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {

    val opts = GPOptionsFactory(options.asMap().toMap)
    logger.debug(s"createMicroBatchReader(\n${opts.dumpParams()}," +
      s"\nschema=${schema.orElse(StructType(new Array[StructField](0))).mkString("(",",",")")})")
    new GreenplumDataSourceReader(opts, Option(schema.orElse(null)), Option(checkpointLocation))
  }

  override def createWriter(writeUUID: String, schema: StructType, saveMode: SaveMode, dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    val opts = GPOptionsFactory(dataSourceOptions.asMap().toMap)
    if (gpClient == null)
      gpClient = GPClient(opts)
    //val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
    logger.debug(s"\npassedInProperties=${opts.dumpParams()}")
    logger.debug(s"\nwriteUUID='${writeUUID}")
    Optional.of( new GreenplumDataSourceWriter(writeUUID, schema, saveMode, opts, gpClient) )
  }

  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    val opts = GPOptionsFactory(options.asMap().toMap)
    if (gpClient == null)
      gpClient = GPClient(opts)
    //val queryId: String = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
    logger.debug(s"\npassedInProperties=${opts.dumpParams()}")
    logger.debug(s"createStreamWriter: queryId='${queryId}")
    new GreenplumDataSourceWriter(queryId, schema, SaveMode.Append, opts, gpClient, Option(mode))
  }

  override def shortName(): String = "its-greenplum"

}

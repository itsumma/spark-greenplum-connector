package com.itsumma.gpconnector.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

/*
case class GreenplumDataWriterInstanceInfo(writeUUID: String, partInstanceId: String,
                                           partitionId: Int, taskId: Long, epochId: Long,
                                           writerInstance: GreenplumDataWriter)
{}
*/

class GreenplumDataWriterFactory(writeUUID: String, schema: StructType,
                                 optionsFactory: GPOptionsFactory, rmiRegistryAddress: String)
 extends DataWriterFactory[InternalRow]
{
  //private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new GreenplumDataWriter(writeUUID, schema, optionsFactory, rmiRegistryAddress, partitionId, taskId, epochId)
  }
}

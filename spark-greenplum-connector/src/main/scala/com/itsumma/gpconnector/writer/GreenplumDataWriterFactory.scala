package com.itsumma.gpconnector.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.types.StructType
/*
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
*/


class GreenplumDataWriterFactory(writeUUID: String, schema: StructType,
                                 optionsFactory: GPOptionsFactory, rmiRegistryAddress: String)
 extends DataWriterFactory
 with StreamingDataWriterFactory
{
  //private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new GreenplumDataWriter(writeUUID, schema, optionsFactory, rmiRegistryAddress, partitionId, taskId)
  }

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new GreenplumDataWriter(writeUUID, schema, optionsFactory, rmiRegistryAddress, partitionId, taskId, Option(epochId))
  }
}

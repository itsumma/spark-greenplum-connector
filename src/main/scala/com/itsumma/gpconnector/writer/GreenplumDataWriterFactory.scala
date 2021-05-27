package com.itsumma.gpconnector.writer

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.{GPOptionsFactory, SparkSchemaUtil}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/*
case class GreenplumDataWriterInstanceInfo(writeUUID: String, partInstanceId: String,
                                           partitionId: Int, taskId: Long, epochId: Long,
                                           writerInstance: GreenplumDataWriter)
{}
*/

class GreenplumDataWriterFactory(writeUUID: String, schema: StructType, saveMode: SaveMode,
                                 optionsFactory: GPOptionsFactory, numPartsLimit: Int)
 extends DataWriterFactory[InternalRow]
{
  private val logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val numWriters = new AtomicLong(0)
  def getNWriters: Long = numWriters.get()

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    if (numWriters.incrementAndGet() > numPartsLimit) {
      logger.info(s"createDataWriter returns null due to parts limit " +
        s"writeUUID=$writeUUID, partitionId=$partitionId, taskId=$taskId, epochId=$epochId")
      return null // Preventing greenplum errors like 'external table has more URLs than available primary segments that can write into them'
    }
    val partInstanceId = SparkSchemaUtil.stripChars(UUID.randomUUID.toString, "-")
    new GreenplumDataWriter(writeUUID, partInstanceId, schema, saveMode, optionsFactory, partitionId, taskId, epochId)
  }
}

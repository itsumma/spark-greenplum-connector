package com.itsumma.gpconnector.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.types.StructType

case class GreenplumPartitionReaderFactory (
                               optionsFactory: GPOptionsFactory,
                               queryId: String,
                               schema: StructType,
                               rmiRegistry: String
                             )
  extends PartitionReaderFactory
{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    if (rmiRegistry == null)
      throw new IllegalArgumentException(s"GreenplumPartitionReaderFactory: " +
        s"called with rmiRegistry==null for ${queryId}")
    val pr = new GreenplumInputPartitionReader(optionsFactory, queryId, schema,
      partition.asInstanceOf[GreenplumInputPartitionMeta].getPartNo, rmiRegistry)
    pr
  }
}

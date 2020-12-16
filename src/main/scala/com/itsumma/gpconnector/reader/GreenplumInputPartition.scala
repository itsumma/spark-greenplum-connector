package com.itsumma.gpconnector.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class GreenplumInputPartition(
                               optionsFactory: GPOptionsFactory,
                               queryId: String,
                               instanceId: String,
                               schema: StructType,
                               partitionNo: Int,
                               bufferSize: Int = 1024*1024
                             ) extends InputPartition[InternalRow] {
  override def preferredLocations: Array[String] = {
    val locations = new Array[String](1)
    locations(0) = java.net.InetAddress.getLocalHost.getHostName
    locations
  }
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val pr = new GreenplumInputPartitionReader(optionsFactory, queryId, instanceId, schema, partitionNo, bufferSize)
    pr
  }
}

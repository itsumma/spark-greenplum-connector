package com.itsumma.gpconnector.reader

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class GreenplumInputPartition(
                               optionsFactory: GPOptionsFactory,
                               queryId: String,
                               instanceId: String,
                               schema: StructType,
                               partitionNo: Int,
                               rmiRegistry: String
                             ) extends InputPartition[InternalRow] {

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

  override def preferredLocations: Array[String] = {
    val locations = new Array[String](0)
    //locations(0) = java.net.InetAddress.getLocalHost.getHostName
    // TODO: gather actual node list somehow
    // e.g. use select distinct address from gp_segment_configuration where content >= 0
    // locations(0) = "node1"
    // locations(1) = "node2"
    locations
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    val pr = new GreenplumInputPartitionReader(optionsFactory, queryId, instanceId, schema, partitionNo, rmiRegistry)
    pr
  }
}

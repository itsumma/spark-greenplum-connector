package com.itsumma.gpconnector.reader

import org.apache.spark.sql.connector.read.InputPartition

//case
class GreenplumInputPartitionMeta(partitionNo: Int, locations: Array[String]) extends InputPartition {
  override def preferredLocations: Array[String] = {
    locations
  }

  def getPartNo: Int = partitionNo
}

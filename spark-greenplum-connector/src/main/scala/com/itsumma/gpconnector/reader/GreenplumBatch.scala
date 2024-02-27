package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.GPOffset
import com.itsumma.gpconnector.rmi.NetUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.types.StructType

class GreenplumBatch(optionsFactory: GPOptionsFactory,
                     queryId: String,
                     schema: StructType,
                     greenplumScan: GreenplumScan
                     )
  extends Batch
  with MicroBatchStream
  with Logging
{
  override def planInputPartitions(): Array[InputPartition] = {
    greenplumScan.planInputPartitions()
  }

  private var readerFactory: GreenplumPartitionReaderFactory = null
  override def createReaderFactory(): PartitionReaderFactory = {
    NetUtils().waitForCompletion(optionsFactory.networkTimeout) {
      //greenplumScan.planDone.get() &&
      !greenplumScan.processing.get()
    }
    if (greenplumScan.rmiMaster == null)
      throw new IllegalStateException(s"queryId=${queryId}: createReaderFactory called before planInputPartitions")
    val registryAddress = greenplumScan.rmiMaster.rmiRegistryAddress
    logDebug(s"queryId ${queryId}: new PartitionReaderFactory created")
    if (readerFactory == null)
      readerFactory = GreenplumPartitionReaderFactory(optionsFactory, queryId, schema, registryAddress)
    readerFactory
  }

  override def latestOffset(): Offset = greenplumScan.getEndOffset

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    greenplumScan.setOffsetRange(Option[Offset](start), Option[Offset](end))
  }

  override def initialOffset(): Offset = greenplumScan.getEndOffset

  override def deserializeOffset(json: String): Offset = GPOffset(Option(json))

  override def commit(end: Offset): Unit = greenplumScan.commit(end)

  override def stop(): Unit = {
    greenplumScan.stop()
    readerFactory = null
  }
}

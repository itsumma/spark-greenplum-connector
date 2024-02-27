package com.itsumma.gpconnector.reader

import com.itsumma.gpconnector.GPOffset
import com.itsumma.gpconnector.rmi.NetUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.itsumma.gpconnector.GPOptionsFactory
import org.apache.spark.sql.types.StructType

class GreenplumMicroBatch(optionsFactory: GPOptionsFactory,
                          queryId: String,
                          schema: StructType,
                          greenplumScan: GreenplumScan)
  extends SupportsAdmissionControl
  with MicroBatchStream
  with Logging
{
  override def getDefaultReadLimit: ReadLimit = {
    logDebug("getDefaultReadLimit called")
    ReadLimit.maxRows(10)
  }

  override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
    greenplumScan.getEndOffset
  }

  override def initialOffset(): Offset = greenplumScan.getEndOffset

  override def deserializeOffset(json: String): Offset = GPOffset(Option(json))

  override def commit(end: Offset): Unit = greenplumScan.commit(end)

  override def stop(): Unit = {
    greenplumScan.stop()
    readerFactory = null
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    greenplumScan.setOffsetRange(Option[Offset](start), Option[Offset](end))
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
}

package com.itsumma.gpconnector.rmi

/**
 * Store metadata for partition handler
 * @param handler TaskHandler -  reference to the handler instance
 * @param queryId String - UUID
 * @param partId Int - Spark partition
 * @param instanceId String in the form partitionId:taskId:epochId
 * @param nodeIp String - IP of partition handler
 * @param dir String - query direction, single char of [rRwW], upper case means GPFDIST service holder
 * @param executorId String as of SparkEnv.get.executorId
 * @param batchNo Integer, default null, assigned by our transaction coordinator, 0 - based
 * @param gpSegmentId String, GP segment, default null
 * @param rowCount Long, default 0, number of rows transferred by partition handler
 * @param status String, default null, single char of [i{initial}c{commit}]
 * @param gpfdistUrl String, URL of GPFDIST service available for this handler
 */
case class PartitionControlBlock(
                                 handler: TaskHandler,
                                 queryId: String, partId: Int, instanceId: String,
                                 nodeIp: String, dir: String,
                                 executorId: String,
                                 batchNo: Integer = null,
                                 gpSegmentId: String = null,
                                 rowCount: Long = 0,
                                 status: String = null,
                                 gpfdistUrl: String = null
                                ) {
  override def toString: String = s"""{"queryId": "$queryId", "partId": "$partId",
                                    "instanceId": "$instanceId", "nodeIp": "$nodeIp", "dir": "$dir",
                                    "executorId": "$executorId", "batchNo": $batchNo, "gpSegmentId": $gpSegmentId,
                                    "rowCount": $rowCount, "status": "$status", "gpfdistUrl": "$gpfdistUrl"}"""
}

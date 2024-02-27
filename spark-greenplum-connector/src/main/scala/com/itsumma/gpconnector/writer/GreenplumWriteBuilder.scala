package com.itsumma.gpconnector.writer

import com.itsumma.gpconnector.GPClient
import com.itsumma.gpconnector.rmi.GPConnectorModes
import com.itsumma.gpconnector.rmi.GPConnectorModes.GPConnectorMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsOverwriteV2, SupportsTruncate, Write, WriteBuilder}
import org.apache.spark.sql.itsumma.gpconnector.{GPColumnMeta, GPOptionsFactory, GPTarget, GpTableTypes, SparkSchemaUtil}

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import org.apache.spark.sql.execution.streaming.AsyncProgressTrackingMicroBatchExecution

class GreenplumWriteBuilder(
                             gpClient: GPClient,
                             private var optionsFactory: GPOptionsFactory,
                             private var schema: StructType,
                             private var writeUUID: String
                           )
  extends WriteBuilder
    //with SupportsOverwriteV2
    //with SupportsOverwrite
    //with SupportsDynamicOverwrite
    with SupportsTruncate
    with Logging
{
  logDebug(s"\nPassed in properties = ${optionsFactory.dumpParams()}")
  private var dbTableName: String = optionsFactory.tableOrQuery
  private var saveMode: SaveMode = SaveMode.Append
  private var gpTableMeta: Map[String, GPColumnMeta] = null
  private var targetTableCanonicalName: String = ""
  private var ignoreInsert: Boolean = false
  private val initDone: AtomicBoolean = new AtomicBoolean(false)

  def update(newOptionsFactory: GPOptionsFactory, newSchema: StructType, newWriteUUID: String): Unit = {
    if ((newOptionsFactory != optionsFactory) ||
      (newSchema != schema) ||
      (newWriteUUID != writeUUID)
    ) {
      writeUUID = newWriteUUID
      schema = newSchema
      optionsFactory = newOptionsFactory
      initDone.set(false)
    }
  }

  private def buildDbObjects(): Unit = {
    if (!initDone.get()) {
      initDone.set(true)
      dbTableName = optionsFactory.tableOrQuery
      ignoreInsert = false
      var dbConnection: Connection = gpClient.getConnection()
      try {
        val dbDefaultSchemaName: String = GPClient.checkDbObjSearchPath(dbConnection, optionsFactory.dbSchema)
        if (!dbConnection.getAutoCommit)
          dbConnection.commit()
        dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        dbConnection.setAutoCommit(false)
        val targetDetails: GPTarget = GPTarget(dbTableName)
        targetTableCanonicalName = targetDetails.getCanonicalName(dbDefaultSchemaName)
        val tableDbSchema = targetDetails.getTableSchema(dbDefaultSchemaName)
        if (targetTableCanonicalName.nonEmpty) {
          if (!GPClient.tableExists(dbConnection, targetTableCanonicalName)) {
            val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target)
            var createTable = s"create table ${targetTableCanonicalName} ($createColumnsClause)"
            if (optionsFactory.distributedBy.nonEmpty) {
              createTable += GPClient.formatDistributedByClause(optionsFactory.distributedBy)
            }
            if (optionsFactory.partitionClause.nonEmpty) {
              createTable += s" (${optionsFactory.partitionClause})"
            }
            GPClient.executeStatement(dbConnection, createTable)
            gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
          } else {
            gpTableMeta = SparkSchemaUtil.getGreenplumTableMeta(dbConnection, tableDbSchema, targetDetails.cleanDbTableName)
            if (saveMode == SaveMode.ErrorIfExists) {
              throw new Exception(s"table ${targetTableCanonicalName} exists and saveMode=ErrorIfExists")
            } else if (saveMode == SaveMode.Ignore) {
              ignoreInsert = true
            } else if (saveMode == SaveMode.Overwrite) {
              logDebug(s"saveMode = overwrite, truncate = ${optionsFactory.truncate}")
              if (optionsFactory.truncate) {
                logTrace(s"truncate table ${targetTableCanonicalName}")
                GPClient.executeStatement(dbConnection, s"truncate table ${targetTableCanonicalName}")
              } else {
                val createColumnsClause = SparkSchemaUtil.getGreenplumTableColumns(schema, GpTableTypes.Target, gpTableMeta)
                GPClient.executeStatement(dbConnection, s"drop table ${targetTableCanonicalName}")
                var createTable = s"create table ${targetTableCanonicalName} ($createColumnsClause)"
                if (optionsFactory.distributedBy.nonEmpty) {
                  createTable += GPClient.formatDistributedByClause(optionsFactory.distributedBy)
                }
                if (optionsFactory.partitionClause.nonEmpty) {
                  createTable += s" (${optionsFactory.partitionClause})"
                }
                GPClient.executeStatement(dbConnection, createTable)
              }
            } else {
              logDebug(s"saveMode = append")
            }
          }
          if (!dbConnection.getAutoCommit)
            dbConnection.commit()
        } else if (optionsFactory.sqlTransfer.isEmpty) {
          throw new IllegalArgumentException(s"Must specify either dbtable or sqltransfer option, or both")
        }
      } finally {
        dbConnection.close()
        dbConnection = null
      }
    } else {
      logDebug(s"writeUUID=${writeUUID} buildDbObjects bypass")
    }
  }

  private def newBatch(connectorMode: GPConnectorMode = GPConnectorModes.Batch): GreenplumBatchWrite = {
    new GreenplumBatchWrite(writeUUID, optionsFactory, schema,
      gpTableMeta, targetTableCanonicalName, gpClient, connectorMode, ignoreInsert)
  }

  private def write: Write = new Write {
    override def toBatch: BatchWrite = newBatch()
    override def toStreaming: StreamingWrite = newBatch(GPConnectorModes.MicroBatch)
  }

  override def build(): Write = {
    buildDbObjects()
    write
  }

  override def truncate(): WriteBuilder = {
    logDebug("truncate called")
    saveMode = SaveMode.Overwrite
    this
  }
}

package com.itsumma.gpconnector

import java.time._
import java.sql.Timestamp
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.Properties

/**
 * Provides some helpful SQL user defined functions for Spark.
 * In Scala these UDFs can be called without prior registration as follows:
 * <p>df.withColumn("spark_ts", com.itsumma.gpconnector.ItsMiscUDFs.getRowTimestamp())
 * <p>In pyspark can be called this way:<p>df.selectExpr("getBatchId() as batch_id", ..)
 * <p>Use registerUDFs() method to make them available for SQL expressions.
 */
object ItsMiscUDFs {
  private var properties: Properties = null

  def getVersion: String = {
    if (properties == null) {
      properties = new Properties()
      var stream = getClass.getResourceAsStream("/version.properties")
      if (stream == null)
        stream = getClass.getResourceAsStream("/project.properties")
      if (stream != null) {
        properties.load(stream)
      }
    }
    s"""${properties.getProperty(s"artifactId")} v${properties.getProperty("version")}"""
  }

  val itsGPConnectorVersion: UserDefinedFunction = udf { () =>
    getVersion
  }

  /**
   * Returns current micro-batch number (or "epoch")
   */
  val getBatchId: UserDefinedFunction = udf { () =>
    TaskContext.get().getLocalProperty("streaming.sql.batchId").toInt
  }

  /**
   * Returns current row timestamp,
   * i.e. the time when this row came to Spark
   */
  val getRowTimestamp: UserDefinedFunction = udf { () =>
    Timestamp.valueOf(LocalDateTime.now())
  }

  /**
   * Registers this object UDfs for usage in Spark SQL expressions.
   * Can be called in pyspark as follows:
   * spark.sparkContext._jvm.com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
   * where spark is a SparkSession instance.
   * <p>In scala call directly: com.itsumma.gpconnector.ItsMiscUDFs.registerUDFs()
   */
  def registerUDFs(): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.udf.register("getBatchId", getBatchId)
    spark.udf.register("getRowTimestamp", getRowTimestamp)
    spark.udf.register("itsGPConnectorVersion", itsGPConnectorVersion)
  }
}

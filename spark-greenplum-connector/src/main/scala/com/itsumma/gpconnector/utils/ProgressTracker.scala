package com.itsumma.gpconnector.utils

import org.apache.spark.internal.Logging

import scala.collection.mutable

class ProgressTracker extends Logging {
  private val processingMs = new mutable.HashMap[String, Long]()

  /** Records the duration of running `body` for the next query progress update. */
  def trackProgress[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = body
    val endTime = System.currentTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    processingMs.synchronized {
      val previousTime = processingMs.getOrElse(triggerDetailKey, 0L)
      processingMs.put(triggerDetailKey, previousTime + timeTaken)
    }
    logDebug(s"$triggerDetailKey took $timeTaken ms")
    result
  }

  def results: Map[String, Long] = processingMs.synchronized {
    processingMs.toMap
  }

  def reportTimeTaken(): String = processingMs.synchronized {
    processingMs.mkString("", ", ", "")
  }
}

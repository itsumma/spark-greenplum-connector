package com.itsumma.gpconnector.rmi

import org.apache.spark.internal.Logging

import java.util.concurrent.locks.{Condition, ReentrantLock}

class LocalEvent(private var signaled: Boolean = false, val name: String = "")
  extends Logging
{
  private val lock: ReentrantLock = new ReentrantLock()
  private val condition: Condition = lock.newCondition()


  def signalEvent(signalAll: Boolean = true): Unit = {
    lock.lock()
    try {
      if (!signaled) {
        logTrace(s"Set $name")
        signaled = true
        if (signalAll) {
          condition.signalAll()
        } else {
          condition.signal()
        }
      }
    } finally {
      lock.unlock()
    }
  }

  def isSignalled(clear: Boolean = false): Boolean = {
    var ret: Boolean = false
    lock.lock()
    try {
      ret = signaled
      if (ret && clear) {
        logTrace(s"Reset $name")
        signaled = false
      }
    } finally {
      lock.unlock()
    }
    ret
  }

  def reset(): Boolean = {
    isSignalled(true)
  }

  def waitForEvent(millis: Long, clear: Boolean = false): Boolean = {
    var nanos = millis * 1000 * 1000
    logTrace(s"Wait start $name")
    lock.lock()
    try {
      while (!signaled) {
        if (nanos <= 0L) {
          logTrace(s"Wait timed out $name")
          return false
        }
        nanos = condition.awaitNanos(nanos)
      }
      if (clear)
        signaled = false
    } finally {
      lock.unlock()
    }
    logTrace(s"Wait success $name")
    true
  }
}

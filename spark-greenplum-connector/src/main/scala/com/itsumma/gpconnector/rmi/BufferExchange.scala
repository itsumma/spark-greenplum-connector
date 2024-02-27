package com.itsumma.gpconnector.rmi

import org.apache.spark.internal.Logging

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable

/**
 * Thread-safe data collecting class with zero-copy capabilities backed by a queue of ByteBuffer instances.
 * <p/>Small input byte arrays are collected into a single ByteBuffer instance until its length reach the watermark.
 * <p/>Input byte arrays with length that reaches the watermark are wrapped into ByteBuffer and passed into the
 * output queue as is.<p/>
 * For a consumer thread waiting for data available in the output queue an event driven notification mechanism
 * is implemented allowing to get minimum possible wait lag.
 * @param watermark
 */
class BufferExchange (watermark: Int) extends Logging
{
  private val instGuard = new ReentrantLock()
  private val fbGuard: ReentrantLock = new ReentrantLock()
  private val fbGuardCond: Condition = fbGuard.newCondition()
  private val emptyBuffers: mutable.Queue[ByteBuffer] = new mutable.Queue[ByteBuffer]()
  private val filledBuffers: mutable.Queue[ByteBuffer] = new mutable.Queue[ByteBuffer]()
  private var buff: ByteBuffer = null
  private val availableBytes: AtomicLong = new AtomicLong(0)
  private val ttlEnqueue: AtomicLong = new AtomicLong(0)
  val totalFill: AtomicLong = new AtomicLong(0)
  val totalDrain: AtomicLong = new AtomicLong(0)

  /**
   *
   * @return Total bytes available for reading from this BufferExchange instance, excluding those not flushed yet.
   */
  def available: Long = availableBytes.get()

  /**
   * With this method one can return buffer to the exchange for reuse,
   * or provision a new ByteBuffer(s)
   * @param bb
   */
  def addBuffer(bb: ByteBuffer): Unit = {
    if (bb.capacity() < watermark)
      return
    logTrace("addBuffer enter")
    bb.clear()
    emptyBuffers.synchronized {
      emptyBuffers.enqueue(bb)
    }
    logTrace("addBuffer exit")
  }

  /**
   * Returns ByteBuffer with data.<p/>
   * The caller then exclusively owns the returned buffer until it put it back for reusing via addBuffer method.
   * <p/>If optional argument msMax>=0 then waits for data available;
   * should be called this way only from the data consumer thread distinct from the data producer thread.
   * @param msMax - time to wait, ms. If 0 then waits without time limit.
   * @return ByteBuffer if data available, null otherwise
   */
  def get(msMax: Long = -1): ByteBuffer = {
    val start = System.currentTimeMillis()
    var ret: ByteBuffer = null
    var break: Boolean = false
    logTrace("get using filledBuffers")
    while (!Thread.currentThread().isInterrupted
      && ((msMax < 0) || (System.currentTimeMillis() - start) <= msMax)
      && (ret == null)
      && !break)
    {
      fbGuard.lock()
      try {
        if (filledBuffers.nonEmpty) {
//          instGuard.synchronized {
//          }
          ret = filledBuffers.dequeue()
          var nBytes = ret.position()
          if (nBytes > 0) {
            totalDrain.addAndGet(nBytes)
            nBytes *= -1
            availableBytes.addAndGet(nBytes)
            break = true
          } else {
            ret = null
          }
        }
        if (!break) {
          if (msMax >= 0) {
            var waitNanos = (msMax - (System.currentTimeMillis() - start)) * 1000 * 1000
            if (waitNanos <= 0) waitNanos = 1000000L
            logTrace(s"get will block, msMax=${msMax}")
            fbGuardCond.awaitNanos(if (msMax > 0) waitNanos else 0)
            logTrace("get unblock")
          } else {
            break = true
          }
        }
      } finally {
        fbGuard.unlock()
      }
    }
    logTrace(s"get stop using filledBuffers, ret is${if (ret == null) "" else " not"} null, " +
      s"available=${availableBytes.get()}, totalFill=${totalFill.get()}, totalDrain=${totalDrain.get()}")
    ret
  }

  /**
   * Collects the dataBytes array either by appending it to the existing work ByteBuffer,
   * or by wrapping it into a new one if its size is greater then the watermark value.
   * Collected data then become available for reading when its size crosses the watermark.
   * @param dataBytes
   */
  def put(dataBytes: Array[Byte]): Unit = {
    if (dataBytes != null) {
      logTrace(s"put enter: ${dataBytes.length} bytes")
    } else {
      logWarning(s"put enter: null buffer")
    }
    if (dataBytes == null || dataBytes.length <= 0) {
      logWarning(s"put exit")
      return
    }
    instGuard.synchronized {
      if (buff != null) {
        if (buff.remaining() < dataBytes.length) {
          doFlush()
        }
      }
      var wrapped = false
      if (buff == null) {
        val allocSize: Int = math.max(watermark * 2, dataBytes.length)
        buff = {
          var b1: ByteBuffer = null
          emptyBuffers.synchronized {
            while (emptyBuffers.nonEmpty && (b1 == null || emptyBuffers.size > 5)) {
              b1 = emptyBuffers.dequeue()
              if (b1.capacity() < allocSize) {
                b1 = null
              }
            }
          }
          b1
        }
        if (dataBytes.length >= watermark) {
          buff = ByteBuffer.wrap(dataBytes)
          buff.position(dataBytes.length)
          wrapped = true
        } else if ((buff == null) || (buff.remaining() < dataBytes.length)) {
          buff = ByteBuffer.allocate(allocSize)
        }
      }
      if (!wrapped) {
        buff.put(dataBytes, 0, dataBytes.length)
      }
      totalFill.addAndGet(dataBytes.length)
      if (buff.position() >= watermark) {
        doFlush()
      }
      logTrace(s"put exit: buff${if (buff == null) "=" else "!"}=null, wrapped=${wrapped}, " +
        s"available=${availableBytes.get()}, totalFill=${totalFill.get()}, totalDrain=${totalDrain.get()}")
    }
  }

  /**
   * Flushes the current working buffer making its content available for reading via the output buffer queue.
   */
  def flush(): Unit = instGuard.synchronized {
    logTrace(s"flush enter")
    doFlush()
    logTrace(s"flush exit")
  }

  private def doFlush(): Unit = {
    logTrace("doFlush using filledBuffers")
    fbGuard.lock()
    try {
      var av = availableBytes.get()
      var enq = ttlEnqueue.get()
      val fill = totalFill.get()
      val drain = totalDrain.get()
      var nBuffers = filledBuffers.size
      if (buff != null) {
        val size = buff.position()
        if (size > 0) {
          filledBuffers.enqueue(buff)
          nBuffers += 1
          av = availableBytes.addAndGet(size)
          enq = ttlEnqueue.addAndGet(size)
          if (enq != fill) {
            throw new Exception(s"doFlush: queue unbalanced: " +
              s"available=${av}, " +
              s"nBuffers=${nBuffers}, " +
              s"totalFill=${fill}, ttlEnqueue=${enq}, totalDrain=${drain}")
          }
          fbGuardCond.signalAll()
        } else {
          logWarning(s"doFlush: buff.position()==0")
        }
        buff = null
      } else {
        logWarning(s"doFlush: buff==null")
      }
      logTrace(s"doFlush stop using filledBuffers, available=${av}, " +
        s"nBuffers=${nBuffers}, " +
        s"totalFill=${fill}, ttlEnqueue=${enq}, totalDrain=${drain}")
    } finally {
      fbGuard.unlock()
    }
  }

  def clear(): Unit = {
    logTrace("clear enter")
    availableBytes.set(0)
    fbGuard.lock()
    try {
      filledBuffers.clear()
    } finally {
      fbGuard.unlock()
    }
    emptyBuffers.synchronized {
      emptyBuffers.clear()
    }
    instGuard.synchronized {
      if (buff != null) buff.clear()
      buff = null
    }
    totalFill.set(0)
    totalDrain.set(0)
    logTrace("clear exit")
  }

}

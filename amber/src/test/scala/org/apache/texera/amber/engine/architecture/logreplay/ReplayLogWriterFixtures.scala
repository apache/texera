/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.logreplay

import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * In-process fixtures shared by `AsyncReplayLogWriterSpec` and
  * `ReplayLogManagerImplSpec`, the two specs that drive a real
  * `AsyncReplayLogWriter` thread. Nothing here touches the filesystem, an
  * ActorSystem, or `AmberRuntime.serde`.
  */
object ReplayLogWriterFixtures {

  /** Ordered trace of everything the writer thread did, in the order it did it. */
  sealed trait WriterEvent
  final case class Wrote(record: ReplayLogRecord) extends WriterEvent
  final case class Sent(msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage])
      extends WriterEvent

  /**
    * A flush that actually made previously written records durable. Without this
    * event a trace cannot tell `write; flush; send` from `write; send; flush`,
    * and the second is a durability inversion: the message is on the network
    * while its log record is still in the `DataOutputStream` buffer.
    */
  case object Flushed extends WriterEvent
  case object Closed extends WriterEvent

  /**
    * Thread-safe recorder. The writer thread appends; the test thread reads.
    * `ConcurrentLinkedQueue` supplies both FIFO ordering and the happens-before
    * edge, so no extra synchronisation is needed on the assertion side.
    */
  class Recorder {
    private val events = new ConcurrentLinkedQueue[WriterEvent]()
    private val firstEvent = new CountDownLatch(1)

    def record(event: WriterEvent): Unit = {
      events.add(event)
      firstEvent.countDown()
    }

    /** Blocks until the writer thread produces its first event, or the timeout elapses. */
    def awaitFirstEvent(timeoutMillis: Long): Boolean =
      firstEvent.await(timeoutMillis, TimeUnit.MILLISECONDS)

    def snapshot: List[WriterEvent] = events.asScala.toList

    /** The `handler` an AsyncReplayLogWriter calls for each released output. */
    val handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit =
      msg => record(Sent(msg))
  }

  /**
    * A `SequentialRecordWriter` that records rather than serialises. Overriding
    * `writeRecord` keeps `AmberRuntime.serde` (and therefore a Pekko
    * `ActorSystem`) out of these specs entirely; the wrapped stream is never
    * written to and the base class's `lazy val output` is never forced.
    *
    * `close()` sleeps before recording. Under pristine code that costs nothing
    * in correctness terms — `run()` closes the writer and only then completes
    * the future `terminate()` waits on, so the `Closed` event is ordered before
    * `terminate()` can return no matter how long the close takes. It is what
    * turns "did close happen before terminate returned?" from a nanosecond race
    * into a decided question: an implementation that completed the future first
    * would hand the assertion a trace with no `Closed` in it.
    *
    * `flush()` records only when something has been written since the last
    * flush. The real writer's `flush()` is `Output.flush()`, a no-op on an empty
    * buffer, so a flush with nothing buffered changes no durability and is not
    * worth a trace entry — and leaving it out keeps the trace independent of how
    * the writer thread happens to split its drain batches.
    */
  class RecordingRecordWriter(recorder: Recorder)
      extends SequentialRecordWriter[ReplayLogRecord](
        new DataOutputStream(new ByteArrayOutputStream())
      ) {
    // Touched only by the writer thread (writeRecord/flush/close all run there).
    private var buffered = false

    override def writeRecord(obj: ReplayLogRecord): Unit = {
      buffered = true
      recorder.record(Wrote(obj))
    }

    override def flush(): Unit =
      if (buffered) {
        buffered = false
        recorder.record(Flushed)
      }

    override def close(): Unit = {
      Thread.sleep(CloseDelayMillis)
      recorder.record(Closed)
    }
  }

  /** See `RecordingRecordWriter.close()`. */
  val CloseDelayMillis = 250L

  /**
    * `AsyncReplayLogWriter.terminate()` blocks on an untimed
    * `CompletableFuture.get()` that is only completed at the very end of
    * `run()`. amber suites are strictly serial (`Tags.limit(Tags.Test, 1)`), so
    * a writer that never finishes would hang the whole module build rather than
    * fail one test. Run the shutdown on a daemon thread and bound the wait, so a
    * wedge surfaces as an ordinary failed assertion instead.
    *
    * The trace is captured on the shutdown thread the instant `shutdown()`
    * returns, so assertions see exactly what the writer had done by the time the
    * caller was released — not whatever it managed to do afterwards while the
    * test thread was getting around to reading.
    *
    * @return (whether the shutdown returned inside the budget, the trace as of
    *         the moment it returned).
    */
  def terminatesWithin(timeoutMillis: Long, recorder: Recorder)(
      shutdown: () => Unit
  ): (Boolean, List[WriterEvent]) = {
    val captured = new AtomicReference[List[WriterEvent]](Nil)
    val t = new Thread(() => {
      try {
        shutdown()
        captured.set(recorder.snapshot)
      } catch { case _: Throwable => () }
    })
    t.setDaemon(true)
    t.start()
    t.join(timeoutMillis)
    (!t.isAlive, captured.get())
  }
}

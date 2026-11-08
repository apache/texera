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

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.logreplay.ReplayLogWriterFixtures.{
  Closed,
  Flushed,
  Recorder,
  RecordingRecordWriter,
  Sent,
  WriterEvent,
  Wrote,
  terminatesWithin
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.scalatest.flatspec.AnyFlatSpec

class AsyncReplayLogWriterSpec extends AnyFlatSpec {

  private val channel =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private def fifo(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(channel, seq, DataFrame(Array.empty))

  private def output(seq: Long): Either[MainThreadDelegateMessage, WorkflowFIFOMessage] =
    Right(fifo(seq))

  /**
    * The other arm of the queue element's `Either`. `DPThread`,
    * `PrepareCheckpointHandler` and `FinalizeCheckpointHandler` all push
    * `Left(MainThreadDelegateMessage(...))` through this writer, so dropping one
    * would break checkpointing silently rather than loudly.
    */
  private def delegate(): Either[MainThreadDelegateMessage, WorkflowFIFOMessage] =
    Left(MainThreadDelegateMessage(_ => ()))

  private def newWriter(recorder: Recorder): AsyncReplayLogWriter = {
    val writer = new AsyncReplayLogWriter(recorder.handler, new RecordingRecordWriter(recorder))
    // Daemon so that even a wedged writer can never keep a JVM from exiting.
    writer.setDaemon(true)
    writer
  }

  private val shutdownBudgetMillis = 30000L

  /**
    * Slack allowed off a configured flush interval when asserting that the
    * writer really slept for it. `Thread.sleep` guarantees no upper bound and in
    * practice can undershoot its argument by a scheduling quantum.
    */
  private val sleepGranularityToleranceMillis = 50L

  /**
    * Shuts the writer down and returns the trace as of the instant
    * `terminate()` returned. Bounded, because `terminate()` blocks on an untimed
    * future (see `ReplayLogWriterFixtures.terminatesWithin`).
    */
  private def shutdownTrace(
      writer: AsyncReplayLogWriter,
      recorder: Recorder
  ): List[WriterEvent] = {
    val (returned, trace) =
      terminatesWithin(shutdownBudgetMillis, recorder)(() => writer.terminate())
    assert(returned, "terminate() did not return")
    trace
  }

  /**
    * `logInterval` is copied from `ApplicationConfig.faultToleranceLogFlushIntervalInMs`,
    * a memoised `val` on a shared object that must NOT be poked: amber suites run
    * serially in one JVM, so mutating it would poison every later suite. The writer
    * stores its own copy in a `private final long` *instance* field, and overwriting
    * that is instance-local. Done before `start()`, so `Thread.start()`'s
    * happens-before edge publishes the new value to the writer thread.
    */
  private def setLogInterval(writer: AsyncReplayLogWriter, millis: Long): Unit = {
    val field = classOf[AsyncReplayLogWriter].getDeclaredField("logInterval")
    field.setAccessible(true)
    field.setLong(writer, millis)
    assert(field.getLong(writer) == millis, "reflective logInterval override did not take effect")
  }

  /** A writer that has completed its full start/terminate lifecycle. */
  private def startedAndTerminated(): AsyncReplayLogWriter = {
    val recorder = new Recorder
    val writer = newWriter(recorder)
    writer.start()
    shutdownTrace(writer, recorder)
    writer
  }

  "AsyncReplayLogWriter" should
    "write and flush queued log records before releasing the queued outputs" in {
    // The class exists to guarantee this ordering: a message must not reach the
    // network before the log record that would let a replay reproduce it — and
    // "written" here has to mean flushed, not merely handed to the stream. Both
    // items are queued *before* start(), so the writer thread's first drainTo is
    // guaranteed to pick up both in one batch, which is exactly the batch in
    // which the ordering can be got wrong.
    val recorder = new Recorder
    val writer = newWriter(recorder)
    val step = ProcessingStep(channel, 0L)
    val released = output(1L)

    writer.putLogRecords(Array(step))
    writer.putOutput(released)
    writer.start()

    assert(shutdownTrace(writer, recorder) == List(Wrote(step), Flushed, Sent(released), Closed))
  }

  it should "write every queued record, in queue order, before the outputs that follow them" in {
    val recorder = new Recorder
    val writer = newWriter(recorder)
    val first = ProcessingStep(channel, 0L)
    val second = MessageContent(fifo(7L))
    val released = output(2L)

    writer.putLogRecords(Array(first, second))
    writer.putOutput(released)
    writer.start()

    assert(
      shutdownTrace(writer, recorder) ==
        List(Wrote(first), Wrote(second), Flushed, Sent(released), Closed)
    )
  }

  it should "release main-thread delegates as well as FIFO messages, in queue order" in {
    // The queue element is an Either precisely so that main-thread delegates
    // (checkpoint closures) ride the same ordering guarantee as network
    // messages. A writer that released only the Right arm would drop every
    // checkpoint closure while still writing its log records.
    val recorder = new Recorder
    val writer = newWriter(recorder)
    val step = ProcessingStep(channel, 0L)
    val closure = delegate()
    val released = output(3L)

    writer.putLogRecords(Array(step))
    writer.putOutput(closure)
    writer.putOutput(released)
    writer.start()

    assert(
      shutdownTrace(writer, recorder) ==
        List(Wrote(step), Flushed, Sent(closure), Sent(released), Closed)
    )
  }

  it should "close the underlying record writer exactly once when it shuts down" in {
    // terminate() is the only shutdown path, and it must both stop the thread
    // and close the record writer — a writer left open would leak the log file
    // handle for the rest of the worker's life. Asserting on the trace captured
    // the instant terminate() returned is what makes this an ordering claim
    // rather than a race: an implementation that released the caller before
    // closing is caught even though it does eventually close.
    val recorder = new Recorder
    val writer = newWriter(recorder)
    writer.start()

    assert(shutdownTrace(writer, recorder) == List(Closed))
  }

  it should "reject further log records once it has been terminated" in {
    val writer = startedAndTerminated()
    intercept[AssertionError] {
      writer.putLogRecords(Array(ProcessingStep(channel, 0L)))
    }
  }

  it should "reject further outputs once it has been terminated" in {
    val writer = startedAndTerminated()
    intercept[AssertionError] {
      writer.putOutput(output(1L))
    }
  }

  it should "wait for the configured flush interval before draining the queue" in {
    // With a positive faultToleranceLogFlushIntervalInMs the writer batches by
    // sleeping at the top of every drain loop. The default is 0 (no sleep), so
    // this arm is only reachable with a non-default interval.
    val recorder = new Recorder
    val writer = newWriter(recorder)
    val flushIntervalMillis = 300L
    setLogInterval(writer, flushIntervalMillis)
    val step = ProcessingStep(channel, 0L)
    writer.putLogRecords(Array(step))

    val startedAt = System.nanoTime()
    writer.start()
    assert(recorder.awaitFirstEvent(shutdownBudgetMillis), "the queued record was never written")
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000L

    assert(shutdownTrace(writer, recorder) == List(Wrote(step), Flushed, Closed))
    // Derived from the interval actually configured above rather than written out
    // as a literal, so the bound follows flushIntervalMillis if it is retuned.
    // The tolerance absorbs the platform's timer granularity: Thread.sleep is
    // allowed to return marginally early, and on Windows it routinely does.
    val lowerBoundMillis = flushIntervalMillis - sleepGranularityToleranceMillis
    assert(
      elapsedMillis >= lowerBoundMillis,
      s"the first flush landed after only ${elapsedMillis}ms, under the ${lowerBoundMillis}ms " +
        s"floor implied by the ${flushIntervalMillis}ms flush interval, so the interval was " +
        "not honoured"
    )
  }
}

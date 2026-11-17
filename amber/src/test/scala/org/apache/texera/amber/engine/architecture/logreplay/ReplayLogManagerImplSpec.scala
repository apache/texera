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

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
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

/**
  * Covers `ReplayLogManagerImpl`, the fault-tolerant implementation.
  * `EmptyReplayLogManagerImpl` (the no-op one) and the trait's own default
  * `withFaultTolerant` are covered by `EmptyReplayLogManagerImplSpec`.
  *
  * Step numbers are written as literals rather than as
  * `ProcessingStepCursor.INIT_STEP (+ 1)`. The step recorded in the log is what
  * a re-executed worker replays up to, so it has to be pinned against something
  * that does not move when the production constant moves: both
  * `ProcessingStepCursor.currentStepCounter` and `ReplayLogger.lastStep` are
  * seeded from `INIT_STEP`, so writing it on the expected side would let the two
  * sides slide together and pin nothing.
  */
class ReplayLogManagerImplSpec extends AnyFlatSpec {

  private val channel =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private def fifo(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(channel, seq, DataFrame(Array.empty))

  private def output(seq: Long): Either[MainThreadDelegateMessage, WorkflowFIFOMessage] =
    Right(fifo(seq))

  /** The main-thread-delegate arm of the output `Either`; see `DPThread` line 113. */
  private def delegate(): Either[MainThreadDelegateMessage, WorkflowFIFOMessage] =
    Left(MainThreadDelegateMessage(_ => ()))

  private val shutdownBudgetMillis = 30000L

  /**
    * Runs `body` against a live manager, then shuts the writer thread down and
    * returns everything the writer observed, in order, as of the instant the
    * shutdown returned. `setupWriter` starts a real `AsyncReplayLogWriter`
    * thread, and its `terminate()` blocks on an untimed future, so the shutdown
    * is bounded (see `terminatesWithin`).
    *
    * `Flushed` is filtered out here, and only here. Unlike
    * `AsyncReplayLogWriterSpec` — which queues everything before `start()` and
    * therefore gets one deterministic drain batch — the manager enqueues into an
    * already-running writer, so where the writer chooses to split its batches
    * (and hence how many flushes it performs) is genuinely racy. The
    * write-flush-then-release ordering is pinned in `AsyncReplayLogWriterSpec`,
    * where it is deterministic; what this spec pins is which records the manager
    * produces and in what order.
    */
  private def drainedEvents(body: ReplayLogManagerImpl => Unit): List[WriterEvent] = {
    val recorder = new Recorder
    val manager = new ReplayLogManagerImpl(recorder.handler)
    manager.setupWriter(new RecordingRecordWriter(recorder))
    var trace = List.empty[WriterEvent]
    try body(manager)
    finally {
      val (returned, captured) =
        terminatesWithin(shutdownBudgetMillis, recorder)(() => manager.terminate())
      assert(returned, "ReplayLogManagerImpl.terminate() did not return")
      trace = captured
    }
    trace.filterNot(_ == Flushed)
  }

  "ReplayLogManagerImpl.markAsReplayDestination" should
    "persist a ReplayDestination record carrying the given ECM id" in {
    // The replay planner scans the log for ReplayDestination markers to decide
    // where a re-executed worker should stop, so both the record *type* and the
    // id it carries are load-bearing.
    val ecmId = EmbeddedControlMessageIdentity("ecm-1")
    val released = output(1L)

    val events = drainedEvents { manager =>
      manager.markAsReplayDestination(ecmId)
      manager.sendCommitted(released)
    }

    assert(events == List(Wrote(ReplayDestination(ecmId)), Sent(released), Closed))
  }

  it should "keep one marker per call, in call order, when several destinations are marked" in {
    val first = EmbeddedControlMessageIdentity("ecm-1")
    val second = EmbeddedControlMessageIdentity("ecm-2")
    val released = output(1L)

    val events = drainedEvents { manager =>
      manager.markAsReplayDestination(first)
      manager.markAsReplayDestination(second)
      manager.sendCommitted(released)
    }

    assert(
      events == List(
        Wrote(ReplayDestination(first)),
        Wrote(ReplayDestination(second)),
        Sent(released),
        Closed
      )
    )
  }

  "ReplayLogManagerImpl.sendCommitted" should
    "persist the pending processing steps before releasing the message" in {
    // withFaultTolerant records the step/message pair; sendCommitted must flush
    // those records to the log *before* the output is handed to the handler.
    // The absolute step values matter: -1 is the "nothing processed yet" cursor
    // and 0 is the first processed message, which is where a replay resumes.
    val processed = fifo(7L)
    val released = output(9L)

    val events = drainedEvents { manager =>
      manager.withFaultTolerant(channel, Some(processed)) {}
      manager.sendCommitted(released)
    }

    assert(
      events == List(
        Wrote(ProcessingStep(channel, -1L)),
        Wrote(MessageContent(processed)),
        Wrote(ProcessingStep(channel, 0L)),
        Sent(released),
        Closed
      )
    )
  }

  it should "release a message with no log records when nothing has been processed" in {
    val released = output(1L)
    val events = drainedEvents(_.sendCommitted(released))
    assert(events == List(Sent(released), Closed))
  }

  it should "release a main-thread delegate through the same committed path" in {
    // Checkpoint closures reach the handler as Left(MainThreadDelegateMessage);
    // WorkflowWorker wires sendCommitted up as the outputHandler for both arms,
    // so a manager that only forwarded Rights would silently drop checkpoints.
    val processed = fifo(2L)
    val closure = delegate()

    val events = drainedEvents { manager =>
      manager.withFaultTolerant(channel, Some(processed)) {}
      manager.sendCommitted(closure)
    }

    assert(
      events == List(
        Wrote(ProcessingStep(channel, -1L)),
        Wrote(MessageContent(processed)),
        Wrote(ProcessingStep(channel, 0L)),
        Sent(closure),
        Closed
      )
    )
  }

  "ReplayLogManagerImpl.withFaultTolerant" should
    "still persist the in-flight step after the processing body throws" in {
    // The override records the step *before* delegating to the trait, which is
    // what makes a crashed worker replayable up to the message that killed it.
    // Losing the record here would silently shorten every recovery.
    val processed = fifo(3L)
    val released = output(4L)

    val events = drainedEvents { manager =>
      intercept[RuntimeException] {
        manager.withFaultTolerant(channel, Some(processed)) {
          throw new RuntimeException("boom")
        }
      }
      assert(manager.getStep == 0L, "the cursor must advance even when the body throws")
      manager.sendCommitted(released)
    }

    assert(
      events == List(
        Wrote(ProcessingStep(channel, -1L)),
        Wrote(MessageContent(processed)),
        Wrote(ProcessingStep(channel, 0L)),
        Sent(released),
        Closed
      )
    )
  }
}

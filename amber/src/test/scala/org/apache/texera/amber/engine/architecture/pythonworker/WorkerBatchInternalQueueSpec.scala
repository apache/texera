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

package org.apache.texera.amber.engine.architecture.pythonworker

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue._
import org.apache.texera.amber.engine.common.actormessage.ActorCommand
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  DirectControlMessagePayload,
  StateFrame
}
import org.scalatest.flatspec.AnyFlatSpec

class WorkerBatchInternalQueueSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test harness — WorkerBatchInternalQueue is a trait. Mix it into an
  // otherwise-empty class so we can drive its public surface directly.
  // ---------------------------------------------------------------------------

  private class TestQueue extends WorkerBatchInternalQueue

  private def channelId(from: String, to: String): ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity(from), ActorVirtualIdentity(to), isControl = false)

  // A one-attribute schema is enough to build real Tuple instances for
  // DataFrame byte-accounting tests.
  private val intAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(intAttr)
  private def tuple(value: Int): Tuple =
    Tuple.builder(schema).add(intAttr, Integer.valueOf(value)).build()

  // Marker DirectControlMessagePayload for ControlElement tests. The
  // trait is empty (just a marker), so any concrete value works.
  private case object DummyCtrl extends DirectControlMessagePayload

  // ---------------------------------------------------------------------------
  // enqueue + getElement — type-preserving round-trip
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue.enqueueData + getElement" should
    "round-trip a DataElement carrying a DataFrame" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    val frame = DataFrame(Array(tuple(1), tuple(2)))
    q.enqueueData(DataElement(frame, cid))
    val out = q.getElement
    assert(out == DataElement(frame, cid))
  }

  it should "round-trip a DataElement carrying a non-DataFrame DataPayload (e.g. StateFrame)" in {
    // The byte-accounting path is `case frame: DataFrame =>`; a non-
    // DataFrame DataPayload (StateFrame) must still pass through
    // enqueue/dequeue cleanly even though credit is not updated.
    val q = new TestQueue
    val cid = channelId("up", "self")
    val state = StateFrame(org.apache.texera.amber.core.state.State(Map.empty))
    q.enqueueData(DataElement(state, cid))
    val out = q.getElement
    assert(out == DataElement(state, cid))
  }

  "WorkerBatchInternalQueue.enqueueCommand + getElement" should
    "round-trip a ControlElement" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    q.enqueueCommand(DummyCtrl, cid)
    val out = q.getElement
    assert(out == ControlElement(DummyCtrl, cid))
  }

  "WorkerBatchInternalQueue.enqueueActorCommand + getElement" should
    "round-trip an ActorCommandElement (control-priority lane)" in {
    val q = new TestQueue
    q.enqueueActorCommand(ActorCommand.Empty)
    val out = q.getElement
    assert(out == ActorCommandElement(ActorCommand.Empty))
  }

  // ---------------------------------------------------------------------------
  // Multi-priority dispatch — control queue (priority 0) wins over data (1)
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue.getElement" should
    "drain the control queue before the data queue (lower numeric priority wins)" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    // Enqueue data FIRST, then control — getElement must still return
    // control first because its sub-queue has priority 0 < 1.
    q.enqueueData(DataElement(DataFrame(Array(tuple(1))), cid))
    q.enqueueCommand(DummyCtrl, cid)
    val first = q.getElement
    val second = q.getElement
    assert(first.isInstanceOf[ControlElement], s"expected control first, got $first")
    assert(second.isInstanceOf[DataElement], s"expected data second, got $second")
  }

  it should "interleave actor-command and direct-control items as they were enqueued" in {
    // Both ActorCommandElement and ControlElement go into the control
    // queue (priority 0). Within a single queue, FIFO order applies.
    val q = new TestQueue
    val cid = channelId("up", "self")
    q.enqueueCommand(DummyCtrl, cid)
    q.enqueueActorCommand(ActorCommand.Empty)
    val first = q.getElement
    val second = q.getElement
    assert(first == ControlElement(DummyCtrl, cid))
    assert(second == ActorCommandElement(ActorCommand.Empty))
  }

  // ---------------------------------------------------------------------------
  // Queue-length accounting
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue.getDataQueueLength" should
    "track only data-queue items (excludes control)" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    assert(q.getDataQueueLength == 0)
    q.enqueueData(DataElement(DataFrame(Array.empty), cid))
    q.enqueueData(DataElement(DataFrame(Array.empty), cid))
    // Control items must NOT inflate the data-queue count.
    q.enqueueCommand(DummyCtrl, cid)
    assert(q.getDataQueueLength == 2)
  }

  "WorkerBatchInternalQueue.getControlQueueLength" should
    "track every control-queue item (ControlElement + ActorCommandElement)" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    assert(q.getControlQueueLength == 0)
    q.enqueueCommand(DummyCtrl, cid)
    q.enqueueActorCommand(ActorCommand.Empty)
    q.enqueueData(DataElement(DataFrame(Array.empty), cid))
    assert(q.getControlQueueLength == 2)
  }

  "WorkerBatchInternalQueue.isControlQueueEmpty" should
    "be true initially and after every control item is consumed" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    assert(q.isControlQueueEmpty)
    q.enqueueCommand(DummyCtrl, cid)
    assert(!q.isControlQueueEmpty)
    q.getElement // drains the control item
    assert(q.isControlQueueEmpty)
  }

  // ---------------------------------------------------------------------------
  // disable / enable data queue — control flow keeps moving
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue.disableDataQueue" should
    "keep enqueued data invisible to getElement until enableDataQueue is called" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    q.enqueueData(DataElement(DataFrame(Array(tuple(1))), cid))
    q.disableDataQueue()
    // With the data queue disabled, the only enqueued item is data —
    // queue an inert control item, getElement must yield the control
    // item even though data was enqueued FIRST.
    q.enqueueCommand(DummyCtrl, cid)
    val out = q.getElement
    assert(out.isInstanceOf[ControlElement], s"expected control while data is disabled, got $out")
    // Re-enable; the originally-enqueued data is now reachable.
    q.enableDataQueue()
    val nextOut = q.getElement
    assert(nextOut.isInstanceOf[DataElement])
  }

  // ---------------------------------------------------------------------------
  // getQueuedCredit — byte-accounting for DataFrame payloads
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue.getQueuedCredit" should
    "report 0 for any sender when nothing has been enqueued" in {
    val q = new TestQueue
    assert(q.getQueuedCredit(channelId("a", "b")) == 0L)
    assert(q.getQueuedCredit(channelId("c", "d")) == 0L)
  }

  it should "track bytes-in minus bytes-out for DataFrame payloads per sender" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    val frame = DataFrame(Array(tuple(1), tuple(2), tuple(3)))
    q.enqueueData(DataElement(frame, cid))
    // After enqueue, bytes-in > 0, bytes-out == 0 → credit positive.
    val creditAfterEnqueue = q.getQueuedCredit(cid)
    assert(creditAfterEnqueue == frame.inMemSize)
    // After getElement, bytes-out catches up → credit drops to 0.
    q.getElement
    assert(q.getQueuedCredit(cid) == 0L)
  }

  it should "NOT increment credit for ControlElement payloads (control bytes are untracked)" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    q.enqueueCommand(DummyCtrl, cid)
    assert(q.getQueuedCredit(cid) == 0L, "control payloads should not affect the credit counter")
  }

  it should "NOT increment credit for non-DataFrame DataPayloads (e.g. StateFrame)" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    val state = StateFrame(org.apache.texera.amber.core.state.State(Map.empty))
    q.enqueueData(DataElement(state, cid))
    assert(q.getQueuedCredit(cid) == 0L, "StateFrame payloads should not affect the credit counter")
  }

  it should "track each sender's credit independently (no cross-sender accumulation)" in {
    val q = new TestQueue
    val s1 = channelId("up-1", "self")
    val s2 = channelId("up-2", "self")
    val f1 = DataFrame(Array(tuple(10)))
    val f2 = DataFrame(Array(tuple(20), tuple(30)))
    q.enqueueData(DataElement(f1, s1))
    q.enqueueData(DataElement(f2, s2))
    assert(q.getQueuedCredit(s1) == f1.inMemSize)
    assert(q.getQueuedCredit(s2) == f2.inMemSize)
    // Drain only s1 by reading the first item (FIFO within data queue).
    q.getElement // s1's frame
    assert(q.getQueuedCredit(s1) == 0L)
    assert(
      q.getQueuedCredit(s2) == f2.inMemSize,
      "s2's credit should still reflect un-consumed data"
    )
  }

  it should "accumulate credit across multiple enqueues for the same sender" in {
    val q = new TestQueue
    val cid = channelId("up", "self")
    val f1 = DataFrame(Array(tuple(1)))
    val f2 = DataFrame(Array(tuple(2)))
    q.enqueueData(DataElement(f1, cid))
    q.enqueueData(DataElement(f2, cid))
    assert(q.getQueuedCredit(cid) == f1.inMemSize + f2.inMemSize)
  }

  // ---------------------------------------------------------------------------
  // Companion constants
  // ---------------------------------------------------------------------------

  "WorkerBatchInternalQueue companion" should
    "define stable queue-priority constants (CONTROL_QUEUE < DATA_QUEUE)" in {
    // Pin the numeric ordering — control must have a lower (higher-priority)
    // queue id than data. The multi-priority semantics in `getElement` rely
    // on this. A reversal would silently change dispatch behavior.
    assert(WorkerBatchInternalQueue.CONTROL_QUEUE == 0)
    assert(WorkerBatchInternalQueue.DATA_QUEUE == 1)
    assert(WorkerBatchInternalQueue.CONTROL_QUEUE < WorkerBatchInternalQueue.DATA_QUEUE)
  }
}

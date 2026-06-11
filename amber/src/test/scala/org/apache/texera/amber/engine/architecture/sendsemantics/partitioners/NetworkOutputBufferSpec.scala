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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.texera.amber.config.ApplicationConfig
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  StateFrame,
  WorkflowFIFOMessage
}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

class NetworkOutputBufferSpec extends AnyFlatSpec {

  // --- fixtures --------------------------------------------------------------

  private val sender = ActorVirtualIdentity("sender")
  private val receiver = ActorVirtualIdentity("receiver-1")

  private val intAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(intAttr)
  private def tuple(value: Int): Tuple =
    Tuple.builder(schema).add(intAttr, value).build()

  /** Recording wrapper around a real `NetworkOutputGateway`. */
  private class Capture {
    val messages: ArrayBuffer[WorkflowFIFOMessage] = ArrayBuffer.empty
    val gateway: NetworkOutputGateway =
      new NetworkOutputGateway(sender, m => messages += m)
  }

  private def newBuffer(batchSize: Int = 4): (NetworkOutputBuffer, Capture) = {
    val cap = new Capture
    val buf = new NetworkOutputBuffer(receiver, cap.gateway, batchSize = batchSize)
    (buf, cap)
  }

  // --- construction defaults -------------------------------------------------

  "NetworkOutputBuffer" should "default batchSize to ApplicationConfig.defaultDataTransferBatchSize" in {
    val cap = new Capture
    val buf = new NetworkOutputBuffer(receiver, cap.gateway)
    assert(buf.batchSize == ApplicationConfig.defaultDataTransferBatchSize)
  }

  it should "expose `to` and `dataOutputPort` as immutable accessors" in {
    val cap = new Capture
    val buf = new NetworkOutputBuffer(receiver, cap.gateway, batchSize = 4)
    assert(buf.to == receiver)
    assert(buf.dataOutputPort eq cap.gateway)
  }

  it should "start with an empty buffer (no implicit auto-flush at construction)" in {
    val (_, cap) = newBuffer()
    assert(cap.messages.isEmpty)
  }

  // --- addTuple buffering / auto-flush --------------------------------------

  "NetworkOutputBuffer.addTuple" should "NOT flush while the buffer is below batchSize" in {
    val (buf, cap) = newBuffer(batchSize = 4)
    buf.addTuple(tuple(0))
    buf.addTuple(tuple(1))
    buf.addTuple(tuple(2))
    assert(cap.messages.isEmpty, "no DataFrame should be sent until batchSize is reached")
  }

  it should "auto-flush when the buffer exactly reaches batchSize" in {
    val (buf, cap) = newBuffer(batchSize = 3)
    buf.addTuple(tuple(0))
    buf.addTuple(tuple(1))
    buf.addTuple(tuple(2)) // boundary: now size == batchSize
    assert(cap.messages.size == 1, "exactly one DataFrame should be auto-flushed at the boundary")
    val frame = cap.messages.head.payload.asInstanceOf[DataFrame]
    assert(frame.frame.toList == List(tuple(0), tuple(1), tuple(2)))
  }

  it should "produce a separate DataFrame for each successive batch" in {
    val (buf, cap) = newBuffer(batchSize = 2)
    (0 until 6).foreach(i => buf.addTuple(tuple(i)))
    assert(cap.messages.size == 3, "three full batches → three DataFrames")
    val payloads = cap.messages.map(_.payload.asInstanceOf[DataFrame].frame.toList)
    assert(payloads.head == List(tuple(0), tuple(1)))
    assert(payloads(1) == List(tuple(2), tuple(3)))
    assert(payloads(2) == List(tuple(4), tuple(5)))
  }

  it should "send DataFrames to the configured receiver only" in {
    val (buf, cap) = newBuffer(batchSize = 2)
    buf.addTuple(tuple(0))
    buf.addTuple(tuple(1))
    assert(cap.messages.size == 1)
    val msg = cap.messages.head
    assert(msg.channelId.fromWorkerId == sender)
    assert(msg.channelId.toWorkerId == receiver)
    assert(!msg.channelId.isControl, "data path must not use the control channel")
  }

  // --- flush() ----------------------------------------------------------------

  "NetworkOutputBuffer.flush" should "send a DataFrame and reset the buffer when the buffer is non-empty" in {
    val (buf, cap) = newBuffer(batchSize = 100) // never auto-flushes
    buf.addTuple(tuple(7))
    buf.addTuple(tuple(8))
    buf.flush()
    assert(cap.messages.size == 1)
    val frame = cap.messages.head.payload.asInstanceOf[DataFrame]
    assert(frame.frame.toList == List(tuple(7), tuple(8)))
    // A second flush() with nothing buffered must not send another frame.
    buf.flush()
    assert(cap.messages.size == 1, "flush() on an empty buffer must be a no-op")
  }

  it should "be a no-op when called on an empty buffer (no DataFrame, no StateFrame)" in {
    val (buf, cap) = newBuffer()
    buf.flush()
    buf.flush()
    buf.flush()
    assert(cap.messages.isEmpty)
  }

  it should "assign monotonically increasing sequence numbers across multiple flushes" in {
    // The gateway tracks sequence numbers per channel; each successive
    // DataFrame on the same channel gets the next number. Pin so a
    // regression that resets seq on flush is visible.
    val (buf, cap) = newBuffer(batchSize = 1) // each addTuple flushes
    (0 until 4).foreach(i => buf.addTuple(tuple(i)))
    val seqs = cap.messages.map(_.sequenceNumber).toList
    assert(seqs == List(0L, 1L, 2L, 3L), s"unexpected sequence: $seqs")
  }

  // --- sendState ----------------------------------------------------------

  "NetworkOutputBuffer.sendState" should "flush pending tuples FIRST, then send the StateFrame" in {
    val (buf, cap) = newBuffer(batchSize = 100)
    buf.addTuple(tuple(0))
    buf.addTuple(tuple(1))
    val state = State(Map("checkpoint" -> 99))
    buf.sendState(state)
    // Expected order: DataFrame (the buffered tuples) → StateFrame.
    assert(cap.messages.size == 2)
    val first = cap.messages.head.payload
    val second = cap.messages(1).payload
    assert(first.isInstanceOf[DataFrame], s"first frame should be DataFrame, got $first")
    assert(first.asInstanceOf[DataFrame].frame.toList == List(tuple(0), tuple(1)))
    assert(second == StateFrame(state))
  }

  it should "send only the StateFrame when no tuples are pending (empty pre-flush is a no-op)" in {
    val (buf, cap) = newBuffer()
    val state = State(Map("k" -> "v"))
    buf.sendState(state)
    assert(cap.messages.size == 1)
    assert(cap.messages.head.payload == StateFrame(state))
  }

  it should "leave the tuple buffer empty after sendState (trailing flush no-op)" in {
    // sendState calls flush() AFTER sending the state too. Pin that the
    // trailing flush doesn't double-send and that subsequent addTuple
    // starts from a clean buffer.
    val (buf, cap) = newBuffer(batchSize = 100)
    buf.addTuple(tuple(0))
    buf.sendState(State(Map.empty))
    val countBefore = cap.messages.size // DataFrame + StateFrame = 2
    assert(countBefore == 2)
    // Add another tuple and explicit flush — must produce one fresh frame.
    buf.addTuple(tuple(99))
    buf.flush()
    assert(cap.messages.size == 3)
    val third = cap.messages(2).payload.asInstanceOf[DataFrame]
    assert(third.frame.toList == List(tuple(99)), "post-state buffer must start empty")
  }

  it should "share a single sequence-number stream across DataFrames and the StateFrame on the same channel" in {
    // Pin: DataFrame and StateFrame go through the same `sendTo` path on
    // the same channel, so they share the gateway's sequence-number
    // counter. A regression that opens a side-channel for StateFrame
    // would produce a non-monotonic stream and fail this.
    val (buf, cap) = newBuffer(batchSize = 100)
    buf.addTuple(tuple(0))
    buf.addTuple(tuple(1))
    buf.sendState(State(Map("x" -> 1)))
    buf.addTuple(tuple(2))
    buf.flush()
    val seqs = cap.messages.map(_.sequenceNumber).toList
    assert(seqs == List(0L, 1L, 2L), s"unexpected sequence: $seqs")
  }

  // --- batchSize edge cases -------------------------------------------------

  "NetworkOutputBuffer with batchSize = 1" should "flush immediately after every addTuple" in {
    val (buf, cap) = newBuffer(batchSize = 1)
    buf.addTuple(tuple(0))
    assert(cap.messages.size == 1)
    buf.addTuple(tuple(1))
    assert(cap.messages.size == 2)
    val frames = cap.messages.toList.map(_.payload.asInstanceOf[DataFrame].frame.toList)
    assert(frames == List(List(tuple(0)), List(tuple(1))))
  }

  // `batchSize <= 0` IS reachable from production today: the
  // workflow-settings UI restricts the value to `>= 1`, but
  // `SyncExecutionResource` accepts `request.workflowSettings` directly
  // from the API and the backend forwards `workflowSettings
  // .dataTransferBatchSize` into `NetworkOutputBuffer` without
  // validating it. The reachable path is covered by a characterization
  // test (current lenient `>=` behavior — flush every tuple) plus a
  // pendingUntilFixed test pinning the desired hardening (rejection
  // at construction). When the hardening lands the characterization
  // test breaks on purpose AND pendingUntilFixed flips into a
  // deliberate failure forcing both markers to be updated together.

  "NetworkOutputBuffer with non-positive batchSize" should
    "currently flush per-tuple under the `>=` guard (characterization, today's lenient behavior)" in {
    // Pin the current observable behavior for the reachable-from-API
    // `batchSize <= 0` path so a regression that breaks per-tuple
    // flush (e.g. a partial change that disables flushing entirely
    // for non-positive batch sizes) surfaces here. A future hardening
    // that rejects `<= 0` at construction WILL break this test on
    // purpose — and the pendingUntilFixed test below will flip into
    // a deliberate failure at the same time, forcing both markers to
    // be updated together.
    val (buf0, cap0) = newBuffer(batchSize = 0)
    buf0.addTuple(tuple(1))
    buf0.addTuple(tuple(2))
    val frames0 = cap0.messages.toList.map(_.payload.asInstanceOf[DataFrame].frame.toList)
    assert(frames0 == List(List(tuple(1)), List(tuple(2))))

    val (bufNeg, capNeg) = newBuffer(batchSize = -1)
    bufNeg.addTuple(tuple(99))
    val framesNeg = capNeg.messages.toList.map(_.payload.asInstanceOf[DataFrame].frame.toList)
    assert(framesNeg == List(List(tuple(99))))
  }

  it should "eventually reject construction (pendingUntilFixed)" in pendingUntilFixed {
    // Today the constructor accepts `batchSize <= 0` and the `>=`
    // guard then fires after every append (the characterization
    // above pins that behavior). The intended contract is that a
    // non-positive batch size is invalid input and should be
    // rejected at construction (e.g. `require(batchSize > 0, ...)`).
    // Asserting `IllegalArgumentException` here flips this from
    // pending to passing once the hardening lands.
    val cap = new Capture
    intercept[IllegalArgumentException] {
      new NetworkOutputBuffer(receiver, cap.gateway, batchSize = 0)
    }
    intercept[IllegalArgumentException] {
      new NetworkOutputBuffer(receiver, cap.gateway, batchSize = -1)
    }
  }
}

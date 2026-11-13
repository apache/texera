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

package org.apache.texera.amber.engine.architecture.messaginglayer

import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload,
  WorkflowMessage
}
import org.scalatest.flatspec.AnyFlatSpec

class FlowControlSpec extends AnyFlatSpec {

  private val channelId =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  // A non-DataFrame payload so that `WorkflowMessage.getInMemSize` falls through to
  // the 200L default branch — using DataFrame(Array.empty) yields 0 bytes, which
  // would let any message squeeze through even when the configured credit is 0.
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload

  private def msg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, id, FixedSizePayload()))

  // Pin the assumed payload size so the test fails loudly if WorkflowMessage's
  // size accounting changes in a way that would invalidate the credit math below.
  private val msgSize: Long = WorkflowMessage.getInMemSize(msg(0).internalMessage)
  assert(msgSize == 200L)

  private val maxBytes = ApplicationConfig.maxCreditAllowedInBytesPerChannel

  // One-field schema, used only to build the oversized DataFrame payload that
  // trips the size-cap guard.
  private val payloadAttr = new Attribute("payload", AttributeType.STRING)
  private val payloadSchema: Schema = Schema().add(payloadAttr)

  "FlowControl" should "report full credit and not be overloaded initially" in {
    val fc = new FlowControl()
    assert(fc.getCredit == maxBytes)
    assert(!fc.isOverloaded)
  }

  "FlowControl.getMessagesToSend" should "forward an incoming message when credit is available" in {
    val fc = new FlowControl()
    val out = fc.getMessagesToSend(msg(1L)).toList
    assert(out == List(msg(1L)))
    assert(!fc.isOverloaded)
  }

  it should "stash an incoming message and become overloaded when credit is exhausted" in {
    val fc = new FlowControl()
    // exhaust the receiver-side credit so getCredit drops to 0
    fc.updateQueuedCredit(maxBytes)
    assert(fc.getCredit == 0L)

    val out = fc.getMessagesToSend(msg(1L)).toList
    assert(out.isEmpty)
    assert(fc.isOverloaded)
  }

  it should "drain stashed messages once credit is restored" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    val firstAttempt = fc.getMessagesToSend(msg(1L)).toList
    assert(firstAttempt.isEmpty)
    assert(fc.isOverloaded)

    fc.updateQueuedCredit(0L)
    val drained = fc.getMessagesToSend.toList
    assert(drained == List(msg(1L)))
    assert(!fc.isOverloaded)
  }

  it should "force new messages through the stash whenever the stash is non-empty" in {
    // While the stash is non-empty, even a new message must be stashed first
    // and then drained in FIFO order — never sent ahead of older stashed work.
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    fc.getMessagesToSend(msg(1L)) // stash msg(1L)
    assert(fc.isOverloaded)

    // Restore enough credit for 2 messages, then push a new one. The branch
    // under test always stashes the new message and then drains FIFO.
    fc.updateQueuedCredit(maxBytes - 2 * msgSize)
    val drained = fc.getMessagesToSend(msg(2L)).toList
    assert(drained == List(msg(1L), msg(2L)))
    assert(!fc.isOverloaded)
  }

  it should "leave isOverloaded true when only some stashed messages can be drained" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(maxBytes)
    fc.getMessagesToSend(msg(1L))
    fc.getMessagesToSend(msg(2L))
    assert(fc.isOverloaded)

    // Restore credit for exactly one message; the second remains stashed.
    fc.updateQueuedCredit(maxBytes - msgSize)
    val drained = fc.getMessagesToSend.toList
    assert(drained == List(msg(1L)))
    assert(fc.isOverloaded, "stash still has msg(2L), so overloaded must remain true")
  }

  // The fast path below the size-cap guard: while every message stays under the
  // cap, each one is forwarded immediately rather than stashed, is charged exactly
  // its own size against the credit, and never flips the overloaded flag. Running
  // a whole batch rather than a single message is what catches accounting that is
  // right for the first message and then drifts. (The guard itself is covered by
  // "reject a message larger than the whole credit cap" further down.)
  it should "forward every under-cap message and charge its size against the credit" in {
    val batch = 1000L
    // Fixture precondition: the whole batch must fit under the cap, otherwise the
    // expectations below would be describing the stashing path instead.
    assert(
      batch * msgSize < maxBytes,
      s"fixture no longer exercises the fast path: $batch * $msgSize >= $maxBytes"
    )

    val fc = new FlowControl()
    (1L to batch).foreach { i =>
      val out = fc.getMessagesToSend(msg(i)).toList
      assert(out == List(msg(i)), s"message $i must be forwarded, not stashed")
      assert(
        fc.getCredit == maxBytes - i * msgSize,
        s"after $i forwarded messages the credit must be down by $i * $msgSize"
      )
      assert(!fc.isOverloaded, s"must not be overloaded after $i under-cap messages")
    }
  }

  "FlowControl.updateQueuedCredit" should "shrink the available credit" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(100L)
    assert(fc.getCredit == maxBytes - 100L)
  }

  it should "be relative to the latest call (not cumulative)" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(100L)
    fc.updateQueuedCredit(50L)
    assert(fc.getCredit == maxBytes - 50L)
  }

  "FlowControl.decreaseInflightCredit" should "free credit equal to the acked amount" in {
    val fc = new FlowControl()

    // Send a message through to seed `inflightCredit` with the actual size used
    // by FlowControl's accounting. This avoids passing an invalid (negative)
    // amount to `decreaseInflightCredit`.
    fc.getMessagesToSend(msg(1L)).toList
    assert(fc.getCredit == maxBytes - msgSize)

    fc.decreaseInflightCredit(msgSize)
    assert(fc.getCredit == maxBytes)
  }

  // ---------------------------------------------------------------------------
  // Edge / invalid-input cases — credit math under abnormal conditions
  // ---------------------------------------------------------------------------

  // The size-cap guard at the top of getMessagesToSend. A payload larger than the
  // entire per-channel cap could never be sent, so FlowControl rejects it outright
  // rather than stashing it forever. Reaching a multi-GB reported size costs
  // almost no memory: DataFrame.inMemSize sums Tuple.inMemSize across its array
  // without deduplicating, so an array holding the same tuple reference many
  // times reports an oversized payload while allocating one tuple and N pointers.
  "FlowControl.getMessagesToSend" should "reject a message larger than the whole credit cap" in {
    val bigTuple = Tuple.builder(payloadSchema).add(payloadAttr, "x" * 100000).build()
    val copies = (maxBytes / bigTuple.inMemSize + 1).toInt
    val oversized = NetworkMessage(
      1L,
      WorkflowFIFOMessage(channelId, 1L, DataFrame(Array.fill(copies)(bigTuple)))
    )
    // Fixture precondition: the payload really is over the cap, so the guard is
    // the thing under test rather than the out-of-credit branch below it.
    val reportedSize = WorkflowMessage.getInMemSize(oversized.internalMessage)
    assert(
      reportedSize > maxBytes,
      s"fixture payload is not oversized: $reportedSize <= $maxBytes"
    )

    val fc = new FlowControl()
    val thrown = intercept[AssertionError](fc.getMessagesToSend(oversized))
    assert(thrown.getMessage.contains("too big to send through flow control"))
    // The rejection must leave the channel untouched — not half-charged, and not
    // marked overloaded as the out-of-credit branch would have done.
    assert(fc.getCredit == maxBytes)
    assert(!fc.isOverloaded)
  }

  "FlowControl" should "eventually drain the stash across many ack cycles (multi-run)" in {
    val fc = new FlowControl()
    // Saturate credit and stash a batch of messages.
    fc.updateQueuedCredit(maxBytes)
    val stashed = (1L to 20L).map { i =>
      fc.getMessagesToSend(msg(i))
      i
    }
    assert(fc.isOverloaded)

    // Now alternately restore credit one message at a time and drain.
    var seen = 0L
    stashed.foreach { _ =>
      fc.updateQueuedCredit(maxBytes - msgSize) // 1 message worth of credit
      val out = fc.getMessagesToSend.toList
      assert(out.size == 1)
      seen += 1
      // Reset queued back to maxBytes so inflight is the only buffer
      fc.decreaseInflightCredit(msgSize)
      fc.updateQueuedCredit(maxBytes)
    }
    assert(seen == stashed.size)
  }

  "FlowControl.updateQueuedCredit" should "accept a zero queued credit (reset back to full)" in {
    val fc = new FlowControl()
    fc.updateQueuedCredit(100L)
    fc.updateQueuedCredit(0L)
    assert(fc.getCredit == maxBytes)
  }

  it should "accept a negative queued credit (overshoot, increasing visible credit)" in {
    // FlowControl performs no validation on queuedCredit; a negative input
    // simply increases getCredit. Pin this so a future input validator
    // surfaces as a test failure.
    val fc = new FlowControl()
    fc.updateQueuedCredit(-100L)
    assert(fc.getCredit == maxBytes - (-100L))
  }

  "FlowControl.decreaseInflightCredit" should "be a tolerated no-op for amount = 0" in {
    val fc = new FlowControl()
    fc.decreaseInflightCredit(0L)
    assert(fc.getCredit == maxBytes)
  }
}

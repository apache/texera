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

import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.scalatest.flatspec.AnyFlatSpec

class CongestionControlSpec extends AnyFlatSpec {

  private val channelId =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private def msg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, id, DataFrame(Array.empty)))

  // Backdate `sentTime` for `id` so the timeout branches (ack > ackTimeLimit
  // and getTimedOutInTransitMessages > resendTimeLimit) become reachable
  // without sleeping. The field is `private val sentTime: LongMap[Long]`,
  // accessed via Java reflection on the instance's backing field.
  private def backdateSentTime(cc: CongestionControl, id: Long, ageMillis: Long): Unit = {
    val field = classOf[CongestionControl].getDeclaredField("sentTime")
    field.setAccessible(true)
    val map = field.get(cc).asInstanceOf[scala.collection.mutable.LongMap[Long]]
    map(id) = System.currentTimeMillis() - ageMillis
  }

  "CongestionControl.canSend" should "be true initially with empty in-transit set" in {
    val cc = new CongestionControl()
    assert(cc.canSend)
  }

  it should "become false once in-transit messages reach the window size" in {
    val cc = new CongestionControl()
    // initial windowSize = 1
    cc.markMessageInTransit(msg(1L))
    assert(!cc.canSend)
  }

  it should "not block markMessageInTransit when in-transit count already exceeds window" in {
    // CongestionControl tracks message *count*, not byte size — payload size
    // does not factor into the window check (that's FlowControl's job, not
    // this class's). markMessageInTransit is a passive setter: it does not
    // check `canSend`. Callers are expected to consult `canSend` first; if
    // they don't, the in-transit map grows past windowSize but `canSend`
    // stays false.
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.markMessageInTransit(msg(2L)) // ignores window; should still record
    cc.markMessageInTransit(msg(3L))
    assert(cc.getInTransitMessages.size == 3)
    assert(!cc.canSend)
  }

  it should "stay true while in-transit count is below the grown window" in {
    val cc = new CongestionControl()
    // After three slow-start acks, the window should be at least 4. Verify
    // that three in-transit messages still leave room for more.
    (1L to 3L).foreach { i =>
      cc.markMessageInTransit(msg(i))
      cc.ack(i)
    }
    cc.markMessageInTransit(msg(10L))
    cc.markMessageInTransit(msg(11L))
    cc.markMessageInTransit(msg(12L))
    assert(cc.canSend, "window grew via slow start; 3 in-transit must not yet hit the cap")
  }

  it should "absorb arbitrarily many enqueued messages even when the window is full" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L)) // fills window-of-1
    assert(!cc.canSend)
    // Receivers may push many more while we are blocked; they must all queue
    // up and surface via getAllMessages without truncation or error.
    (10L until 30L).foreach(i => cc.enqueueMessage(msg(i)))
    val all = cc.getAllMessages.map(_.messageId).toSet
    assert(all.contains(1L))
    assert((10L until 30L).forall(all.contains))
  }

  "CongestionControl.ack" should "be a no-op for an unknown message id" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(99L)
    // CongestionControl.ack returns silently for ids not in `inTransit`
    // (no logging, no exception, no window change). Pin the state-level
    // no-op: the previously in-transit message survives, window stays full.
    assert(cc.getInTransitMessages.exists(_.messageId == 1L))
    assert(cc.getInTransitMessages.size == 1)
    assert(!cc.canSend)
  }

  it should "be a no-op when the same message id is acked twice" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(1L)
    val sizeAfterFirst = cc.getInTransitMessages.size
    cc.ack(1L) // duplicate ack — must not throw or further alter state
    assert(cc.getInTransitMessages.size == sizeAfterFirst)
  }

  it should "remove an acked in-transit message and allow more sending" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(1L)
    assert(!cc.getInTransitMessages.exists(_.messageId == 1L))
    assert(cc.canSend)
  }

  it should "grow the window via slow start when acked within the ack time limit" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.ack(1L) // immediate ack — well within ackTimeLimit (3s)
    // After the first slow-start ack, windowSize should be at least 2.
    cc.markMessageInTransit(msg(2L))
    assert(
      cc.canSend,
      "window must permit at least one more in-transit message after slow-start ack"
    )
  }

  it should "double the window during slow start, then increment linearly past ssThreshold" in {
    // ssThreshold defaults to 16 and windowSize to 1. Five quick acks should
    // double 1→2→4→8→16, then increment to 17 on the next ack (the fifth ack
    // hits the linear branch because windowSize == ssThreshold == 16).
    val cc = new CongestionControl()
    for (i <- 0 until 5) {
      cc.markMessageInTransit(msg(i.toLong))
      cc.ack(i.toLong)
    }
    assert(
      cc.getStatusReport.contains("current window size = 17"),
      s"unexpected status: ${cc.getStatusReport}"
    )
  }

  "CongestionControl.ack outside ackTimeLimit" should
    "halve ssThreshold and snap windowSize back to ssThreshold" in {
    // Drive windowSize up to 16 (== ssThreshold) via four in-window acks,
    // then backdate the next send so the ack falls outside ackTimeLimit.
    // The timeout branch should halve ssThreshold to 8 and snap windowSize
    // back to 8.
    val cc = new CongestionControl()
    for (i <- 0 until 4) {
      cc.markMessageInTransit(msg(i.toLong))
      cc.ack(i.toLong)
    }
    assert(cc.getStatusReport.contains("current window size = 16"))

    cc.markMessageInTransit(msg(99L))
    backdateSentTime(cc, 99L, 5000) // > ackTimeLimit (3000)
    cc.ack(99L)
    assert(
      cc.getStatusReport.contains("current window size = 8"),
      s"unexpected status: ${cc.getStatusReport}"
    )
  }

  it should "clamp ssThreshold at 1 so repeated timeouts never collapse the window to zero" in {
    // Five consecutive timed-out acks halve ssThreshold 16→8→4→2→1→0. The clamp
    // must lift that final 0 back to 1, because windowSize is then set from
    // ssThreshold and `canSend` is `inTransit.size < windowSize`: a window of 0
    // can never be satisfied, so the sender would wedge permanently.
    //
    // The window is pinned through `canSend` across an empty/full boundary
    // rather than through the `getStatusReport` text, since it is the sender's
    // behaviour and not the report's wording that this test is about: a drained
    // sender that may send, and that one in-transit message then blocks, has a
    // windowSize of exactly 1. Substring matching on the report could not do
    // this job — "current window size = 1" is a prefix of "= 16", so it would
    // pass a clamp that restored 16 — and the report's exact format is pinned
    // once, in `getStatusReport`'s own test at the bottom of this file. The
    // report is still built here, as the clue on a failure.
    val cc = new CongestionControl()
    (1L to 5L).foreach { i =>
      cc.markMessageInTransit(msg(i))
      backdateSentTime(cc, i, 5000) // > ackTimeLimit (3000)
      cc.ack(i)
    }
    assert(
      cc.getInTransitMessages.isEmpty,
      s"the acks left messages in transit: ${cc.getStatusReport}"
    )
    assert(
      cc.canSend,
      s"a fully drained sender must still be permitted to send: ${cc.getStatusReport}"
    )
    cc.markMessageInTransit(msg(6L))
    assert(
      !cc.canSend,
      s"one in-transit message must exhaust a window of 1: ${cc.getStatusReport}"
    )

    // A sixth timeout must keep it pinned at 1 rather than driving it negative.
    backdateSentTime(cc, 6L, 5000)
    cc.ack(6L)
    assert(
      cc.getInTransitMessages.isEmpty,
      s"the sixth ack left its message in transit: ${cc.getStatusReport}"
    )
    assert(cc.canSend, s"the clamp did not survive a second timeout: ${cc.getStatusReport}")
    cc.markMessageInTransit(msg(7L))
    assert(!cc.canSend, s"the window is no longer exactly 1: ${cc.getStatusReport}")
  }

  "CongestionControl.getBufferedMessagesToSend" should "be bounded by remaining window capacity" in {
    val cc = new CongestionControl()
    cc.enqueueMessage(msg(1L))
    cc.enqueueMessage(msg(2L))
    cc.enqueueMessage(msg(3L))
    // initial windowSize = 1, inTransit.size = 0  →  send up to 1
    val first = cc.getBufferedMessagesToSend.toList
    assert(first.size == 1)
    assert(first.head.messageId == 1L)
  }

  it should "return an empty iterable when the window is fully consumed" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.enqueueMessage(msg(2L))
    assert(cc.getBufferedMessagesToSend.isEmpty)
  }

  "CongestionControl.getAllMessages" should "include both in-transit and queued messages" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    cc.enqueueMessage(msg(2L))
    val all = cc.getAllMessages.map(_.messageId).toSet
    assert(all == Set(1L, 2L))
  }

  "CongestionControl.getTimedOutInTransitMessages" should "be empty when no message has been marked in transit" in {
    val cc = new CongestionControl()
    assert(cc.getTimedOutInTransitMessages.isEmpty)
  }

  it should "exclude messages that are still inside the resend time limit" in {
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(1L))
    // The message was just enqueued, so it is well inside the 60s resend
    // window and must not be reported as timed out.
    assert(cc.getTimedOutInTransitMessages.isEmpty)
  }

  it should "return only the messages whose sentTime is older than resendTimeLimit" in {
    // Cover the PekkoMessageTransferService.checkResend() retransmission path:
    // the in-transit message that has been sitting past the 60s
    // resendTimeLimit must surface; the freshly-sent one must not.
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(0L))
    cc.markMessageInTransit(msg(1L))
    backdateSentTime(cc, 0L, 70000) // > resendTimeLimit (60000)
    val timedOut = cc.getTimedOutInTransitMessages.toList.map(_.messageId)
    assert(timedOut == List(0L))
  }

  "CongestionControl.enqueueMessage" should "not place the message into the in-transit set on its own" in {
    val cc = new CongestionControl()
    cc.enqueueMessage(msg(1L))
    assert(cc.getInTransitMessages.isEmpty)
    // The message should still surface via getAllMessages (which unions
    // inTransit and toBeSent), proving it was buffered, not dropped.
    assert(cc.getAllMessages.exists(_.messageId == 1L))
  }

  "CongestionControl.getStatusReport" should
    "format the three core counters in the documented order" in {
    // Pin the exact format string: the separator, the label wording, and which
    // counter each label is actually reading.
    //
    // The three counters are driven to three *distinct* values on purpose. With
    // all three at 1 the string cannot pin the label-to-counter binding at all:
    // a report that read `toBeSent.size` where it says "in transit" and
    // `inTransit.size` where it says "waiting" renders byte-identically, so it
    // would pass. Nothing else in this file catches that swap either — the
    // report's other assertions only look at the window-size field. 2/1/3 makes
    // every field distinguishable from the other two.
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(0L))
    cc.ack(0L) // in-window ack: slow start doubles windowSize 1 -> 2
    cc.markMessageInTransit(msg(1L))
    (2L to 4L).foreach(i => cc.enqueueMessage(msg(i)))
    assert(
      cc.getStatusReport == "current window size = 2 \t in transit = 1 \t waiting = 3",
      s"unexpected format: ${cc.getStatusReport}"
    )
  }
}

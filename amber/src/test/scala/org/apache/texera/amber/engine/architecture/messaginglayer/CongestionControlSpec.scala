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
    // Cover the AkkaMessageTransferService.checkResend() retransmission path:
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
    // Pin the exact format string (separator + ordering) so a reorder of
    // the three fields or a tab-vs-comma swap fails this spec.
    val cc = new CongestionControl()
    cc.markMessageInTransit(msg(0L))
    cc.enqueueMessage(msg(1L))
    assert(
      cc.getStatusReport == "current window size = 1 \t in transit = 1 \t waiting = 1",
      s"unexpected format: ${cc.getStatusReport}"
    )
  }
}

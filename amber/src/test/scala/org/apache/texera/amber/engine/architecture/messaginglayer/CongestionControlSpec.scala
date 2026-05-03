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
import org.apache.texera.amber.engine.common.ambermessage.{
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CongestionControlSpec extends AnyFlatSpec with Matchers {

  private case object StubPayload extends WorkflowFIFOMessagePayload
  private val channelId = ChannelIdentity(
    ActorVirtualIdentity("from"),
    ActorVirtualIdentity("to"),
    isControl = false
  )

  private def netMsg(id: Long): NetworkMessage =
    NetworkMessage(id, WorkflowFIFOMessage(channelId, sequenceNumber = id, StubPayload))

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

  // ----- canSend / windowSize default -----

  "CongestionControl" should "start with windowSize=1 (so canSend is true at zero in-flight)" in {
    val cc = new CongestionControl
    cc.canSend shouldBe true
  }

  // ----- enqueue + send -----

  "enqueueMessage + getBufferedMessagesToSend" should "release up to (windowSize - inTransit) messages" in {
    val cc = new CongestionControl
    cc.enqueueMessage(netMsg(0))
    cc.enqueueMessage(netMsg(1))
    cc.enqueueMessage(netMsg(2))
    val released = cc.getBufferedMessagesToSend.toList
    // windowSize = 1, no in-flight yet → exactly one message released.
    released should have length 1
    released.head.messageId shouldBe 0L
  }

  it should "release nothing once inTransit reaches windowSize" in {
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    // windowSize = 1, inTransit = 1 → already saturated.
    cc.canSend shouldBe false
    cc.enqueueMessage(netMsg(1))
    cc.getBufferedMessagesToSend.toList shouldBe empty
  }

  // ----- markMessageInTransit -----

  "markMessageInTransit" should "register the message in inTransit and stamp send time" in {
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(7))
    cc.getInTransitMessages.map(_.messageId).toList shouldBe List(7L)
    // No throw means sentTime got registered too — ack uses it next.
    cc.ack(7L)
  }

  // ----- ack: in-window growth -----

  "ack within ackTimeLimit" should "double the window during slow start, then increment linearly past the threshold" in {
    val cc = new CongestionControl
    // ssThreshold defaults to 16 and windowSize to 1. Five quick acks should
    // double 1→2→4→8→16, then increment to 17 on the next ack.
    for (i <- 0 until 5) {
      cc.markMessageInTransit(netMsg(i))
      cc.ack(i)
    }
    // After 5 acks: windowSize moves 1→2→4→8→16→17 (the fifth ack hits the
    // linear branch because windowSize == ssThreshold == 16).
    cc.getStatusReport should include("current window size = 17")
  }

  it should "be a no-op for an unknown message id" in {
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    val statusBefore = cc.getStatusReport
    cc.ack(99) // not in inTransit
    cc.getStatusReport shouldBe statusBefore
  }

  // ----- ack: timeout shrinks the window -----

  "ack outside ackTimeLimit" should "halve ssThreshold and reset windowSize to ssThreshold" in {
    // Drive windowSize up to 16 (== ssThreshold) via four in-window acks,
    // then backdate the next send so the ack falls outside ackTimeLimit.
    // The timeout branch should halve ssThreshold to 8 and snap windowSize
    // back to 8.
    val cc = new CongestionControl
    for (i <- 0 until 4) {
      cc.markMessageInTransit(netMsg(i))
      cc.ack(i)
    }
    cc.getStatusReport should include("current window size = 16")

    cc.markMessageInTransit(netMsg(99))
    backdateSentTime(cc, 99L, 5000) // > ackTimeLimit (3000)
    cc.ack(99)
    cc.getStatusReport should include("current window size = 8")
  }

  // ----- timed-out in-transit -----

  "getTimedOutInTransitMessages" should "return nothing when nothing has been sitting past resendTimeLimit" in {
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    cc.getTimedOutInTransitMessages.toList shouldBe empty
  }

  it should "return only the messages whose sentTime is older than resendTimeLimit" in {
    // Cover the AkkaMessageTransferService.checkResend() retransmission path:
    // the in-transit message that has been sitting past the 60s
    // resendTimeLimit must surface; the freshly-sent one must not.
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    cc.markMessageInTransit(netMsg(1))
    backdateSentTime(cc, 0L, 70000) // > resendTimeLimit (60000)

    val timedOut = cc.getTimedOutInTransitMessages.toList.map(_.messageId)
    timedOut shouldBe List(0L)
  }

  // ----- getInTransitMessages / getAllMessages -----

  "getAllMessages" should "return the union of inTransit and toBeSent buffers" in {
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    cc.enqueueMessage(netMsg(1))
    cc.enqueueMessage(netMsg(2))
    val all = cc.getAllMessages.map(_.messageId).toSet
    all shouldBe Set(0L, 1L, 2L)
  }

  // ----- status report format -----

  "getStatusReport" should "format the three core counters in the documented order" in {
    // Pin the exact format string (separator + ordering) so a reorder of
    // the three fields or a tab-vs-comma swap fails this spec.
    val cc = new CongestionControl
    cc.markMessageInTransit(netMsg(0))
    cc.enqueueMessage(netMsg(1))
    cc.getStatusReport shouldBe
      "current window size = 1 \t in transit = 1 \t waiting = 1"
  }
}

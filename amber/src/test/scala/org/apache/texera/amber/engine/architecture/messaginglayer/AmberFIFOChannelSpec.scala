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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.common.ambermessage.{
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AmberFIFOChannelSpec extends AnyFlatSpec with Matchers {

  // A dummy non-DataFrame payload — getInMemSize falls through to its 200L
  // default, so each message contributes a known fixed credit cost.
  private case object StubPayload extends WorkflowFIFOMessagePayload
  private val FIXED_CREDIT_PER_MESSAGE = 200L

  private val channelId = ChannelIdentity(
    ActorVirtualIdentity("from"),
    ActorVirtualIdentity("to"),
    isControl = false
  )

  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(channelId, seq, StubPayload)

  // ----- initial state -----

  "AmberFIFOChannel" should "start with current=0 and no queued or stashed messages" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.getCurrentSeq shouldBe 0L
    ch.hasMessage shouldBe false
    ch.isEnabled shouldBe true
    ch.getQueuedCredit shouldBe 0L
    ch.getTotalMessageSize shouldBe 0L
    ch.getTotalStashedSize shouldBe 0L
  }

  // ----- acceptMessage -----

  "acceptMessage" should "enqueue an in-order message and advance current" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(0))
    ch.hasMessage shouldBe true
    ch.getCurrentSeq shouldBe 1L
    ch.getQueuedCredit shouldBe FIXED_CREDIT_PER_MESSAGE
  }

  it should "stash a future message without changing current" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(2))
    ch.getCurrentSeq shouldBe 0L
    ch.hasMessage shouldBe false
    ch.getTotalStashedSize shouldBe FIXED_CREDIT_PER_MESSAGE
  }

  it should "drop a duplicate message (sequenceNumber below current)" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(0))
    ch.acceptMessage(msg(0)) // duplicate
    ch.getCurrentSeq shouldBe 1L
    ch.getQueuedCredit shouldBe FIXED_CREDIT_PER_MESSAGE // not double-counted
  }

  it should "drop a message whose sequenceNumber is already stashed" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(2))
    ch.acceptMessage(msg(2)) // already in ofoMap
    ch.getTotalStashedSize shouldBe FIXED_CREDIT_PER_MESSAGE
  }

  it should "drain a contiguous run from the stash once the gap fills" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(1))
    ch.acceptMessage(msg(2))
    ch.acceptMessage(msg(4)) // non-contiguous gap at 3
    ch.acceptMessage(msg(0)) // unblocks 1, 2; 4 stays stashed because 3 missing
    ch.getCurrentSeq shouldBe 3L
    // queued: 0, 1, 2 — three messages worth of credit
    ch.getQueuedCredit shouldBe 3 * FIXED_CREDIT_PER_MESSAGE
    ch.getTotalStashedSize shouldBe FIXED_CREDIT_PER_MESSAGE // only seq=4 remains
  }

  // ----- take -----

  "take" should "return messages in FIFO order and decrement holdCredit" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(0))
    ch.acceptMessage(msg(1))
    ch.take.sequenceNumber shouldBe 0L
    ch.getQueuedCredit shouldBe FIXED_CREDIT_PER_MESSAGE
    ch.take.sequenceNumber shouldBe 1L
    ch.getQueuedCredit shouldBe 0L
    ch.hasMessage shouldBe false
  }

  // ----- enable / isEnabled -----

  "enable(false)" should "flip the enabled flag" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.isEnabled shouldBe true
    ch.enable(false)
    ch.isEnabled shouldBe false
    ch.enable(true)
    ch.isEnabled shouldBe true
  }

  // ----- size accessors -----

  "getTotalMessageSize" should "report the sum of in-memory size across queued messages" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(0))
    ch.acceptMessage(msg(1))
    ch.getTotalMessageSize shouldBe 2 * FIXED_CREDIT_PER_MESSAGE
  }

  "getTotalStashedSize" should "report the sum of in-memory size across stashed messages only" in {
    val ch = new AmberFIFOChannel(channelId)
    ch.acceptMessage(msg(2))
    ch.acceptMessage(msg(4))
    ch.getTotalStashedSize shouldBe 2 * FIXED_CREDIT_PER_MESSAGE
    ch.getTotalMessageSize shouldBe 0L // none are queued yet
  }

  // ----- portId -----

  "setPortId / getPortId" should "round-trip a PortIdentity once set" in {
    val ch = new AmberFIFOChannel(channelId)
    val port = PortIdentity(id = 3, internal = true)
    ch.setPortId(port)
    ch.getPortId shouldBe port
  }

  it should "throw NoSuchElementException when getPortId is called before setPortId (current behavior)" in {
    // Pin: getPortId calls `.get` on an Option that defaults to None. Calling
    // it before setPortId yields NoSuchElementException — there is no
    // explicit guard or default. Documenting so a future change to a safer
    // accessor (Option getter, or a sentinel default) breaks this spec.
    val ch = new AmberFIFOChannel(channelId)
    assertThrows[NoSuchElementException](ch.getPortId)
  }
}

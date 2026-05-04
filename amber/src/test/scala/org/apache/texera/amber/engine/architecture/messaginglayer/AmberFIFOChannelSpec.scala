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

class AmberFIFOChannelSpec extends AnyFlatSpec {

  private val cid =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  // Non-DataFrame payload, so each message has a deterministic 200L size
  // for credit/queued/stashed accounting.
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload
  private val msgSize: Long = 200L

  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(cid, seq, FixedSizePayload())

  // ---------------------------------------------------------------------------
  // Construction defaults
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel" should "expose the configured channelId and have an empty queue at construction" in {
    val ch = new AmberFIFOChannel(cid)
    assert(ch.channelId == cid)
    assert(!ch.hasMessage)
    assert(ch.getCurrentSeq == 0L)
    assert(ch.getQueuedCredit == 0L)
    assert(ch.getTotalMessageSize == 0L)
    assert(ch.getTotalStashedSize == 0L)
  }

  it should "default to enabled" in {
    val ch = new AmberFIFOChannel(cid)
    assert(ch.isEnabled)
  }

  // ---------------------------------------------------------------------------
  // FIFO ordering and stash
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel.acceptMessage" should "forward an in-order seq=0 message and advance the current sequence" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(0L))
    assert(ch.hasMessage)
    assert(ch.getCurrentSeq == 1L)
    assert(ch.getQueuedCredit == msgSize)
    assert(ch.getTotalMessageSize == msgSize)
  }

  it should "stash an out-of-order message until its predecessor arrives, then drain in FIFO order" in {
    val ch = new AmberFIFOChannel(cid)
    // arrives out of order: seq 1 first, then seq 0
    ch.acceptMessage(msg(1L))
    assert(!ch.hasMessage, "ahead-of-window message must be stashed, not delivered")
    assert(ch.getCurrentSeq == 0L)
    assert(ch.getTotalStashedSize == msgSize)

    ch.acceptMessage(msg(0L))
    // both should drain
    assert(ch.hasMessage)
    assert(ch.getCurrentSeq == 2L)
    assert(ch.getQueuedCredit == 2 * msgSize)
    assert(ch.getTotalStashedSize == 0L)

    val first = ch.take
    val second = ch.take
    assert(first.sequenceNumber == 0L)
    assert(second.sequenceNumber == 1L)
    assert(!ch.hasMessage)
    assert(ch.getQueuedCredit == 0L)
  }

  it should "drain a contiguous run from the stash once the gap fills, leaving a non-contiguous stashed message behind" in {
    // A three-message stash with a gap: seq 1, 2, 4 are all stashed because
    // seq 0 hasn't arrived; once 0 arrives, the contiguous run 0..2 drains
    // but 4 stays stashed because seq 3 is still missing.
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(1L))
    ch.acceptMessage(msg(2L))
    ch.acceptMessage(msg(4L))
    ch.acceptMessage(msg(0L))
    assert(ch.getCurrentSeq == 3L, "drain must advance current to the first missing seq")
    // queued: 0, 1, 2 — three messages worth of credit
    assert(ch.getQueuedCredit == 3 * msgSize)
    assert(ch.getTotalStashedSize == msgSize, "only seq=4 remains stashed")
  }

  it should "drop duplicates whose sequence number is below the current high-water mark" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(0L))
    ch.acceptMessage(msg(0L)) // duplicate
    assert(ch.getCurrentSeq == 1L, "duplicate must not advance the sequence")
    // only one message is buffered
    val out = ch.take
    assert(out.sequenceNumber == 0L)
    assert(!ch.hasMessage)
  }

  it should "drop duplicates that are stashed twice ahead of the current window" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(2L))
    ch.acceptMessage(msg(2L)) // duplicate stash
    assert(ch.getTotalStashedSize == msgSize, "duplicate stash must not double-count")
    // unblock by delivering 0 and 1
    ch.acceptMessage(msg(0L))
    ch.acceptMessage(msg(1L))
    assert(ch.getCurrentSeq == 3L)
    val received = (0 until 3).map(_ => ch.take.sequenceNumber).toList
    assert(received == List(0L, 1L, 2L))
  }

  // ---------------------------------------------------------------------------
  // Accounting under take
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel.take" should "decrement getQueuedCredit by the size of the dequeued message" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(0L))
    ch.acceptMessage(msg(1L))
    assert(ch.getQueuedCredit == 2 * msgSize)
    ch.take
    assert(ch.getQueuedCredit == msgSize)
    ch.take
    assert(ch.getQueuedCredit == 0L)
  }

  // ---------------------------------------------------------------------------
  // Size accessors
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel.getTotalMessageSize" should "report the sum of in-memory size across queued messages" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(0L))
    ch.acceptMessage(msg(1L))
    assert(ch.getTotalMessageSize == 2 * msgSize)
  }

  "AmberFIFOChannel.getTotalStashedSize" should "report the sum of in-memory size across stashed messages only" in {
    val ch = new AmberFIFOChannel(cid)
    ch.acceptMessage(msg(2L))
    ch.acceptMessage(msg(4L))
    assert(ch.getTotalStashedSize == 2 * msgSize)
    assert(ch.getTotalMessageSize == 0L, "stashed messages do not count toward queued size")
  }

  // ---------------------------------------------------------------------------
  // enable / isEnabled
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel.enable" should "toggle the enabled flag" in {
    val ch = new AmberFIFOChannel(cid)
    ch.enable(false)
    assert(!ch.isEnabled)
    ch.enable(true)
    assert(ch.isEnabled)
  }

  // ---------------------------------------------------------------------------
  // PortId association
  // ---------------------------------------------------------------------------

  "AmberFIFOChannel.getPortId" should "throw when no portId has been set" in {
    val ch = new AmberFIFOChannel(cid)
    // Option.get on None
    assertThrows[NoSuchElementException] {
      ch.getPortId
    }
  }

  it should "return the most recently configured portId" in {
    val ch = new AmberFIFOChannel(cid)
    ch.setPortId(PortIdentity(0))
    assert(ch.getPortId == PortIdentity(0))
    ch.setPortId(PortIdentity(7))
    assert(ch.getPortId == PortIdentity(7))
  }
}

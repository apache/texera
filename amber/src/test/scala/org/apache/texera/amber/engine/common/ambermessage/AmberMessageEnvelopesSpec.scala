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

package org.apache.texera.amber.engine.common.ambermessage

import org.apache.pekko.actor.{Address, ActorSystem}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class AmberMessageEnvelopesSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // Suite-local actor system used only by the ResendOutputTo test below;
  // shut down via TestKit.shutdownActorSystem in afterAll so threads do not
  // outlive the test, matching the cleanup pattern in ControllerSpec /
  // WorkerSpec.
  private val pekkoSystem: ActorSystem = ActorSystem("amber-message-envelopes-test")

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(pekkoSystem)
    super.afterAll()
  }

  private val channel =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)

  private val intSchema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
  private def tuple(v: Int): Tuple =
    Tuple
      .builder(intSchema)
      .add(intSchema.getAttribute("v"), Integer.valueOf(v))
      .build()

  // ---------------------------------------------------------------------------
  // WorkflowFIFOMessage / WorkflowRecoveryMessage envelope shape
  // ---------------------------------------------------------------------------

  "WorkflowFIFOMessage" should "carry channelId, sequenceNumber, and payload as constructed" in {
    val payload = DataFrame(Array(tuple(1)))
    val msg = WorkflowFIFOMessage(channel, 7L, payload)
    assert(msg.channelId == channel)
    assert(msg.sequenceNumber == 7L)
    assert(msg.payload == payload)
  }

  it should "be a WorkflowMessage and Serializable" in {
    val msg = WorkflowFIFOMessage(channel, 0L, DataFrame(Array.empty))
    assert(msg.isInstanceOf[WorkflowMessage])
    assert(msg.isInstanceOf[Serializable])
  }

  "WorkflowRecoveryMessage" should "carry the sender and payload as constructed" in {
    val from = ActorVirtualIdentity("worker-1")
    val payload = UpdateRecoveryStatus(isRecovering = true)
    val msg = WorkflowRecoveryMessage(from, payload)
    assert(msg.from == from)
    assert(msg.payload == payload)
  }

  // ---------------------------------------------------------------------------
  // RecoveryPayload subtypes
  // ---------------------------------------------------------------------------

  "RecoveryPayload subtypes" should "carry their constructor arguments" in {
    val update = UpdateRecoveryStatus(isRecovering = true)
    assert(update.isRecovering)

    val updateOff = UpdateRecoveryStatus(isRecovering = false)
    assert(!updateOff.isRecovering)

    val nodeFailure = NotifyFailedNode(Address("pekko", "test"))
    assert(nodeFailure.addr == Address("pekko", "test"))
  }

  it should "exercise ResendOutputTo via a real ActorRef so the case class wires correctly" in {
    val deadRef = pekkoSystem.deadLetters
    val vid = ActorVirtualIdentity("downstream")
    val payload = ResendOutputTo(vid, deadRef)
    assert(payload.vid == vid)
    assert(payload.ref == deadRef)
  }

  it should "be Serializable on every subtype" in {
    val payloads: Seq[RecoveryPayload] = Seq(
      UpdateRecoveryStatus(isRecovering = true),
      NotifyFailedNode(Address("pekko", "n"))
    )
    payloads.foreach(p => assert(p.isInstanceOf[Serializable]))
  }

  // ---------------------------------------------------------------------------
  // WorkflowMessage.getInMemSize
  // ---------------------------------------------------------------------------

  // A non-DataFrame payload so getInMemSize falls into the 200L default branch.
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload

  "WorkflowMessage.getInMemSize" should "be the DataFrame's inMemSize for a WorkflowFIFOMessage carrying a DataFrame" in {
    val df = DataFrame(Array(tuple(1), tuple(2)))
    val msg = WorkflowFIFOMessage(channel, 0L, df)
    assert(WorkflowMessage.getInMemSize(msg) == df.inMemSize)
  }

  it should "be zero for an empty-frame WorkflowFIFOMessage" in {
    val msg = WorkflowFIFOMessage(channel, 0L, DataFrame(Array.empty))
    assert(WorkflowMessage.getInMemSize(msg) == 0L)
  }

  it should "default to 200L for a non-DataFrame WorkflowFIFOMessagePayload" in {
    val msg = WorkflowFIFOMessage(channel, 0L, FixedSizePayload())
    assert(WorkflowMessage.getInMemSize(msg) == 200L)
  }

  // The catch-all `case _ => 200L` for non-WorkflowFIFOMessage subtypes is
  // guarded by `WorkflowMessage` being sealed. Today the sealed hierarchy
  // only has `WorkflowFIFOMessage`, so this branch is dead by construction;
  // we leave it untested rather than open the seal.

  // ---------------------------------------------------------------------------
  // WorkflowFIFOMessagePayload trait wiring (sanity)
  // ---------------------------------------------------------------------------

  "WorkflowFIFOMessagePayload trait" should "be implementable as a custom payload" in {
    val payload: WorkflowFIFOMessagePayload = FixedSizePayload()
    assert(payload.isInstanceOf[Serializable])
  }

  "DirectControlMessagePayload trait" should "be a WorkflowFIFOMessagePayload subtype" in {
    val custom: DirectControlMessagePayload = new DirectControlMessagePayload {}
    assert(custom.isInstanceOf[WorkflowFIFOMessagePayload])
  }
}

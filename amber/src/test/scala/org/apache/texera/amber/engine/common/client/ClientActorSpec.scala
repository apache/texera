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

package org.apache.texera.amber.engine.common.client

import org.apache.pekko.actor.{ActorSystem, Address}
import org.apache.pekko.pattern.StatusReply.Ack
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.texera.amber.core.virtualidentity.{
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  CreditResponse,
  NetworkAck,
  NetworkMessage
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmbeddedControlMessage,
  EmbeddedControlMessageType,
  EmptyRequest
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import org.apache.texera.amber.engine.common.ambermessage.{
  NotifyFailedNode,
  WorkflowFIFOMessage,
  WorkflowRecoveryMessage
}
import org.apache.texera.amber.engine.common.client.ClientActor.ClosureRequest
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

// This spec lives in `...engine.common.client` on purpose: `ClientActor`, its
// companion, and their members are `private[client]`, so a spec in any other
// package could not construct the actor or reference `ClosureRequest`.
class ClientActorSpec
    extends TestKit(ActorSystem("ClientActorSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  // TestActorRef gives synchronous, in-thread dispatch (so replies land before
  // the next assertion) and `.underlyingActor` access. We deliberately do NOT
  // send InitializeRequest: that arm would spin up a real Coordinator/engine.
  private def newClientActor(): TestActorRef[ClientActor] =
    TestActorRef(new ClientActor())

  private val channelId: ChannelIdentity =
    ChannelIdentity(CLIENT, COORDINATOR, isControl = true)

  "ClientActor" should "reply CreditResponse(channelId, 0) on a CreditRequest" in {
    val ref = newClientActor()

    // The client never queues credits, so getQueuedCredit is hard-coded to 0.
    ref ! CreditRequest(channelId)
    expectMsg(CreditResponse(channelId, 0L))
  }

  it should "reply with the closure's result on a successful ClosureRequest" in {
    val ref = newClientActor()

    ref ! ClosureRequest(() => 42)
    expectMsg(42)
  }

  it should "reply with the thrown exception on a failing ClosureRequest" in {
    val ref = newClientActor()

    ref ! ClosureRequest(() => throw new RuntimeException("boom"))
    val e = expectMsgType[RuntimeException]
    e.getMessage shouldBe "boom"
  }

  it should "ack the sender and forward to coordinator on WorkflowRecoveryMessage" in {
    val ref = newClientActor()
    val coordinatorProbe = TestProbe()
    // Must inject the coordinator before sending: the arm does
    // `coordinator ! x` with no null-guard, so a null coordinator would NPE.
    ref.underlyingActor.coordinator = coordinatorProbe.ref

    val recoveryMsg =
      WorkflowRecoveryMessage(CLIENT, NotifyFailedNode(Address("pekko", "ClientActorSpec")))
    ref ! recoveryMsg

    expectMsg(Ack)
    coordinatorProbe.expectMsg(recoveryMsg)
  }

  it should "ack a NetworkMessage carrying a non-ReturnInvocation control payload" in {
    val ref = newClientActor()

    // ControlInvocation is a DirectControlMessagePayload but not a
    // ReturnInvocation, exercising the inner "should not receive control
    // invocation" warn arm. EmptyRequest keeps the payload trivial.
    val payload = ControlInvocation("m", EmptyRequest(), AsyncRPCContext(CLIENT, COORDINATOR), 0)
    val fifoMsg = WorkflowFIFOMessage(channelId, 0, payload)
    ref ! NetworkMessage(1L, fifoMsg)

    val ack = expectMsgType[NetworkAck]
    ack.messageId shouldBe 1L
    ack.ackedCredit shouldBe getInMemSize(fifoMsg)
    ack.queuedCredit shouldBe 0L
  }

  it should "ack a NetworkMessage carrying an unrecognized payload" in {
    val ref = newClientActor()

    // EmbeddedControlMessage is a WorkflowFIFOMessagePayload that is neither a
    // DirectControlMessagePayload, DataPayload, nor ClientEvent, so it falls to
    // the catch-all "Amber Client received" info arm.
    val payload = EmbeddedControlMessage(
      EmbeddedControlMessageIdentity("ecm"),
      EmbeddedControlMessageType.NO_ALIGNMENT,
      Seq.empty,
      Map.empty
    )
    val fifoMsg = WorkflowFIFOMessage(channelId, 0, payload)
    ref ! NetworkMessage(2L, fifoMsg)

    val ack = expectMsgType[NetworkAck]
    ack.messageId shouldBe 2L
    ack.ackedCredit shouldBe getInMemSize(fifoMsg)
    ack.queuedCredit shouldBe 0L
  }
}

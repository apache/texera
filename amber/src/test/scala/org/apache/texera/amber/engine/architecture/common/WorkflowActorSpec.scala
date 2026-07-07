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

package org.apache.texera.amber.engine.architecture.common

import org.apache.pekko.actor.{ActorSystem, Props, UnhandledMessage}
import org.apache.pekko.testkit.{TestActorRef, TestKit, TestProbe}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  CreditResponse,
  GetActorRef,
  MessageBecomesDeadLetter,
  NetworkMessage,
  RegisterActorRef
}
import org.apache.texera.amber.engine.architecture.control.utils.TrivialControlTester
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  MainThreadDelegateMessage,
  TriggerSend
}
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

class WorkflowActorSpec
    extends TestKit(ActorSystem("WorkflowActorSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val selfId: ActorVirtualIdentity = ActorVirtualIdentity("self-worker")
  private val otherId: ActorVirtualIdentity = ActorVirtualIdentity("other-worker")

  // A control channel whose destination (toWorkerId) is the tester actor itself.
  private def channelTo(dest: ActorVirtualIdentity, isControl: Boolean = true): ChannelIdentity =
    ChannelIdentity(
      fromWorkerId = ActorVirtualIdentity("upstream"),
      toWorkerId = dest,
      isControl = isControl
    )

  // A NetworkMessage carrying a (data) payload addressed to `dest`.
  // The payload type is irrelevant for the branches under test here: for the
  // dead-letter retry path the redelivered message is stashed out-of-order and
  // never surfaces to the payload match, so no processing side effect occurs.
  private def networkMessageTo(
      dest: ActorVirtualIdentity,
      seq: Long = 0L,
      isControl: Boolean = true
  ): NetworkMessage =
    NetworkMessage(
      0L,
      WorkflowFIFOMessage(channelTo(dest, isControl), seq, DataFrame(Array.empty))
    )

  // Spawn the tester as a child of `probe` so `context.parent == probe.ref`
  // (needed to observe GetActorRef forwarding). TestActorRef also exposes
  // `underlyingActor` and processes self-sends synchronously on the calling
  // thread, which the log-writer routing tests rely on.
  private def newTester(
      probe: TestProbe,
      name: String
  ): TestActorRef[TrivialControlTester] =
    TestActorRef[TrivialControlTester](
      Props(new TrivialControlTester(selfId)),
      probe.ref,
      name
    )

  // ---------------------------------------------------------------------------
  // receiveCreditMessages (WorkflowActor lines 151-155)
  // ---------------------------------------------------------------------------

  "WorkflowActor" should "reply CreditResponse(channel, 0) to the sender on a CreditRequest" in {
    val sender = TestProbe()
    val parent = TestProbe()
    val ref = newTester(parent, "credit-request")
    val channel = channelTo(selfId)

    sender.send(ref, CreditRequest(channel))

    // TrivialControlTester.getQueuedCredit is hard-coded to 0.
    sender.expectMsg(CreditResponse(channel, 0L))
  }

  it should "accept a CreditResponse and forward it to the transfer service without crashing" in {
    val parent = TestProbe()
    val ref = newTester(parent, "credit-response")
    val channel = channelTo(selfId)

    // updateChannelCreditFromReceiver creates a fresh FlowControl and has no
    // messages to send, so this is a no-op that must not throw or kill the actor.
    ref ! CreditResponse(channel, 5L)

    // Actor stays alive and still services further requests.
    val sender = TestProbe()
    sender.send(ref, CreditRequest(channel))
    sender.expectMsg(CreditResponse(channel, 0L))
  }

  // ---------------------------------------------------------------------------
  // receiveDeadLetterMessage (WorkflowActor lines 158-170)
  // ---------------------------------------------------------------------------

  it should "keep its own actor-ref registered when a dead letter is addressed to itself (retry branch)" in {
    val parent = TestProbe()
    val ref = newTester(parent, "deadletter-self")

    // The tester registers `selfId -> self` in the WorkflowActor constructor.
    assert(ref.underlyingActor.actorRefMappingService.hasActorRef(selfId))

    // dest == actorId -> schedules a retry-to-self (does NOT remove the ref).
    // The redelivered message (seq = 1) is stashed out-of-order on the FIFO
    // channel, so no payload processing happens ~100ms later.
    ref ! MessageBecomesDeadLetter(networkMessageTo(selfId, seq = 1L))

    // The retry branch must not touch the actor-ref mapping.
    assert(ref.underlyingActor.actorRefMappingService.hasActorRef(selfId))
  }

  it should "remove the destination actor-ref when a dead letter is addressed to another worker (remove branch)" in {
    val parent = TestProbe()
    val ref = newTester(parent, "deadletter-other")
    val someRef = TestProbe().ref

    ref ! RegisterActorRef(otherId, someRef)
    assert(ref.underlyingActor.actorRefMappingService.hasActorRef(otherId))

    // dest != actorId -> removeActorRef(dest).
    ref ! MessageBecomesDeadLetter(networkMessageTo(otherId))

    assert(!ref.underlyingActor.actorRefMappingService.hasActorRef(otherId))
  }

  it should "treat removeActorRef as a no-op for a dead letter to an unknown worker (remove branch, unregistered)" in {
    val parent = TestProbe()
    val ref = newTester(parent, "deadletter-unknown")
    val unknownId = ActorVirtualIdentity("never-registered")

    assert(!ref.underlyingActor.actorRefMappingService.hasActorRef(unknownId))

    // Removing an id that was never registered must not throw or kill the actor.
    ref ! MessageBecomesDeadLetter(networkMessageTo(unknownId))

    assert(!ref.underlyingActor.actorRefMappingService.hasActorRef(unknownId))
    // Actor still alive: its own ref survives.
    assert(ref.underlyingActor.actorRefMappingService.hasActorRef(selfId))
  }

  // ---------------------------------------------------------------------------
  // receiveActorRefRelatedMessages (WorkflowActor lines 129-133)
  // ---------------------------------------------------------------------------

  it should "store an actor ref on RegisterActorRef" in {
    val parent = TestProbe()
    val ref = newTester(parent, "register-ref")
    val registered = TestProbe().ref

    assert(!ref.underlyingActor.actorRefMappingService.hasActorRef(otherId))
    ref ! RegisterActorRef(otherId, registered)
    assert(ref.underlyingActor.actorRefMappingService.hasActorRef(otherId))
    assert(ref.underlyingActor.actorRefMappingService.getActorRef(otherId) == registered)
  }

  it should "forward GetActorRef for an unknown id to its parent" in {
    val parent = TestProbe()
    val ref = newTester(parent, "get-ref-unknown")
    val unknownId = ActorVirtualIdentity("unknown-target")
    val replyTo = TestProbe().ref

    parent.send(ref, GetActorRef(unknownId, Set(replyTo)))

    // The tester is not the COORDINATOR, so an unknown id propagates to the
    // parent as GetActorRef, with the tester appended to the replyTo set.
    val forwarded = parent.expectMsgType[GetActorRef]
    assert(forwarded.id == unknownId)
    assert(forwarded.replyTo.contains(replyTo))
    assert(forwarded.replyTo.contains(ref))
  }

  // ---------------------------------------------------------------------------
  // sendMessageFromLogWriterToActor / handleTriggerSend (WorkflowActor 119-126)
  // ---------------------------------------------------------------------------

  it should "self-send a TriggerSend for a Right(WorkflowFIFOMessage) from the log writer" in {
    val parent = TestProbe()
    val ref = newTester(parent, "logwriter-right")
    // Control channel to an unknown worker: TriggerSend -> transferService.send
    // -> forwardToActor(unknown) -> retrieveActorRef -> parent ! GetActorRef.
    val unknownDest = ActorVirtualIdentity("downstream")
    val msg = WorkflowFIFOMessage(channelTo(unknownDest), 0L, DataFrame(Array.empty))

    ref.underlyingActor.sendMessageFromLogWriterToActor(Right(msg))

    // Observing the GetActorRef at the parent proves the TriggerSend was
    // self-delivered and handled by handleTriggerSend.
    val forwarded = parent.expectMsgType[GetActorRef]
    assert(forwarded.id == unknownDest)
  }

  it should "self-send the delegate value for a Left(MainThreadDelegateMessage) from the log writer" in {
    val parent = TestProbe()
    val ref = newTester(parent, "logwriter-left")
    val delegate = MainThreadDelegateMessage(_ => ())

    // WorkflowActor.receive does not handle MainThreadDelegateMessage, so the
    // self-sent value surfaces as an UnhandledMessage on the event stream.
    val listener = TestProbe()
    system.eventStream.subscribe(listener.ref, classOf[UnhandledMessage])
    try {
      ref.underlyingActor.sendMessageFromLogWriterToActor(Left(delegate))

      val unhandled = listener.expectMsgType[UnhandledMessage](2.seconds)
      assert(unhandled.message == delegate)
      assert(unhandled.recipient == ref)
    } finally {
      system.eventStream.unsubscribe(listener.ref)
    }
  }

  it should "route TriggerSend delivered as a normal message through handleTriggerSend" in {
    val parent = TestProbe()
    val ref = newTester(parent, "trigger-send-direct")
    val unknownDest = ActorVirtualIdentity("downstream-direct")
    val msg = WorkflowFIFOMessage(channelTo(unknownDest), 0L, DataFrame(Array.empty))

    ref ! TriggerSend(msg)

    val forwarded = parent.expectMsgType[GetActorRef]
    assert(forwarded.id == unknownDest)
  }
}

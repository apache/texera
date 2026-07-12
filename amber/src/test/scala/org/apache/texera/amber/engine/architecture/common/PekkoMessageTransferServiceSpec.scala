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

import org.apache.pekko.actor.{Actor, ActorContext, ActorSystem, Cancellable, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import org.apache.texera.amber.engine.architecture.messaginglayer.{CongestionControl, FlowControl}
import org.apache.texera.amber.engine.common.ambermessage.{
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload,
  WorkflowMessage
}
import org.apache.texera.common.config.ApplicationConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
  * Unit tests for [[PekkoMessageTransferService]].
  *
  * There is no Mockito in the amber test tree, so instead of mocking `ActorContext`
  * we obtain a real one from Pekko TestKit: a minimal [[CtxHolder]] actor is spawned
  * via `TestActorRef`, and `underlyingActor.context` supplies the live context that
  * `PekkoActorService` / `PekkoActorRefMappingService` eagerly dereference
  * (self / dispatcher / parent).
  *
  * `initialize()` is deliberately NOT called in the getAllUnAckedMessages and
  * backpressure recipes — it would schedule real repeating timers on the system
  * scheduler. The scheduler-driven `checkResend` path is covered separately by
  * overriding `scheduleWithFixedDelay` to capture (rather than schedule) the
  * callback (see [[CapturingActorService]]).
  */
class PekkoMessageTransferServiceSpec
    extends TestKit(ActorSystem("PekkoMessageTransferServiceSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("transfer-test-worker")

  // A non-DataFrame payload so `WorkflowMessage.getInMemSize` falls through to the
  // 200L default branch. (DataFrame(Array.empty) is 0 bytes and would never be able
  // to overload flow control regardless of the configured credit.)
  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload

  private def dataChannel(): ChannelIdentity =
    ChannelIdentity(
      fromWorkerId = ActorVirtualIdentity("from"),
      toWorkerId = ActorVirtualIdentity("to"),
      isControl = false
    )

  private def fifo(chan: ChannelIdentity, seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(chan, seq, FixedSizePayload())

  private def net(id: Long, chan: ChannelIdentity, seq: Long): NetworkMessage =
    NetworkMessage(id, fifo(chan, seq))

  // Pin the assumed payload size so this test fails loudly if the size accounting
  // changes in a way that would invalidate the credit math below.
  assert(WorkflowMessage.getInMemSize(fifo(dataChannel(), 0L)) == 200L)

  private val maxBytes: Long = ApplicationConfig.maxCreditAllowedInBytesPerChannel

  // Each service instance needs its own live ActorContext; spawn a fresh holder
  // actor (unique name) and hand back its context.
  private val ctxCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  private def freshContext(): ActorContext = {
    val holder =
      TestActorRef[CtxHolder](Props(new CtxHolder), s"ctx-holder-${ctxCounter.incrementAndGet()}")
    holder.underlyingActor.context
  }

  private def newActorService(): PekkoActorService =
    new PekkoActorService(actorId, freshContext())

  // ---------------------------------------------------------------------------
  // getAllUnAckedMessages (PekkoMessageTransferService lines 137-145)
  // ---------------------------------------------------------------------------

  "getAllUnAckedMessages" should
    "return both flow-control-stashed and congestion-control in-transit messages" in {
    val actorService = newActorService()
    val refService = new PekkoActorRefMappingService(actorService)
    val service = new PekkoMessageTransferService(actorService, refService, _ => ())

    val chan = dataChannel()

    // FlowControl carrying a stashed message that is drainable at read time:
    // stash it while credit is 0, then restore credit so getAllUnAckedMessages'
    // internal `fc.getMessagesToSend` actually returns it.
    val fc = new FlowControl()
    val fcNet = net(1L, chan, 1L)
    fc.updateQueuedCredit(maxBytes) // getCredit -> 0
    fc.getMessagesToSend(fcNet) // stash fcNet (returns empty, becomes overloaded)
    fc.updateQueuedCredit(0L) // restore credit; fcNet remains in the stash
    service.channelToFC(chan) = fc

    // CongestionControl holding one in-transit message.
    val cc = new CongestionControl()
    val ccNet = net(2L, chan, 2L)
    cc.markMessageInTransit(ccNet)
    service.channelToCC(chan) = cc

    val result = service.getAllUnAckedMessages.toList
    assert(result.contains(fcNet.internalMessage))
    assert(result.contains(ccNet.internalMessage))
    assert(result.size == 2)
  }

  // ---------------------------------------------------------------------------
  // send / updateChannelCreditFromReceiver -> checkForBackPressure
  // (PekkoMessageTransferService lines 79-104, 147-165)
  // ---------------------------------------------------------------------------

  "send / updateChannelCreditFromReceiver" should
    "raise backpressure when a data channel overloads and lower it when credit returns" in {
    var backpressured = false
    val actorService = newActorService()
    val refService = new PekkoActorRefMappingService(actorService)
    val service =
      new PekkoMessageTransferService(actorService, refService, b => backpressured = b)

    val dataChan = dataChannel()

    // Exhaust the receiver-side credit so getCredit drops to 0. The stash is still
    // empty here, so no channel is overloaded yet and backpressure stays off.
    service.updateChannelCreditFromReceiver(dataChan, maxBytes)
    assert(!backpressured)

    // A 200-byte data message cannot fit into 0 credit, so FlowControl stashes it and
    // the channel becomes overloaded -> checkForBackPressure flips false -> true.
    service.send(fifo(dataChan, 0L))
    assert(backpressured)

    // Restore credit: the stash drains, the channel is no longer overloaded, and
    // checkForBackPressure flips true -> false.
    service.updateChannelCreditFromReceiver(dataChan, 0L)
    assert(!backpressured)
  }

  // ---------------------------------------------------------------------------
  // initialize + checkResend (PekkoMessageTransferService lines 58-63, 167-181)
  // ---------------------------------------------------------------------------

  "initialize" should
    "schedule the resend/credit-polling callbacks and let checkResend run without error" in {
    val actorService = new CapturingActorService(actorId, freshContext())
    val refService = new PekkoActorRefMappingService(actorService)
    val service = new PekkoMessageTransferService(actorService, refService, _ => ())

    // At least one CongestionControl entry so the checkResend foreach body executes.
    val chan = dataChannel()
    service.channelToCC(chan) = new CongestionControl()

    service.initialize()
    // initialize() schedules exactly two fixed-delay callbacks (resend + credit poll).
    assert(actorService.capturedCallables.size == 2)

    // Invoke the captured resend callback directly. With no timed-out in-transit
    // messages, the body simply iterates the CC map and returns without throwing.
    val resendCallback = actorService.capturedCallables.head
    resendCallback()

    service.stop() // cancels the (already-cancelled) captured handles; must not throw
    assert(service.channelToCC.contains(chan))
  }
}

/** Minimal actor used only to obtain a real `ActorContext` from Pekko TestKit. */
class CtxHolder extends Actor {
  override def receive: Receive = { case _ => () }
}

/**
  * A [[PekkoActorService]] that captures scheduled callbacks instead of registering
  * real repeating timers, so `initialize()` can run and the resend callback can be
  * invoked deterministically from the test thread.
  */
class CapturingActorService(vid: ActorVirtualIdentity, ac: ActorContext)
    extends PekkoActorService(vid, ac) {

  val capturedCallables: mutable.ArrayBuffer[() => Unit] = mutable.ArrayBuffer()

  override def scheduleWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      callable: () => Unit
  ): Cancellable = {
    capturedCallables += callable
    Cancellable.alreadyCancelled
  }
}

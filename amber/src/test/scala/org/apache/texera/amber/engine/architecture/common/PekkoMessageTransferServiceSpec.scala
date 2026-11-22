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

  private def backdateSentTime(cc: CongestionControl, id: Long): Unit = {
    val field = classOf[CongestionControl].getDeclaredField("sentTime")
    field.setAccessible(true)
    val sentTime = field.get(cc).asInstanceOf[mutable.LongMap[Long]]
    sentTime(id) = System.currentTimeMillis() - cc.resendTimeLimit - 1
  }

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

  "receiveAck" should "drain buffered messages in FIFO order when the congestion window reopens" in {
    val actorService = newActorService()
    val destination = ActorVirtualIdentity("destination")
    val refService = new RecordingRefService(actorService, Set(destination))
    val service = new PekkoMessageTransferService(actorService, refService, _ => ())
    // A control channel skips byte-based flow control, so only the congestion window gates sends.
    val channel = ChannelIdentity(actorId, destination, isControl = true)
    val messages = (0L until 80L).map(id => net(id, channel, id))

    def ack(id: Long): Unit = service.receiveAck(id, 0L, 0L)

    def assertForwarded(count: Int): Unit = {
      assert(refService.forwardedMessages.toVector == messages.take(count))
    }

    // The window starts at 1, so one message goes out and the other 39 wait in the queue.
    messages.take(40).foreach(message => service.send(message.internalMessage))
    assertForwarded(1)
    // Prompt ACKs double the window 1 -> 2 -> 4 -> 8 -> 16 (the slow-start threshold),
    // then grow it by one to 17. Each ACK sends queued messages until the window is full.
    (0L to 4L).zip(Seq(3, 6, 11, 20, 22)).foreach {
      case (id, count) =>
        ack(id)
        assertForwarded(count)
    }

    // The late ACK shrinks the window from 17 to 8 with 16 messages still in flight.
    val congestionControl = service.channelToCC(channel)
    backdateSentTime(congestionControl, 5L)
    ack(5L)
    assert(congestionControl.getInTransitMessages.size == 16)
    assertForwarded(22)
    // A repeated ACK and an unknown ACK change nothing.
    ack(5L)
    ack(-1L)
    assertForwarded(22)

    // Each prompt ACK now frees one slot and grows the window by one, so the gap of 8
    // closes two at a time and nothing is sent until ACK 10.
    (6L to 9L).foreach { id =>
      ack(id)
      assertForwarded(22)
    }
    assert(congestionControl.getInTransitMessages.size == 12)
    ack(10L)
    assertForwarded(24)

    // New sends wait behind the 16 messages already queued instead of jumping ahead.
    messages.drop(40).foreach(message => service.send(message.internalMessage))
    assertForwarded(24)
    // From here each ACK sends the next two queued messages, oldest first, until all 80
    // have been sent and acknowledged.
    (11 until messages.size).foreach { id =>
      ack(id.toLong)
      assertForwarded(math.min(messages.size, 24 + 2 * (id - 10)))
    }
    assert(service.getAllUnAckedMessages.isEmpty)
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

  "checkResend" should "resend a timed-out message when its destination is registered" in {
    val actorService = new CapturingActorService(actorId, freshContext())
    val sender = ActorVirtualIdentity("known-sender")
    val destination = ActorVirtualIdentity("known-destination")
    val refService = new RecordingRefService(actorService, Set(destination))
    val service = new PekkoMessageTransferService(actorService, refService, _ => ())
    val channel = ChannelIdentity(sender, destination, isControl = false)
    val message = net(10L, channel, 0L)
    val cc = new CongestionControl()
    cc.markMessageInTransit(message)
    backdateSentTime(cc, message.messageId)
    service.channelToCC(channel) = cc

    service.initialize()
    actorService.capturedCallables.head()

    assert(refService.queriedIds == Seq(destination))
    assert(refService.forwardedMessages == Seq(message))
  }

  it should "not resend a timed-out message when only its sender is registered" in {
    val actorService = new CapturingActorService(actorId, freshContext())
    val sender = ActorVirtualIdentity("known-sender")
    val destination = ActorVirtualIdentity("removed-destination")
    val refService = new RecordingRefService(actorService, Set(sender))
    val service = new PekkoMessageTransferService(actorService, refService, _ => ())
    val channel = ChannelIdentity(sender, destination, isControl = false)
    val message = net(11L, channel, 0L)
    val cc = new CongestionControl()
    cc.markMessageInTransit(message)
    backdateSentTime(cc, message.messageId)
    service.channelToCC(channel) = cc

    service.initialize()
    actorService.capturedCallables.head()

    assert(refService.queriedIds == Seq(destination))
    assert(refService.forwardedMessages.isEmpty)
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

class RecordingRefService(
    actorService: PekkoActorService,
    knownIds: Set[ActorVirtualIdentity]
) extends PekkoActorRefMappingService(actorService) {

  val queriedIds: mutable.ArrayBuffer[ActorVirtualIdentity] = mutable.ArrayBuffer()
  val forwardedMessages: mutable.ArrayBuffer[NetworkMessage] = mutable.ArrayBuffer()

  override def hasActorRef(id: ActorVirtualIdentity): Boolean = {
    queriedIds += id
    knownIds.contains(id)
  }

  override def forwardToActor(msg: NetworkMessage): Unit = forwardedMessages += msg
}

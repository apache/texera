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
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  CreditResponse,
  GetActorRef,
  MessageBecomesDeadLetter,
  NetworkMessage,
  RegisterActorRef
}
import org.apache.texera.amber.engine.architecture.control.utils.TrivialControlTester
import org.apache.texera.amber.engine.architecture.logreplay.{
  MessageContent,
  ProcessingStep,
  ReplayLogRecord
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  MainThreadDelegateMessage,
  StateRestoreConfig,
  TriggerSend
}
import org.apache.texera.amber.engine.common.CheckpointState
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

class WorkflowActorSpec
    extends TestKit(ActorSystem("WorkflowActorSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    // WorkflowActor's getAvailableNodeAddressesFunc asks "/user/cluster-info";
    // without a listener there that ask times out.
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val selfId: ActorVirtualIdentity = ActorVirtualIdentity("self-worker")

  // Carries a marker only this spec writes, so a replayed data payload can be traced back to the
  // log rather than merely type-checked.
  private val replayedTuple: Tuple =
    Tuple
      .builder(Schema().add("marker", AttributeType.STRING))
      .add("marker", AttributeType.STRING, "replayed-data")
      .build()
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

  private val replayDestination: EmbeddedControlMessageIdentity =
    EmbeddedControlMessageIdentity("workflow-actor-spec-replay-to")

  /** Write `records` as the replay log named `logFileName` under `folder`.
    *
    * Each case uses its own `ram://` folder because the VFS manager is a
    * JVM-wide singleton.
    */
  private def writeLog(folder: URI, logFileName: String, records: Seq[ReplayLogRecord]): Unit = {
    val writer =
      SequentialRecordStorage.getStorage[ReplayLogRecord](Some(folder)).getWriter(logFileName)
    try {
      records.foreach(writer.writeRecord)
      writer.flush()
    } finally {
      writer.close()
    }
  }

  /** Counts `loadFromCheckpoint` calls so the checkpoint branch of `setupReplay`
    * is observable without depending on what the (currently unreadable)
    * checkpoint record deserializes to.
    */
  private class CheckpointCountingTester(workerId: ActorVirtualIdentity)
      extends TrivialControlTester(workerId) {
    val loadFromCheckpointCalls = new AtomicInteger()

    override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {
      loadFromCheckpointCalls.incrementAndGet()
    }
  }

  /** Fails every input message, to exercise `receiveMessageAndAck`'s catch. */
  private class FailingInputTester(workerId: ActorVirtualIdentity)
      extends TrivialControlTester(workerId) {
    override def handleInputMessage(messageId: Long, workflowMsg: WorkflowFIFOMessage): Unit =
      throw new IllegalStateException("handleInputMessage failed")
  }

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

  // ---------------------------------------------------------------------------
  // getAvailableNodeAddressesFunc (WorkflowActor lines 89-97)
  // ---------------------------------------------------------------------------

  it should "resolve cluster node addresses by asking /user/cluster-info" in {
    val parent = TestProbe()
    val ref = newTester(parent, "cluster-addresses")

    // PekkoActorService ships with `() => Array.empty`; the WorkflowActor
    // constructor replaces it with an ask against "/user/cluster-info".
    // ExecutorDeployment.createWorkers round-robins workers over whatever this
    // returns and asserts it is non-empty ("no available computation nodes"),
    // so losing the assignment breaks every deployment.
    val addresses = ref.underlyingActor.actorService.getClusterNodeAddresses

    // SingleNodeListener answers with its own path address, which for a local
    // (non-remote) ActorSystem is the address shared by every local actor.
    assert(addresses.toSeq == Seq(parent.ref.path.address))
  }

  // ---------------------------------------------------------------------------
  // receiveMessageAndAck failure path (WorkflowActor lines 138-146)
  // ---------------------------------------------------------------------------

  it should "register the sender and then rethrow when handleInputMessage fails" in {
    val parent = TestProbe()
    val messageSender = TestProbe()
    val ref = TestActorRef[FailingInputTester](
      Props(new FailingInputTester(selfId)),
      parent.ref,
      "input-message-throws"
    )
    val upstreamId = ActorVirtualIdentity("upstream")
    assert(!ref.underlyingActor.actorRefMappingService.hasActorRef(upstreamId))

    // `TestActorRef.receive` runs the behavior directly instead of going through
    // the mailbox, so supervision does not swallow the failure and `intercept`
    // sees whatever escapes `receiveMessageAndAck`. The contract under test is
    // that the handler's exception is re-thrown rather than logged-and-eaten:
    // the coordinator's AllForOneStrategy can only report a FatalError if the
    // failure actually reaches it.
    val thrown = intercept[IllegalStateException] {
      ref.receive(networkMessageTo(selfId), messageSender.ref)
    }
    assert(thrown.getMessage == "handleInputMessage failed")

    // The mapping is updated before the handler runs, so a failed message still
    // leaves a route back to its sender.
    assert(ref.underlyingActor.actorRefMappingService.getActorRef(upstreamId) == messageSender.ref)
  }

  // ---------------------------------------------------------------------------
  // setupReplay (WorkflowActor lines 190-227)
  // ---------------------------------------------------------------------------

  it should "replay from scratch by injecting logged messages behind the logged channel order" in {
    val parent = TestProbe()
    val ref = newTester(parent, "setup-replay-scratch")
    val actor = ref.underlyingActor
    val gateway = actor.ap.inputGateway

    val dataCid = channelTo(selfId, isControl = false)
    val controlCid = channelTo(selfId, isControl = true)
    val readFrom = new URI("ram:///workflow-actor-spec-replay-from-scratch/")
    writeLog(
      readFrom,
      actor.getLogName,
      Seq(
        // ProcessingStepCursor.INIT_STEP is -1 and the log manager has not
        // stepped yet, so the first record has to carry step -1; with any
        // larger value ReplayOrderEnforcer never latches a current channel and
        // nothing is ever pickable.
        ProcessingStep(dataCid, -1L),
        ProcessingStep(controlCid, 0L),
        MessageContent(WorkflowFIFOMessage(dataCid, 0L, DataFrame(Array(replayedTuple)))),
        MessageContent(
          WorkflowFIFOMessage(
            controlCid,
            0L,
            ControlInvocation(
              "replayed-control",
              EmptyRequest(),
              AsyncRPCContext(otherId, selfId),
              0
            )
          )
        )
      )
    )

    val completions = new AtomicInteger()
    actor.setupReplay(
      actor.ap,
      StateRestoreConfig(readFrom, replayDestination),
      () => completions.incrementAndGet()
    )

    // NetworkInputGateway.tryPickChannel prefers CONTROL channels, and both
    // channels hold a replayed message. The DATA channel can only come back
    // first because setupReplay installed a ReplayOrderEnforcer seeded from
    // logManager.getStep, which pins step -1 to the data channel. Dropping the
    // addEnforcer call, or starting the enforcer at step 0 instead of the log
    // manager's step, both hand back the control channel here.
    val first = gateway.tryPickChannel
    assert(first.map(_.channelId).contains(dataCid))
    assert(completions.get() == 0, "replay must not be complete while a step is still pending")

    val replayedData = first.get.take
    // The marker string exists nowhere but the log this test wrote, so this establishes
    // provenance -- a bare `isInstanceOf[DataFrame]` would not, since any DataFrame satisfies it.
    assert(replayedData.payload match {
      case DataFrame(frame) => frame.toSeq.map(_.getField[String]("marker")) == Seq("replayed-data")
      case other            => fail(s"unexpected replayed payload: $other")
    })

    // Processing that message advances the cursor to step 0, which is what
    // releases the next logged step (the control channel).
    actor.logManager.withFaultTolerant(dataCid, None) {
      // the replayed data message is the "work" for this step
    }

    val second = gateway.tryPickChannel
    assert(second.map(_.channelId).contains(controlCid))
    // The enforcer's queue is now drained, so replay is over -- exactly once.
    assert(completions.get() == 1)
    assert(second.get.take.payload match {
      case invocation: ControlInvocation => invocation.methodName == "replayed-control"
      case other                         => fail(s"unexpected replayed payload: $other")
    })
  }

  it should "take the checkpoint branch of setupReplay when the destination subfolder exists" in {
    val parent = TestProbe()
    val ref = TestActorRef[CheckpointCountingTester](
      Props(new CheckpointCountingTester(selfId)),
      parent.ref,
      "setup-replay-checkpoint"
    )
    val actor = ref.underlyingActor

    val readFrom = new URI("ram:///workflow-actor-spec-replay-checkpoint/")
    // Seed the top-level log as well, so the two branches are distinguishable:
    // had setupReplay taken the from-scratch branch it would have injected this
    // message into the input gateway.
    writeLog(
      readFrom,
      actor.getLogName,
      Seq(
        ProcessingStep(channelTo(selfId, isControl = false), -1L),
        MessageContent(
          WorkflowFIFOMessage(channelTo(selfId, isControl = false), 0L, DataFrame(Array.empty))
        )
      )
    )
    // Creating a checkpoint storage at the per-destination subfolder creates
    // that folder, which is what `containsFolder` keys on. The record file has
    // to exist too (empty is fine): SequentialRecordReader opens it through a
    // `lazy val`, and on a missing file the catch handler re-forces the failed
    // lazy val and throws straight out of the iterator.
    SequentialRecordStorage
      .getStorage[CheckpointState](Some(readFrom.resolve(replayDestination.toString)))
      .getWriter(actor.getLogName)
      .close()

    actor.setupReplay(actor.ap, StateRestoreConfig(readFrom, replayDestination), () => ())

    // Only branch selection is asserted. The record read back from an empty
    // checkpoint file is null, and CheckpointState is not java.io.Serializable
    // while cluster.conf turns java serialization off, so no real
    // CheckpointState can be written or read today; pinning the null would
    // cement that.
    assert(actor.loadFromCheckpointCalls.get() == 1)
    assert(
      actor.ap.inputGateway.getAllChannels.isEmpty,
      "the checkpoint branch must not replay the top-level log"
    )
  }

}

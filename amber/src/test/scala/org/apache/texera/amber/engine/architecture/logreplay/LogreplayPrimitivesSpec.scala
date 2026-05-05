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

package org.apache.texera.amber.engine.architecture.logreplay

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.{
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class LogreplayPrimitivesSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val workerId = ActorVirtualIdentity("worker-1")
  private val cidA =
    ChannelIdentity(ActorVirtualIdentity("up1"), workerId, isControl = false)
  private val cidB =
    ChannelIdentity(ActorVirtualIdentity("up2"), workerId, isControl = false)
  private val cidC =
    ChannelIdentity(ActorVirtualIdentity("up3"), workerId, isControl = false)

  // Suite-local ActorSystem + Serialization injected into AmberRuntime so the
  // ReplayLogRecord round-trip below uses the same Pekko serialization stack
  // that SequentialRecordStorage uses in production. Torn down in afterAll
  // so no Pekko threads outlive the suite. (Same pattern as
  // CheckpointSubsystemSpec.)
  private val testSystem: ActorSystem =
    ActorSystem("LogreplayPrimitivesSpec-test", AmberRuntime.akkaConfig)
  private val testSerde: Serialization = SerializationExtension(testSystem)

  private def setAmberRuntimeField(name: String, value: AnyRef): Unit = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(AmberRuntime, value)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_serde", null)
    setAmberRuntimeField("_actorSystem", null)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  private case class FixedSizePayload() extends WorkflowFIFOMessagePayload
  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(cidA, seq, FixedSizePayload())

  // ---------------------------------------------------------------------------
  // ReplayLoggerImpl
  // ---------------------------------------------------------------------------

  "ReplayLoggerImpl.logCurrentStepWithMessage" should "append a ProcessingStep when the channel changes" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val drained = l.drainCurrentLogRecords(0L)
    assert(drained.toList == List(ProcessingStep(cidA, 0L)))
  }

  it should "skip a same-channel call with no message" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    l.drainCurrentLogRecords(0L) // reset
    l.logCurrentStepWithMessage(1L, cidA, None) // same channel, no message
    val drained = l.drainCurrentLogRecords(1L)
    // Should still only carry the trailing ProcessingStep emitted by drain.
    assert(drained.toList == List(ProcessingStep(cidA, 1L)))
  }

  it should "append both a ProcessingStep and a MessageContent when a message is provided" in {
    val l = new ReplayLoggerImpl()
    val m = msg(7L)
    l.logCurrentStepWithMessage(2L, cidA, Some(m))
    val drained = l.drainCurrentLogRecords(2L)
    assert(drained.toList == List(ProcessingStep(cidA, 2L), MessageContent(m)))
  }

  it should "still log when a same-channel call carries a message (only no-message + same-channel is skipped)" in {
    // The skip guard in logCurrentStepWithMessage is `currentChannelId == channelId
    // && message.isEmpty` — both conditions, not just the channel match. After a
    // first call sets the current channel, a *subsequent* same-channel call with
    // a non-empty message must still emit ProcessingStep + MessageContent.
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None) // sets currentChannelId = cidA
    l.drainCurrentLogRecords(0L) // reset
    val m = msg(11L)
    l.logCurrentStepWithMessage(1L, cidA, Some(m)) // SAME channel, WITH message
    val drained = l.drainCurrentLogRecords(1L)
    assert(drained.toList == List(ProcessingStep(cidA, 1L), MessageContent(m)))
  }

  it should "append a ProcessingStep on a channel switch even if no message is provided" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    l.logCurrentStepWithMessage(1L, cidB, None) // channel change → must record
    val drained = l.drainCurrentLogRecords(1L)
    assert(drained.toList == List(ProcessingStep(cidA, 0L), ProcessingStep(cidB, 1L)))
  }

  "ReplayLoggerImpl.markAsReplayDestination" should
    "preserve exact ordering: in-flight ProcessingStep, then ReplayDestination, then synthetic trailing step" in {
    // ReplayLogGenerator depends on the relative position of ReplayDestination
    // within the record stream — replay stops at it. So a `contains` check
    // would silently accept a regression that duplicated ReplayDestination or
    // moved it after the synthetic trailing ProcessingStep emitted by drain.
    // Pin the full sequence instead.
    val l = new ReplayLoggerImpl()
    val ecm = EmbeddedControlMessageIdentity("checkpoint-1")
    l.logCurrentStepWithMessage(0L, cidA, None) // sets currentChannelId, appends ProcessingStep
    l.markAsReplayDestination(ecm)
    // Drain at a step beyond lastStep so the synthetic trailing ProcessingStep
    // is also emitted; this is exactly the production drain behavior we need
    // to lock down (the synthetic step must come AFTER the destination).
    val drained = l.drainCurrentLogRecords(3L).toList
    assert(
      drained == List(
        ProcessingStep(cidA, 0L),
        ReplayDestination(ecm),
        ProcessingStep(cidA, 3L)
      ),
      s"unexpected record order: $drained"
    )
  }

  "ReplayLoggerImpl.drainCurrentLogRecords" should "clear the buffer between drains" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val first = l.drainCurrentLogRecords(0L)
    val second = l.drainCurrentLogRecords(0L)
    assert(first.nonEmpty)
    assert(second.isEmpty, "second drain must yield no leftover records")
  }

  it should "append a synthetic ProcessingStep when the requested step differs from lastStep" in {
    val l = new ReplayLoggerImpl()
    l.logCurrentStepWithMessage(0L, cidA, None)
    val drained = l.drainCurrentLogRecords(5L)
    // Two records: the original ProcessingStep at step 0 and the synthetic one at step 5.
    assert(drained.toList == List(ProcessingStep(cidA, 0L), ProcessingStep(cidA, 5L)))
  }

  // ---------------------------------------------------------------------------
  // OrderEnforcer trait
  // ---------------------------------------------------------------------------

  "OrderEnforcer trait" should "be implementable as a custom subclass" in {
    val enf = new OrderEnforcer {
      override var isCompleted: Boolean = false
      override def canProceed(channelId: ChannelIdentity): Boolean = !isCompleted
    }
    assert(enf.canProceed(cidA))
    enf.isCompleted = true
    assert(!enf.canProceed(cidA))
  }

  // ---------------------------------------------------------------------------
  // ReplayOrderEnforcer
  // ---------------------------------------------------------------------------

  /** Stub that exposes a controllable `getStep`. */
  private class StubLogManager(
      handler: Either[
        org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage,
        WorkflowFIFOMessage
      ] => Unit
  ) extends EmptyReplayLogManagerImpl(handler) {
    private var step = 0L
    def setStep(s: Long): Unit = { step = s }
    override def getStep: Long = step
  }

  "ReplayOrderEnforcer" should "be completed immediately when the step queue is empty" in {
    val mgr = new StubLogManager(_ => ())
    val empty = mutable.Queue[ProcessingStep]()
    var fired = false
    val enf = new ReplayOrderEnforcer(mgr, empty, startStep = 0L, () => fired = true)
    assert(enf.isCompleted)
    assert(fired)
  }

  it should "skip log entries whose step is at or below startStep during construction (boundary inclusive)" in {
    // Use distinct channels at and around the boundary so the test is sensitive
    // to a `step < startStep` (vs `<= startStep`) regression. With startStep=1L:
    //   - correct impl drops steps 0 and 1 → after ctor, head is step 2 (cidC)
    //   - buggy impl that drops only step < 1 leaves step 1 (cidB) at the head
    // At step=2, canProceed(cidC) consumes step 2 and returns true under the
    // correct impl, but the buggy impl never matches the leftover step-1 entry
    // (`head.step == step` is 1 != 2), so currentChannelId stays at cidA (set
    // from the only forwardNext that ran on step 0) and canProceed(cidC) returns
    // false. Either side mismatching this assertion catches the boundary bug.
    val mgr = new StubLogManager(_ => ())
    mgr.setStep(2L)
    val q = mutable.Queue[ProcessingStep](
      ProcessingStep(cidA, 0L),
      ProcessingStep(cidB, 1L), // boundary entry — distinct channel
      ProcessingStep(cidC, 2L),
      ProcessingStep(cidA, 3L)
    )
    val enf = new ReplayOrderEnforcer(mgr, q, startStep = 1L, () => ())
    assert(!enf.isCompleted)
    val proceeded = enf.canProceed(cidC)
    assert(
      proceeded,
      "boundary entry must be dropped (step <= startStep), so cidC at step 2 is the next allowed channel"
    )
  }

  it should "advance to the next channel and fire onComplete on the non-empty-to-empty transition" in {
    val mgr = new StubLogManager(_ => ())
    mgr.setStep(0L)
    val q = mutable.Queue[ProcessingStep](
      ProcessingStep(cidA, 0L),
      ProcessingStep(cidB, 1L)
    )
    var fired = 0
    val enf = new ReplayOrderEnforcer(mgr, q, startStep = -1L, () => fired += 1)
    assert(fired == 0, "onComplete must NOT fire at construction while the queue is non-empty")

    // At step 0, the head matches and is consumed; currentChannelId becomes cidA.
    assert(enf.canProceed(cidA))
    assert(!enf.canProceed(cidB), "still on cidA until the next step is observed")
    assert(!enf.isCompleted)
    assert(fired == 0, "onComplete must NOT fire while the queue still has entries")

    mgr.setStep(1L)
    // The pre-advancement query: cidA is the previous channel, but the while
    // loop in canProceed will consume step 1 (cidB) before evaluating the
    // membership check, so cidA is rejected at step 1.
    assert(!enf.canProceed(cidA), "step 1's channel is cidB, not cidA")
    assert(enf.isCompleted, "queue is exhausted, replay must mark completed")
    // Now the previously consumed cidB is the current channel — pin that
    // a regression that drains the queue without updating the active
    // channel would NOT just satisfy this test by silence.
    assert(enf.canProceed(cidB), "cidB must be the active channel after step 1 is consumed")
    // onComplete must fire exactly once, on the empty-transition.
    assert(fired == 1, "onComplete must fire on the non-empty-to-empty transition")
    enf.canProceed(cidA) // further calls past completion must not refire
    assert(fired == 1)
  }

  it should "fire onComplete exactly once even if canProceed is called repeatedly past the end" in {
    val mgr = new StubLogManager(_ => ())
    var fired = 0
    val enf = new ReplayOrderEnforcer(
      mgr,
      mutable.Queue.empty[ProcessingStep],
      startStep = 0L,
      () => fired += 1
    )
    assert(fired == 1)
    enf.canProceed(cidA) // already completed → must not refire
    enf.canProceed(cidB)
    assert(fired == 1)
  }

  it should "consume every queue entry sharing the current step (the duplicate-step while loop)" in {
    // canProceed contains a `while (head.step == step) forwardNext()` loop
    // specifically because checkpoints produce duplicate step records (the
    // MainThreadDelegateMessage path emits an extra ProcessingStep at the
    // same step). A regression that consumed only one entry per step would
    // leave a stale duplicate at the head, so a subsequent canProceed at
    // the next step would still see the old channel — not the real next one.
    // Pin the multi-consume behavior with two adjacent same-step entries.
    val mgr = new StubLogManager(_ => ())
    mgr.setStep(0L)
    val q = mutable.Queue[ProcessingStep](
      ProcessingStep(cidA, 0L),
      ProcessingStep(cidB, 0L), // duplicate step — must be consumed too
      ProcessingStep(cidC, 1L)
    )
    val enf = new ReplayOrderEnforcer(mgr, q, startStep = -1L, () => ())

    // After both step-0 entries are consumed, currentChannelId is the LAST
    // one (cidB). cidA was the head but is no longer the active channel.
    assert(enf.canProceed(cidB), "the second step-0 entry (cidB) must be the active channel")
    assert(
      !enf.canProceed(cidA),
      "cidA was consumed by the duplicate-step while loop and is no longer active"
    )
    assert(!enf.isCompleted, "step 1 (cidC) is still queued")

    // Advancing to step 1 consumes cidC, leaving the queue empty.
    mgr.setStep(1L)
    assert(enf.canProceed(cidC))
    assert(enf.isCompleted)
  }

  // ---------------------------------------------------------------------------
  // ReplayLogRecord serde
  // ---------------------------------------------------------------------------

  // Round-trip each ReplayLogRecord subtype through Pekko Serialization (the
  // exact path SequentialRecordStorage uses in production via
  // AmberRuntime.serde). A broken serde registration or a deserialization
  // mismatch would fail this test, where `isInstanceOf[Serializable]` would
  // not.
  private def roundTrip(r: ReplayLogRecord): ReplayLogRecord = {
    val bytes = AmberRuntime.serde.serialize(r).get
    AmberRuntime.serde.deserialize(bytes, classOf[ReplayLogRecord]).get
  }

  // Production never writes a DataFrame to the replay log: both the
  // controller and DP-thread paths filter for `DirectControlMessagePayload`
  // before logging (see `Controller.scala` and `DPThread.scala` use of
  // `_.payload.isInstanceOf[DirectControlMessagePayload]`). The trait has
  // two concrete subtypes that production actually serializes —
  // `ControlInvocation` (outgoing call) and `ReturnInvocation` (reply) —
  // and `processDCM` handles both. Round-trip each so a serializer
  // regression on either subtype fails this spec.

  "ReplayLogRecord MessageContent" should "round-trip a ControlInvocation payload through AmberRuntime.serde" in {
    val payload = ControlInvocation(
      methodName = "doNothing",
      command = EmptyRequest(),
      context = AsyncRPCContext(workerId, workerId),
      commandId = 42L
    )
    val msg = WorkflowFIFOMessage(cidA, 1L, payload)
    val original: ReplayLogRecord = MessageContent(msg)
    val restored = roundTrip(original)
    assert(restored == original)
    val restoredMsg = restored.asInstanceOf[MessageContent].message
    assert(restoredMsg == msg)
    val restoredPayload = restoredMsg.payload.asInstanceOf[ControlInvocation]
    assert(restoredPayload.methodName == "doNothing")
    assert(restoredPayload.commandId == 42L)
  }

  it should "round-trip a ReturnInvocation payload through AmberRuntime.serde" in {
    val payload = ReturnInvocation(commandId = 42L, returnValue = EmptyReturn())
    val msg = WorkflowFIFOMessage(cidA, 2L, payload)
    val original: ReplayLogRecord = MessageContent(msg)
    val restored = roundTrip(original)
    assert(restored == original)
    val restoredMsg = restored.asInstanceOf[MessageContent].message
    assert(restoredMsg == msg)
    val restoredPayload = restoredMsg.payload.asInstanceOf[ReturnInvocation]
    assert(restoredPayload.commandId == 42L)
    assert(restoredPayload.returnValue == EmptyReturn())
  }

  "ReplayLogRecord ProcessingStep" should "round-trip through AmberRuntime.serde" in {
    val original: ReplayLogRecord = ProcessingStep(cidA, 7L)
    val restored = roundTrip(original)
    assert(restored == original)
    val ps = restored.asInstanceOf[ProcessingStep]
    assert(ps.channelId == cidA)
    assert(ps.step == 7L)
  }

  "ReplayLogRecord ReplayDestination" should "round-trip through AmberRuntime.serde" in {
    val ecm = EmbeddedControlMessageIdentity("ecm-1")
    val original: ReplayLogRecord = ReplayDestination(ecm)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.asInstanceOf[ReplayDestination].id == ecm)
  }

  // NOTE: TerminateSignal is intentionally NOT round-tripped here. It is an
  // in-memory shutdown sentinel for AsyncReplayLogWriter and is filtered
  // out before records are written to storage, so a Pekko-serialization
  // round-trip is not on a real production path. Pinning `eq`-identity
  // post-deserialization would over-constrain the serializer (a future
  // change that re-creates the case-object via reflection — still
  // semantically correct — would fail). Subtype membership is already
  // pinned by the case-object's compile-time `extends ReplayLogRecord`.
}

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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.pekko.actor.{Actor, ActorContext, ActorSystem, Cancellable, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.common.PekkoActorService
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
  * Unit tests for [[CoordinatorTimerService]].
  *
  * There is no Mockito in the amber test tree, so instead of mocking `ActorContext`
  * we obtain a real one from Pekko TestKit: a minimal [[TimerCtxHolder]] actor is
  * spawned via `TestActorRef`, and `underlyingActor.context` supplies the live
  * context that `PekkoActorService` eagerly dereferences (self / dispatcher).
  *
  * `sendToSelfWithFixedDelay` is overridden to capture the (initialDelay, delay,
  * message) triple instead of registering a real repeating timer, and each capture
  * returns a fresh [[RecordingCancellable]] so the tests can assert cancellation
  * and the independence of the two timer handles deterministically, with no real
  * scheduling and no sleeps.
  */
class CoordinatorTimerServiceSpec
    extends TestKit(ActorSystem("CoordinatorTimerServiceSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("timer-test-coordinator")

  private val statusIntervalMs: Long = 1000L
  private val runtimeIntervalMs: Long = 3000L

  // Each service instance needs its own live ActorContext; spawn a fresh holder
  // actor (unique name) and hand back its context.
  private val ctxCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  private def freshContext(): ActorContext = {
    val holder = TestActorRef[TimerCtxHolder](
      Props(new TimerCtxHolder),
      s"timer-ctx-holder-${ctxCounter.incrementAndGet()}"
    )
    holder.underlyingActor.context
  }

  private def newTimerService(
      statusUpdateIntervalMs: Option[Long],
      runtimeStatisticsPersistenceIntervalMs: Option[Long],
      cancelResult: Boolean = true
  ): (CoordinatorTimerService, CapturingSelfSchedulerService) = {
    val actorService = new CapturingSelfSchedulerService(actorId, freshContext(), cancelResult)
    val config = CoordinatorConfig(
      statusUpdateIntervalMs,
      runtimeStatisticsPersistenceIntervalMs,
      stateRestoreConfOpt = None,
      faultToleranceConfOpt = None
    )
    (new CoordinatorTimerService(config, actorService), actorService)
  }

  private def expectedInvocation(target: StatisticsUpdateTarget): ControlInvocation =
    AsyncRPCClient.ControlInvocation(
      METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS,
      QueryStatisticsRequest(Seq.empty, target),
      AsyncRPCContext(SELF, SELF),
      0
    )

  // ---------------------------------------------------------------------------
  // enable with unconfigured interval (CoordinatorTimerService line 47)
  // ---------------------------------------------------------------------------

  "enableStatusUpdate / enableRuntimeStatisticsCollection" should
    "schedule nothing when the corresponding interval is unconfigured" in {
    val (service, actorService) = newTimerService(None, None)

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()

    assert(actorService.captured.isEmpty)
    assert(service.statusUpdateAskHandle.isEmpty)
    assert(service.runtimeStatisticsAskHandle.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // enable with configured interval: full scheduling payload per entry point
  // (CoordinatorTimerService lines 48-59, 74-88)
  // ---------------------------------------------------------------------------

  "enableStatusUpdate" should
    "schedule a UI_ONLY statistics query at the configured status interval" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()

    assert(actorService.captured.size == 1)
    val capture = actorService.captured.head
    assert(capture.initialDelay == 0.milliseconds)
    assert(capture.delay == FiniteDuration(statusIntervalMs, "ms"))
    // Assert the individual fields first for readable failures, then the whole
    // message, so any newly-added field would still be pinned by the equality.
    val invocation = capture.msg.asInstanceOf[ControlInvocation]
    assert(invocation.methodName == METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS.getBareMethodName)
    assert(invocation.command == QueryStatisticsRequest(Seq.empty, StatisticsUpdateTarget.UI_ONLY))
    assert(invocation.context == AsyncRPCContext(SELF, SELF))
    assert(invocation.commandId == 0)
    assert(invocation == expectedInvocation(StatisticsUpdateTarget.UI_ONLY))
    assert(service.statusUpdateAskHandle.contains(capture.handle))
    // The other entry point's handle must be untouched.
    assert(service.runtimeStatisticsAskHandle.isEmpty)
  }

  "enableRuntimeStatisticsCollection" should
    "schedule a PERSISTENCE_ONLY statistics query at the configured persistence interval" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableRuntimeStatisticsCollection()

    assert(actorService.captured.size == 1)
    val capture = actorService.captured.head
    assert(capture.initialDelay == 0.milliseconds)
    assert(capture.delay == FiniteDuration(runtimeIntervalMs, "ms"))
    val invocation = capture.msg.asInstanceOf[ControlInvocation]
    assert(invocation.methodName == METHOD_COORDINATOR_INITIATE_QUERY_STATISTICS.getBareMethodName)
    assert(
      invocation.command ==
        QueryStatisticsRequest(Seq.empty, StatisticsUpdateTarget.PERSISTENCE_ONLY)
    )
    assert(invocation.context == AsyncRPCContext(SELF, SELF))
    assert(invocation.commandId == 0)
    assert(invocation == expectedInvocation(StatisticsUpdateTarget.PERSISTENCE_ONLY))
    assert(service.runtimeStatisticsAskHandle.contains(capture.handle))
    assert(service.statusUpdateAskHandle.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // asymmetric config: each enabler reads only its own interval field
  // ---------------------------------------------------------------------------

  "enableStatusUpdate / enableRuntimeStatisticsCollection" should
    "each read only their own interval when a single interval is configured" in {
    val (statusOnlyService, statusOnlyActorService) = newTimerService(Some(statusIntervalMs), None)

    statusOnlyService.enableStatusUpdate()
    statusOnlyService.enableRuntimeStatisticsCollection()

    assert(statusOnlyActorService.captured.size == 1)
    val statusCapture = statusOnlyActorService.captured.head
    assert(statusCapture.msg == expectedInvocation(StatisticsUpdateTarget.UI_ONLY))
    assert(statusOnlyService.statusUpdateAskHandle.contains(statusCapture.handle))
    assert(statusOnlyService.runtimeStatisticsAskHandle.isEmpty)

    // Mirror case: only the persistence interval is configured.
    val (runtimeOnlyService, runtimeOnlyActorService) =
      newTimerService(None, Some(runtimeIntervalMs))

    runtimeOnlyService.enableStatusUpdate()
    runtimeOnlyService.enableRuntimeStatisticsCollection()

    assert(runtimeOnlyActorService.captured.size == 1)
    val runtimeCapture = runtimeOnlyActorService.captured.head
    assert(runtimeCapture.msg == expectedInvocation(StatisticsUpdateTarget.PERSISTENCE_ONLY))
    assert(runtimeOnlyService.runtimeStatisticsAskHandle.contains(runtimeCapture.handle))
    assert(runtimeOnlyService.statusUpdateAskHandle.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // idempotence while a timer is running (CoordinatorTimerService lines 47, 60-62)
  // ---------------------------------------------------------------------------

  "enableStatusUpdate / enableRuntimeStatisticsCollection" should
    "be idempotent while their timer is already running" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    val statusHandle = service.statusUpdateAskHandle.get
    val runtimeHandle = service.runtimeStatisticsAskHandle.get

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()

    // No new schedule call, and the handles are still the very same instances.
    assert(actorService.captured.size == 2)
    assert(service.statusUpdateAskHandle.get eq statusHandle)
    assert(service.runtimeStatisticsAskHandle.get eq runtimeHandle)
  }

  // ---------------------------------------------------------------------------
  // disable cancels and resets; a second disable is a no-op
  // (CoordinatorTimerService lines 65-72, 90-96)
  // ---------------------------------------------------------------------------

  "disableStatusUpdate / disableRuntimeStatisticsCollection" should
    "cancel the running timer, reset the handle, and no-op when already disabled" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    val statusHandle = actorService.captured.head.handle
    val runtimeHandle = actorService.captured(1).handle

    service.disableStatusUpdate()
    service.disableRuntimeStatisticsCollection()

    assert(statusHandle.cancelCount == 1)
    assert(runtimeHandle.cancelCount == 1)
    assert(service.statusUpdateAskHandle.isEmpty)
    assert(service.runtimeStatisticsAskHandle.isEmpty)

    // Disabling again must neither throw nor cancel a second time.
    service.disableStatusUpdate()
    service.disableRuntimeStatisticsCollection()
    assert(statusHandle.cancelCount == 1)
    assert(runtimeHandle.cancelCount == 1)
  }

  "disableStatusUpdate / disableRuntimeStatisticsCollection" should
    "reset the handle even when cancel() returns false" in {
    // disableTimer discards the Boolean returned by cancel() (line 67) and resets
    // unconditionally; the reset must not depend on the cancellation succeeding.
    val (service, actorService) =
      newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs), cancelResult = false)

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    service.disableStatusUpdate()
    service.disableRuntimeStatisticsCollection()

    assert(actorService.captured.head.handle.cancelCount == 1)
    assert(actorService.captured(1).handle.cancelCount == 1)
    assert(service.statusUpdateAskHandle.isEmpty)
    assert(service.runtimeStatisticsAskHandle.isEmpty)
  }

  "disableStatusUpdate / disableRuntimeStatisticsCollection" should
    "be a no-op when the timer was never enabled" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.disableStatusUpdate()
    service.disableRuntimeStatisticsCollection()

    assert(actorService.captured.isEmpty)
    assert(service.statusUpdateAskHandle.isEmpty)
    assert(service.runtimeStatisticsAskHandle.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // restart after disable: the guard is on the handle, not on history
  // ---------------------------------------------------------------------------

  "enableStatusUpdate" should "schedule a fresh timer after a disable" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), None)

    service.enableStatusUpdate()
    service.disableStatusUpdate()
    service.enableStatusUpdate()

    assert(actorService.captured.size == 2)
    val secondHandle = actorService.captured(1).handle
    assert(service.statusUpdateAskHandle.contains(secondHandle))
    assert(!secondHandle.isCancelled)
  }

  "enableRuntimeStatisticsCollection" should "schedule a fresh timer after a disable" in {
    val (service, actorService) = newTimerService(None, Some(runtimeIntervalMs))

    service.enableRuntimeStatisticsCollection()
    service.disableRuntimeStatisticsCollection()
    service.enableRuntimeStatisticsCollection()

    assert(actorService.captured.size == 2)
    val secondCapture = actorService.captured(1)
    assert(service.runtimeStatisticsAskHandle.contains(secondCapture.handle))
    assert(!secondCapture.handle.isCancelled)
    assert(secondCapture.msg == expectedInvocation(StatisticsUpdateTarget.PERSISTENCE_ONLY))
  }

  // ---------------------------------------------------------------------------
  // full pause-resume lifecycle: StartWorkflowHandler enables both timers,
  // PauseHandler disables both, resume re-enables both
  // ---------------------------------------------------------------------------

  "enableStatusUpdate / enableRuntimeStatisticsCollection" should
    "re-schedule both timers with their own target and interval across a pause-resume cycle" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    service.disableStatusUpdate()
    service.disableRuntimeStatisticsCollection()
    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()

    assert(actorService.captured.size == 4)
    // The first-round handles were each cancelled exactly once by the pause.
    assert(actorService.captured.head.handle.cancelCount == 1)
    assert(actorService.captured(1).handle.cancelCount == 1)
    // The second-round handles are installed and live.
    val statusResume = actorService.captured(2)
    val runtimeResume = actorService.captured(3)
    assert(service.statusUpdateAskHandle.contains(statusResume.handle))
    assert(service.runtimeStatisticsAskHandle.contains(runtimeResume.handle))
    assert(!statusResume.handle.isCancelled)
    assert(!runtimeResume.handle.isCancelled)
    // The re-enabled timers still carry their own interval and statistics target.
    assert(statusResume.delay == FiniteDuration(statusIntervalMs, "ms"))
    assert(statusResume.msg == expectedInvocation(StatisticsUpdateTarget.UI_ONLY))
    assert(runtimeResume.delay == FiniteDuration(runtimeIntervalMs, "ms"))
    assert(runtimeResume.msg == expectedInvocation(StatisticsUpdateTarget.PERSISTENCE_ONLY))
  }

  // ---------------------------------------------------------------------------
  // the two handles are independent of each other
  // ---------------------------------------------------------------------------

  "disableStatusUpdate" should "not affect the runtime statistics timer" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    service.disableStatusUpdate()

    val runtimeHandle = actorService.captured(1).handle
    assert(!runtimeHandle.isCancelled)
    assert(service.runtimeStatisticsAskHandle.contains(runtimeHandle))
    assert(service.statusUpdateAskHandle.isEmpty)
  }

  "disableRuntimeStatisticsCollection" should "not affect the status update timer" in {
    val (service, actorService) = newTimerService(Some(statusIntervalMs), Some(runtimeIntervalMs))

    service.enableStatusUpdate()
    service.enableRuntimeStatisticsCollection()
    service.disableRuntimeStatisticsCollection()

    val statusHandle = actorService.captured.head.handle
    assert(!statusHandle.isCancelled)
    assert(service.statusUpdateAskHandle.contains(statusHandle))
    assert(service.runtimeStatisticsAskHandle.isEmpty)
  }
}

/** Minimal actor used only to obtain a real `ActorContext` from Pekko TestKit. */
class TimerCtxHolder extends Actor {
  override def receive: Receive = { case _ => () }
}

/**
  * A [[Cancellable]] that records how many times it was cancelled. `cancelResult`
  * is what `cancel()` reports back: Pekko's contract returns false when the task
  * was already cancelled or the scheduler is shut down.
  */
class RecordingCancellable(cancelResult: Boolean = true) extends Cancellable {
  var cancelCount: Int = 0

  override def cancel(): Boolean = {
    cancelCount += 1
    cancelResult
  }

  override def isCancelled: Boolean = cancelCount > 0
}

/** One captured `sendToSelfWithFixedDelay` call and the fake handle it returned. */
case class CapturedSchedule(
    initialDelay: FiniteDuration,
    delay: FiniteDuration,
    msg: Any,
    handle: RecordingCancellable
)

/**
  * A [[PekkoActorService]] that captures self-send scheduling requests instead of
  * registering real repeating timers, returning a fresh [[RecordingCancellable]]
  * per call so cancellation of each timer can be asserted independently.
  */
class CapturingSelfSchedulerService(
    vid: ActorVirtualIdentity,
    ac: ActorContext,
    cancelResult: Boolean = true
) extends PekkoActorService(vid, ac) {

  val captured: mutable.ArrayBuffer[CapturedSchedule] = mutable.ArrayBuffer()

  override def sendToSelfWithFixedDelay(
      initialDelay: FiniteDuration,
      delay: FiniteDuration,
      msg: Any
  ): Cancellable = {
    val handle = new RecordingCancellable(cancelResult)
    captured += CapturedSchedule(initialDelay, delay, msg, handle)
    handle
  }
}

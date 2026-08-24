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

import org.apache.pekko.actor.{ActorContext, ActorSystem, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.coordinator.{
  CapturingSelfSchedulerService,
  TimerCtxHolder
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_FLUSH_NETWORK_BUFFER
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF
import org.apache.texera.common.config.ApplicationConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

/**
  * Unit tests for [[WorkerTimerService]].
  *
  * The seam under test is [[org.apache.texera.amber.engine.architecture.common.PekkoActorService]],
  * not Pekko's scheduler: `CapturingSelfSchedulerService` (shared with
  * `CoordinatorTimerServiceSpec`, which is the coordinator-side analogue of this
  * class) overrides `sendToSelfWithFixedDelay` to record the (initialDelay, delay,
  * message) triple and hand back a `RecordingCancellable` instead of arming a real
  * repeating timer. Consequently there is no wall-clock waiting anywhere in this
  * suite - no sleeps, no `awaitCond`, no real scheduling.
  *
  * `PekkoActorService` eagerly dereferences `actorContext.self` and
  * `actorContext.dispatcher` in its constructor, so a live `ActorContext` is
  * required; it comes from a minimal `TimerCtxHolder` actor spawned per service.
  */
class WorkerTimerServiceSpec
    extends TestKit(ActorSystem("WorkerTimerServiceSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val actorId: ActorVirtualIdentity = ActorVirtualIdentity("timer-test-worker")

  private val ctxCounter = new java.util.concurrent.atomic.AtomicInteger(0)
  private def freshContext(): ActorContext = {
    val holder = TestActorRef[TimerCtxHolder](
      Props(new TimerCtxHolder),
      s"worker-timer-ctx-holder-${ctxCounter.incrementAndGet()}"
    )
    holder.underlyingActor.context
  }

  /**
    * @param cancelResult what the fake timer handles report from `cancel()`. Pekko
    *                     returns false when the task was already cancelled or the
    *                     scheduler is shut down, and `stopAdaptiveBatching`
    *                     discards that Boolean on purpose.
    */
  private def newTimerService(
      cancelResult: Boolean = true
  ): (WorkerTimerService, CapturingSelfSchedulerService) = {
    val actorService = new CapturingSelfSchedulerService(actorId, freshContext(), cancelResult)
    (new WorkerTimerService(actorService), actorService)
  }

  /**
    * A service whose constructor-captured adaptive-batching flag has been forced
    * to `false`.
    *
    * `ApplicationConfig.enableAdaptiveNetworkBuffering` is an object `val` bound to
    * `enable-adaptive-buffering = true` in application.conf and is therefore fixed
    * for the whole (shared, unforked) amber test JVM; the only test-only way to
    * reach the disabled path is to overwrite the private field the constructor
    * copied it into. The write is asserted below so that a JVM which silently
    * refuses it fails loudly instead of turning the test vacuous.
    */
  private def newDisabledTimerService(): (WorkerTimerService, CapturingSelfSchedulerService) = {
    val (service, actorService) = newTimerService()
    val field = classOf[WorkerTimerService].getDeclaredField("enabledAdaptiveBatching")
    field.setAccessible(true)
    field.setBoolean(service, false)
    (service, actorService)
  }

  private def expectedFlushInvocation: ControlInvocation =
    AsyncRPCClient.ControlInvocation(
      METHOD_FLUSH_NETWORK_BUFFER,
      EmptyRequest(),
      AsyncRPCContext(SELF, SELF),
      AsyncRPCClient.IgnoreReplyAndDoNotLog
    )

  // ---------------------------------------------------------------------------
  // startAdaptiveBatching: the scheduling payload (lines 51-62)
  // ---------------------------------------------------------------------------

  "startAdaptiveBatching" should
    "arm one immediate FLUSH_NETWORK_BUFFER self-send repeating at the configured interval" in {
    val (service, actorService) = newTimerService()

    service.startAdaptiveBatching()

    assert(actorService.captured.size == 1)
    val capture = actorService.captured.head
    // Immediate first flush, then one every `adaptiveBufferingTimeoutMs`. This
    // assertion reads the same accessor the class under test reads, so by itself it
    // pins the argument ORDER and the unit rather than the value: swapping the
    // initializer for a config key that happens to hold 500 too (there is exactly
    // one, `constants.status-update-interval`) would slip past it. The next test
    // closes that gap from the other side.
    assert(capture.initialDelay == 0.milliseconds)
    assert(
      capture.delay == FiniteDuration(ApplicationConfig.adaptiveBufferingTimeoutMs, MILLISECONDS)
    )
    // Field-by-field first for readable failures, then whole-message equality so a
    // newly added field would still be pinned.
    val invocation = capture.msg.asInstanceOf[ControlInvocation]
    assert(invocation.methodName == METHOD_FLUSH_NETWORK_BUFFER.getBareMethodName)
    assert(invocation.command == EmptyRequest())
    assert(invocation.context == AsyncRPCContext(SELF, SELF))
    assert(invocation.commandId == AsyncRPCClient.IgnoreReplyAndDoNotLog)
    assert(invocation == expectedFlushInvocation)
    assert(service.adaptiveBatchingHandle.contains(capture.handle))
    assert(!capture.handle.isCancelled)
    assert(!service.isPaused)
  }

  it should "repeat at the interval held by the field its constructor captured" in {
    // Two things no assertion phrased in terms of `ApplicationConfig` can see,
    // because the real interval is 500 and so is `constants.status-update-interval`:
    // that the delay flows from the captured field at all (rather than from a
    // hard-coded `500.milliseconds`), and that the field is an Int read from the
    // adaptive-buffering key (a Long-typed sibling key makes `getInt` below throw).
    // Forcing the field to a value no key holds pins the first; the equality
    // guarding the write pins the second. Same reflective technique as
    // `newDisabledTimerService`; the write is read back so a JVM that silently
    // refuses it fails loudly instead of turning the test vacuous.
    val (service, actorService) = newTimerService()
    val field = classOf[WorkerTimerService].getDeclaredField("adaptiveBatchInterval")
    field.setAccessible(true)
    assert(field.getInt(service) == ApplicationConfig.adaptiveBufferingTimeoutMs)
    field.setInt(service, 137)
    assert(field.getInt(service) == 137)

    service.startAdaptiveBatching()

    assert(actorService.captured.size == 1)
    assert(actorService.captured.head.delay == 137.milliseconds)
    assert(actorService.captured.head.initialDelay == 0.milliseconds)
  }

  it should "be idempotent while its timer is already armed" in {
    // DataProcessor.outputOneTuple() calls this on every output tuple, so the
    // already-armed guard is on the hot path.
    val (service, actorService) = newTimerService()

    service.startAdaptiveBatching()
    val handle = service.adaptiveBatchingHandle.get
    service.startAdaptiveBatching()
    service.startAdaptiveBatching()

    assert(actorService.captured.size == 1)
    assert(service.adaptiveBatchingHandle.get eq handle)
  }

  it should "arm nothing when adaptive network buffering is disabled" in {
    // Guard the reflective write itself: if it stopped taking effect this test
    // would otherwise pass for the wrong reason.
    assert(ApplicationConfig.enableAdaptiveNetworkBuffering)
    val (service, actorService) = newDisabledTimerService()
    val field = classOf[WorkerTimerService].getDeclaredField("enabledAdaptiveBatching")
    field.setAccessible(true)
    assert(!field.getBoolean(service))

    service.startAdaptiveBatching()

    assert(actorService.captured.isEmpty)
    assert(service.adaptiveBatchingHandle.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // stopAdaptiveBatching (lines 66-69)
  // ---------------------------------------------------------------------------

  "stopAdaptiveBatching" should "cancel the armed timer exactly once" in {
    val (service, actorService) = newTimerService()

    service.startAdaptiveBatching()
    service.stopAdaptiveBatching()

    assert(actorService.captured.head.handle.cancelCount == 1)
    assert(!service.isPaused)
  }

  it should "be a no-op when no timer was ever armed" in {
    // WorkflowWorker.postStop() stops the timer unconditionally, so this path is
    // reached whenever a worker dies before producing its first output tuple.
    val (service, actorService) = newTimerService()

    service.stopAdaptiveBatching()

    assert(actorService.captured.isEmpty)
    assert(service.adaptiveBatchingHandle.isEmpty)
    assert(!service.isPaused)
  }

  it should "lower the paused latch even when cancel() reports false" in {
    // `stopAdaptiveBatching` discards the Boolean `cancel()` returns, and Pekko
    // returns false for an already-cancelled task or a shut-down scheduler - which
    // is exactly the shape of WorkflowWorker.postStop() stopping the timer after a
    // pause already cancelled it. The bookkeeping must not depend on that Boolean.
    val (service, actorService) = newTimerService(cancelResult = false)

    service.startAdaptiveBatching()
    service.pauseAdaptiveBatching()
    service.stopAdaptiveBatching()

    assert(actorService.captured.head.handle.cancelCount == 2)
    assert(!service.isPaused)
  }

  // ---------------------------------------------------------------------------
  // pauseAdaptiveBatching (lines 73-74)
  // ---------------------------------------------------------------------------

  "pauseAdaptiveBatching" should "cancel the armed timer and raise the paused latch" in {
    val (service, actorService) = newTimerService()

    service.startAdaptiveBatching()
    service.pauseAdaptiveBatching()

    assert(actorService.captured.head.handle.cancelCount == 1)
    assert(service.isPaused)
  }

  // ---------------------------------------------------------------------------
  // resumeAdaptiveBatching (lines 78-79)
  // ---------------------------------------------------------------------------

  "resumeAdaptiveBatching" should "arm nothing when the paused latch is down" in {
    val (service, actorService) = newTimerService()

    service.resumeAdaptiveBatching()

    assert(actorService.captured.isEmpty)
    assert(service.adaptiveBatchingHandle.isEmpty)
  }

  it should "re-arm the flush timer while the paused latch is up" in {
    // The precondition (latch up, no live handle) is reached through the production
    // API alone: a worker paused before it ever emitted an output tuple has never
    // armed a timer, so `pauseAdaptiveBatching` raises the latch over an empty
    // handle. Note that start -> pause -> resume is deliberately NOT tested here:
    // `stopAdaptiveBatching` cancels the handle without clearing it, so after a
    // real start+pause resume cannot re-arm at all; pinning either that no-op or
    // its fix is out of scope for a test-only change (see the PR body).
    val (service, actorService) = newTimerService()

    service.pauseAdaptiveBatching()
    assert(service.isPaused)
    assert(service.adaptiveBatchingHandle.isEmpty)

    service.resumeAdaptiveBatching()

    assert(actorService.captured.size == 1)
    val resumed = actorService.captured.head
    assert(service.adaptiveBatchingHandle.contains(resumed.handle))
    assert(!resumed.handle.isCancelled)
    // The re-armed timer carries the same schedule and payload a start would.
    assert(resumed.initialDelay == 0.milliseconds)
    assert(
      resumed.delay == FiniteDuration(ApplicationConfig.adaptiveBufferingTimeoutMs, MILLISECONDS)
    )
    assert(resumed.msg == expectedFlushInvocation)
    // Characterization, not a specified contract: nothing in production reads
    // `isPaused`, and as the class stands `stopAdaptiveBatching` is its only
    // lowerer - resuming re-arms the timer and leaves the latch as it found it. If
    // the latch semantics are ever deliberately changed so that resume lowers it,
    // update this assertion.
    assert(service.isPaused)
  }

  it should "arm nothing after a stop has lowered the paused latch" in {
    val (service, actorService) = newTimerService()

    service.pauseAdaptiveBatching()
    service.stopAdaptiveBatching()

    service.resumeAdaptiveBatching()

    assert(!service.isPaused)
    assert(actorService.captured.isEmpty)
    assert(service.adaptiveBatchingHandle.isEmpty)
  }
}

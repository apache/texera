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

package org.apache.texera.web

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.AppenderBase
import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  RUNNING
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters._

class WorkflowLifecycleManagerSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // WorkflowLifecycleManager schedules through AmberRuntime's process-wide actor system.
  // Preserve that shared reference because amber suites run concurrently in one JVM.
  private lazy val testSystem: ActorSystem =
    ActorSystem("WorkflowLifecycleManagerSpec-test", AmberRuntime.pekkoConfig)

  private var previousActorSystem: AnyRef = _
  private var previousSerde: AnyRef = _

  private def getAmberRuntimeField(name: String): AnyRef = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(AmberRuntime)
  }

  private def setAmberRuntimeField(name: String, value: AnyRef): Unit = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(AmberRuntime, value)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    previousActorSystem = getAmberRuntimeField("_actorSystem")
    previousSerde = getAmberRuntimeField("_serde")
    setAmberRuntimeField("_actorSystem", testSystem)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_serde", previousSerde)
    setAmberRuntimeField("_actorSystem", previousActorSystem)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  private def managerWithCallback(
      cleanUpTimeout: Int = 1,
      id: String = "workflow-lifecycle-manager-spec"
  ): (WorkflowLifecycleManager, CountDownLatch) = {
    val cleaned = new CountDownLatch(1)
    val manager = new WorkflowLifecycleManager(
      id = id,
      cleanUpTimeout = cleanUpTimeout,
      cleanUpCallback = () => cleaned.countDown()
    )
    (manager, cleaned)
  }

  /** Counts the clean-ups instead of latching on the first one, so a repeat is observable. */
  private def managerWithCounter(
      cleanUpTimeout: Int = 1,
      id: String = "workflow-lifecycle-manager-spec"
  ): (WorkflowLifecycleManager, AtomicInteger) = {
    val cleanUps = new AtomicInteger(0)
    val manager = new WorkflowLifecycleManager(
      id = id,
      cleanUpTimeout = cleanUpTimeout,
      cleanUpCallback = () => { cleanUps.incrementAndGet(); () }
    )
    (manager, cleanUps)
  }

  private def assertCleanUpWithin(cleaned: CountDownLatch, seconds: Long): Unit = {
    assert(
      cleaned.await(seconds, TimeUnit.SECONDS),
      "cleanup callback was not invoked before the deadline"
    )
  }

  /**
    * Buffers the events a logger emits. logback's own ListAppender collects into a plain
    * ArrayList, and the clean-up body runs on the scheduler's execution context rather than on
    * the test thread, so the buffer has to tolerate appends from another thread.
    */
  private final class CollectingAppender extends AppenderBase[ILoggingEvent] {
    private val events = new ConcurrentLinkedQueue[ILoggingEvent]

    override def append(event: ILoggingEvent): Unit = events.add(event)

    def messages: Seq[String] = events.asScala.map(_.getFormattedMessage).toSeq
  }

  private val managerLoggerName = classOf[WorkflowLifecycleManager].getName

  /**
    * Runs `body` with the manager's logger raised to INFO, handing it a live view of the
    * messages logged for `id`.
    *
    * The manager reports every lifecycle decision at INFO, and amber's logback.xml pins
    * `org.apache` to WARN, so those calls sit behind `isInfoEnabled` and stay dormant unless a
    * test raises the level. Additivity is switched off so the captured lines do not also reach
    * the shared console and rolling-file appenders.
    */
  private def withInfoLogs[T](id: String)(body: (() => Seq[String]) => T): T = {
    val logger = LoggerFactory.getLogger(managerLoggerName).asInstanceOf[LogbackLogger]
    logger.synchronized {
      val appender = new CollectingAppender
      val previousLevel = logger.getLevel
      val previousAdditive = logger.isAdditive
      appender.setContext(logger.getLoggerContext)
      appender.setName(
        s"workflow-lifecycle-manager-spec-appender-$id-${Thread.currentThread().getId}"
      )
      appender.start()
      logger.addAppender(appender)
      logger.setLevel(Level.INFO)
      logger.setAdditive(false)
      try {
        body(() => appender.messages.filter(_.startsWith(s"[$id] ")))
      } finally {
        logger.detachAppender(appender)
        logger.setAdditive(previousAdditive)
        logger.setLevel(previousLevel)
        appender.stop()
      }
    }
  }

  /** Waits for `fragment` to show up, then returns everything logged so far. */
  private def awaitMessage(
      messages: () => Seq[String],
      fragment: String,
      seconds: Long
  ): Seq[String] = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
    while (System.nanoTime() < deadline && !messages().exists(_.contains(fragment))) {
      Thread.sleep(50)
    }
    val captured = messages()
    assert(
      captured.exists(_.contains(fragment)),
      s"no logged message contained '$fragment', got: $captured"
    )
    captured
  }

  private def awaitCleanUpCount(cleanUps: AtomicInteger, expected: Int, seconds: Long): Unit = {
    val deadline = System.currentTimeMillis() + seconds * 1000
    while (System.currentTimeMillis() < deadline && cleanUps.get() < expected) {
      Thread.sleep(50)
    }
    assert(cleanUps.get() == expected, s"expected $expected clean-up(s), saw ${cleanUps.get()}")
  }

  /**
    * The scheduled deadline, read straight off the manager.
    *
    * Used only as a non-vacuity probe: pekko marks a task that has already fired as neither
    * cancelled nor cancellable, while a task cancelled before it fired reports isCancelled. The
    * two "reconnects as the deadline fires" tests would otherwise pass on a runner so slow that
    * the deadline never fired at all.
    */
  private def scheduledCleanUp(manager: WorkflowLifecycleManager): Cancellable = {
    val field = classOf[WorkflowLifecycleManager].getDeclaredField("cleanUpExecution")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[Cancellable]
  }

  "WorkflowLifecycleManager" should "wait for the last user before scheduling cleanup" in {
    val (manager, cleaned) = managerWithCallback()

    manager.increaseUserCount()
    manager.increaseUserCount()
    manager.decreaseUserCount(Some(COMPLETED))

    assert(
      !cleaned.await(1500, TimeUnit.MILLISECONDS),
      "cleanup ran while a user was still present"
    )

    manager.decreaseUserCount(None)
    assertCleanUpWithin(cleaned, seconds = 5)
  }

  it should "cancel cleanup while the workflow is running and resume after completion" in {
    val (manager, cleaned) = managerWithCallback()
    val stateStore = new ExecutionStateStore

    manager.registerCleanUpOnStateChange(stateStore)
    stateStore.metadataStore.updateState(_.withState(RUNNING))

    assert(!cleaned.await(1500, TimeUnit.MILLISECONDS), "a running workflow must not be cleaned up")

    stateStore.metadataStore.updateState(_.withState(COMPLETED))
    assertCleanUpWithin(cleaned, seconds = 5)
  }

  it should "refresh the deadline when a later terminal state arrives" in {
    val id = "refresh-deadline"
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 4, id = id)
    val stateStore = new ExecutionStateStore

    withInfoLogs(id) { messages =>
      manager.registerCleanUpOnStateChange(stateStore)
      // Registration alone must arm a deadline: the replayed state is not RUNNING and no user is
      // connected. Asserting it is what makes the refresh below meaningful -- with no earlier
      // deadline there is nothing to replace, and the timing windows on their own cannot tell a
      // refreshed deadline from a first one armed at the update.
      assert(
        messages().count(_.contains("will start at")) == 1,
        s"registration did not arm a deadline: ${messages()}"
      )

      Thread.sleep(2000)
      val beforeUpdate = LocalDateTime.now()
      stateStore.metadataStore.updateState(_.withState(COMPLETED))
      val armed = messages().filter(_.contains("will start at"))
      assert(armed.size == 2, s"the terminal state did not re-arm the deadline: ${messages()}")

      // The refreshed deadline has to be cleanUpTimeout out, and this is the only test that reads
      // a logged deadline at a timeout other than 3, so it is what stops the logged deadline from
      // being satisfied by one hardcoded number of seconds.
      val refreshed =
        LocalDateTime.parse(armed.last.stripPrefix(s"[$id] workflow state clean up will start at "))
      assert(
        !refreshed.isBefore(beforeUpdate.plusSeconds(4)) &&
          refreshed.isBefore(beforeUpdate.plusSeconds(6)),
        s"the refreshed deadline $refreshed is not 4s past $beforeUpdate"
      )

      assert(
        !cleaned.await(3000, TimeUnit.MILLISECONDS),
        "a refreshed deadline must cancel the earlier one"
      )
      assertCleanUpWithin(cleaned, seconds = 5)
    }
  }

  it should "postpone the clean-up while a user is connected, reporting the count and status" in {
    val id = "postpone-with-user"
    // cleanUpTimeout must differ from every user count this test reads, or the manager's two Int
    // fields render interchangeably and the count interpolants are unpinned.
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 3, id = id)
    val stateStore = new ExecutionStateStore

    val messages = withInfoLogs(id) { messages =>
      manager.increaseUserCount()
      manager.increaseUserCount()
      // metadataStore is a BehaviorSubject, so subscribing replays the current state
      // (UNINITIALIZED) synchronously on this thread.
      manager.registerCleanUpOnStateChange(stateStore)
      // UNINITIALIZED is the protobuf default the replay hands over for free, so a status
      // interpolant hardcoded to it would render identically. Push a state the manager cannot
      // have defaulted to: userCount > 0 short-circuits the guard, so COMPLETED still takes the
      // postpone arm and the status has to be reported as COMPLETED.
      stateStore.metadataStore.updateState(_.withState(COMPLETED))
      // Drop back to one user and push a third state, so the status line is read at two different
      // counts and three different statuses. A mutant that hardcodes whichever count or status
      // this test happens to expect cannot render all of them.
      manager.decreaseUserCount(Some(RUNNING))
      stateStore.metadataStore.updateState(_.withState(RUNNING))
      messages()
    }

    assert(
      messages == Seq(
        s"[$id] workflow state clean up postponed. current user count = 1",
        s"[$id] workflow state clean up postponed. current user count = 2",
        s"[$id] workflow state clean up postponed. current user count = 2, " +
          "workflow status = UNINITIALIZED",
        s"[$id] workflow state clean up postponed. current user count = 2, " +
          "workflow status = COMPLETED",
        s"[$id] workflow state clean up postponed. current user count = 1",
        s"[$id] workflow state clean up postponed. current user count = 1, " +
          "workflow status = RUNNING"
      ),
      s"unexpected lifecycle log: $messages"
    )
    assert(
      !cleaned.await(3500, TimeUnit.MILLISECONDS),
      "no deadline may be armed while a user is connected"
    )
  }

  it should "log the deadline it arms and the completion of the clean-up" in {
    val id = "log-deadline"
    // 3 seconds, not 1: the window below has to be narrow enough to reject an arithmetic slip in
    // the logged deadline, and 3 does not collide with the user count this test reads.
    val cleanUpTimeout = 3
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = cleanUpTimeout, id = id)

    val armedAt = LocalDateTime.now()
    val (messages, firedAt) = withInfoLogs(id) { messages =>
      manager.increaseUserCount()
      manager.decreaseUserCount(None)
      assertCleanUpWithin(cleaned, seconds = 6)
      val firedAt = LocalDateTime.now()
      // the completion is logged after the callback returns
      (awaitMessage(messages, "clean up completed.", seconds = 5), firedAt)
    }

    // The expected wording is a literal, not the production string echoed back: comparing
    // `armed.head` against itself would move both sides of the assertion together.
    val armedPrefix = s"[$id] workflow state clean up will start at "
    val armed = messages.filter(_.contains("will start at"))
    assert(armed.size == 1, s"expected exactly one armed deadline, got: $messages")
    assert(
      armed.head.startsWith(armedPrefix),
      s"unexpected armed-deadline wording: ${armed.head}"
    )
    val loggedDeadline = LocalDateTime.parse(armed.head.stripPrefix(armedPrefix))
    assert(
      !loggedDeadline.isBefore(armedAt.plusSeconds(cleanUpTimeout.toLong)),
      s"the logged deadline $loggedDeadline is less than ${cleanUpTimeout}s past $armedAt"
    )
    assert(
      loggedDeadline.isBefore(armedAt.plusSeconds(cleanUpTimeout + 2L)),
      s"the logged deadline $loggedDeadline is more than ${cleanUpTimeout + 2}s past $armedAt"
    )
    // The advertised deadline and the deadline actually armed are separate expressions in
    // refreshDeadline, so cross-check them: the clean-up must fire at the instant it advertised.
    // The 200ms of slack on the lower bound absorbs the pekko wheel's tick rounding and the
    // nanoTime/wall-clock mismatch; it is far tighter than any plausible arithmetic slip.
    assert(
      !firedAt.isBefore(loggedDeadline.minusNanos(200000000L)),
      s"the clean-up fired at $firedAt, before the advertised $loggedDeadline"
    )
    assert(
      firedAt.isBefore(loggedDeadline.plusSeconds(1)),
      s"the clean-up fired at $firedAt, well past the advertised $loggedDeadline"
    )
    assert(
      messages == Seq(
        s"[$id] workflow state clean up postponed. current user count = 1",
        armedPrefix + loggedDeadline.toString,
        s"[$id] workflow state clean up completed."
      ),
      s"unexpected lifecycle log: $messages"
    )
  }

  it should "schedule the clean-up when the last user leaves a finished workflow" in {
    val (manager, cleaned) = managerWithCallback(id = "finished-workflow")

    manager.increaseUserCount()
    manager.decreaseUserCount(Some(COMPLETED))

    assertCleanUpWithin(cleaned, seconds = 5)
  }

  it should "keep a still-running workflow alive when its last user leaves" in {
    val id = "running-workflow"
    // 5 collides with none of the counts below (0, 1, 2), so no count interpolant can be swapped
    // for the timeout and still render.
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 5, id = id)

    val messages = withInfoLogs(id) { messages =>
      manager.increaseUserCount()
      manager.increaseUserCount()
      // 2 -> 1 takes the postpone arm because the count is not zero yet; 1 -> 0 takes it because
      // the workflow is still RUNNING. Reading that line at a NONZERO count is the point: at zero
      // it absorbs multiplicative mutations, so a count read only at zero is not pinned at all.
      manager.decreaseUserCount(Some(RUNNING))
      manager.decreaseUserCount(Some(RUNNING))
      messages()
    }

    // The exhaustive comparison is also what proves nothing was armed: an armed deadline would
    // add a "will start at" line here.
    assert(
      messages == Seq(
        s"[$id] workflow state clean up postponed. current user count = 1",
        s"[$id] workflow state clean up postponed. current user count = 2",
        s"[$id] workflow state clean up postponed. current user count = 1",
        s"[$id] workflow state clean up postponed. current user count = 0"
      ),
      s"unexpected lifecycle log: $messages"
    )
    assert(
      !cleaned.await(2, TimeUnit.SECONDS),
      "a running workflow must not be cleaned up when its last user leaves"
    )
  }

  it should "cancel the pending deadline when a user reconnects before it fires" in {
    val id = "reconnect-before-deadline"
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 3, id = id)

    withInfoLogs(id) { messages =>
      manager.increaseUserCount()
      manager.decreaseUserCount(None)
      // Well inside the 3s deadline, so the task below is certainly still pending.
      Thread.sleep(500)
      manager.increaseUserCount()
      // increaseUserCount cancels the armed deadline outright. pekko marks a task cancelled
      // before it fired as isCancelled; a task still pending, or one that already fired, is not.
      // So this holds only if the reconnect really cancelled the live task.
      assert(
        scheduledCleanUp(manager).isCancelled,
        "the pending deadline was not cancelled by the reconnect"
      )
      assert(
        !messages().exists(_.contains("clean up failed.")),
        s"a cancelled deadline still reached the clean-up: ${messages()}"
      )

      // And the cancellation leaves the manager able to arm a replacement.
      manager.decreaseUserCount(None)
      assertCleanUpWithin(cleaned, seconds = 6)
    }
  }

  it should "decline the clean-up when a user reconnects as the deadline fires" in {
    val id = "reconnect-at-deadline"
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 1, id = id)

    val messages = withInfoLogs(id) { messages =>
      manager.increaseUserCount()
      manager.decreaseUserCount(None)
      manager.synchronized {
        // The deadline fires while this monitor is held: the scheduler marks the task as run and
        // hands the clean-up to the execution context, where it blocks until the reconnect below
        // has been recorded.
        Thread.sleep(2000)
        // Two reconnects, so the reported count (2) differs from cleanUpTimeout (which has to
        // stay at 1 for the 2000ms hold to straddle the deadline). Otherwise the declined-clean-up
        // message could interpolate either field and render the same.
        manager.increaseUserCount()
        manager.increaseUserCount()
        assert(
          !scheduledCleanUp(manager).isCancelled,
          "the deadline was cancelled instead of fired, so nothing raced the reconnect"
        )
      }
      awaitMessage(messages, "clean up failed.", seconds = 5)
    }

    assert(
      messages.contains(s"[$id] workflow state clean up failed. current user count = 2"),
      s"the declined clean-up was not reported: $messages"
    )
    assert(
      !messages.exists(_.contains("clean up completed.")),
      s"the clean-up completed even though a user had reconnected: $messages"
    )
    assert(cleaned.getCount == 1, "the clean-up callback ran even though a user had reconnected")
  }

  it should "decline the clean-up at the logger's default level as well" in {
    val id = "reconnect-at-deadline-quiet"
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 1, id = id)

    manager.increaseUserCount()
    manager.decreaseUserCount(None)
    manager.synchronized {
      Thread.sleep(2000)
      manager.increaseUserCount()
      assert(
        !scheduledCleanUp(manager).isCancelled,
        "the deadline was cancelled instead of fired, so nothing raced the reconnect"
      )
    }

    assert(
      !cleaned.await(3, TimeUnit.SECONDS),
      "the clean-up ran even though a user had reconnected"
    )
  }

  it should "arm no replacement deadline after a clean-up has run, because the manager is single-use" in {
    val (manager, cleanUps) = managerWithCounter(cleanUpTimeout = 1, id = "single-clean-up")

    manager.increaseUserCount()
    manager.decreaseUserCount(None)
    awaitCleanUpCount(cleanUps, expected = 1, seconds = 5)

    // Single-use is not a designed guarantee of this class; it falls out of two facts outside it.
    // (1) cleanUp leaves cleanUpExecution pointing at the task that just fired -- it never resets
    // the field. (2) pekko's LightArrayRevolverScheduler reports a fired task as neither cancelled
    // nor cancellable, so refreshDeadline's `isCancelled || cancel()` guard is false from then on.
    // Production never notices, because cleanUpCallback drops the WorkflowService from
    // workflowServiceMapping and a reconnect builds a fresh manager.
    //
    // This is therefore a description of the current single-use contract, NOT a statement that
    // reuse would be wrong. Anyone making WorkflowLifecycleManager reusable, and anyone hitting a
    // change in pekko's post-fire Cancellable semantics, should expect to rewrite this test rather
    // than treat it as a specification.
    manager.increaseUserCount()
    manager.decreaseUserCount(None)
    Thread.sleep(3000)

    assert(cleanUps.get() == 1, s"the clean-up ran ${cleanUps.get()} times, expected once")
  }
}

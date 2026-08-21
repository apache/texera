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

package org.apache.texera.amber.engine.common

import com.typesafe.config.Config
import org.apache.pekko.actor.{ActorRef, ActorSystem, Address, DeadLetter}
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.clustering.ClusterListener
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Try

/**
  * Characterizes [[AmberRuntime]] — the process-wide holder of the engine's Pekko
  * `ActorSystem` and `Serialization`, and the code that builds them.
  *
  * What this spec catches:
  *   - `serde` losing its memoization, or bootstrapping a system when one is already
  *     installed (either would hand different halves of the engine different
  *     serializers, and the bootstrap branch leaks an untracked cluster system),
  *   - the schedule helpers ignoring their delays, or handing back a `Cancellable`
  *     that does not actually cancel (the reconfiguration and monitoring paths rely
  *     on both),
  *   - a regression in `startActorWorker`'s config assembly: this is the entire
  *     startup path of `ComputingUnitWorker`, and the seed-node URI, the artery
  *     hostname and the `withFallback(pekkoConfig)` are what make a worker join the
  *     right cluster at all,
  *   - `createAmberSystem` renaming/dropping the `cluster-info` or
  *     `dead-letter-monitor-actor` children, or dropping the `DeadLetter`
  *     subscription — that last one silently disables dead-letter forwarding, with
  *     nothing failing anywhere.
  *
  * Deliberately NOT covered:
  *   - `getNodeIpAddress` opens `http://checkip.amazonaws.com` with no parameter,
  *     no overridable member and no config knob to redirect it, so neither its
  *     success nor its (no-op rethrow) failure arm is reachable from a test. That
  *     also makes the `mainNodeAddress.isDefined` arm of `startActorWorker`
  *     untestable, hence `startActorWorker(None)` throughout.
  *   - `startActorMaster` binds artery on the FIXED port 2552 and lists itself as a
  *     seed node, so the node self-joins and reaches Up. That fires a real
  *     `MemberUp` into `ClusterListener.updateClusterStatus`, which fans a
  *     `ClusterStatusUpdateEvent` out to every entry of the JVM-global
  *     `SessionState` registry — and amber runs its suites in ONE shared JVM, where
  *     `ClusterListenerSpec` holds live per-test ScalaMock sessions in exactly that
  *     registry. `ComputingUnitMasterSpec` refuses this path for the same reason.
  *     `createMasterAddress` is still covered, via `startActorWorker`.
  *
  * JVM-global state: this object IS the global other amber suites depend on, and sbt
  * runs them concurrently in one JVM without forking. Every global this spec touches
  * (`_actorSystem`, `_serde`, `AmberConfig.masterNodeAddr`,
  * `ClusterListener.numWorkerNodesInCluster`) is saved in `beforeAll` and put back in
  * `afterAll` — the save/restore camp of `HDFSRecordStorageSpec` /
  * `ComputingUnitMasterSpec`, not the null-out camp, because nulling makes
  * `AmberRuntime.serde` take its bootstrap branch mid-run inside a sibling suite and
  * spin up a system nobody shuts down. Tests that have to write those globals
  * themselves restore them in a `finally` rather than deferring to `afterAll`, so the
  * write window is one test long.
  */
class AmberRuntimeSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // `lazy` matters: BeforeAndAfterAll skips beforeAll/afterAll entirely when the
  // active filter selects no test from this suite (which is what the
  // amber-integration CI job does via AMBER_TEST_FILTER). A strict `val` would
  // still build an ActorSystem during suite construction and never shut it down.
  private lazy val testSystem: ActorSystem =
    ActorSystem("AmberRuntimeSpec-test", AmberRuntime.pekkoConfig)
  private lazy val testSerde: Serialization = SerializationExtension(testSystem)

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

  private var previousActorSystem: AnyRef = _
  private var previousSerde: AnyRef = _
  private var previousMasterNodeAddr: Address = _
  private var previousNodeCount: Int = 0

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    previousActorSystem = getAmberRuntimeField("_actorSystem")
    previousSerde = getAmberRuntimeField("_serde")
    previousMasterNodeAddr = AmberConfig.masterNodeAddr
    previousNodeCount = ClusterListener.numWorkerNodesInCluster
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_serde", previousSerde)
    setAmberRuntimeField("_actorSystem", previousActorSystem)
    AmberConfig.masterNodeAddr = previousMasterNodeAddr
    // Belt and braces: the worker system's seed list points at localhost:2552, so on
    // a host that happens to be running a local coordinator it could in principle
    // join and have ClusterListener bump this count.
    ClusterListener.numWorkerNodesInCluster = previousNodeCount
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  /**
    * Installs `actorSystem` / `serde` into the two private statics for the duration of
    * `body`, then puts the suite's own pair back.
    *
    * Restoring inside the test rather than in `afterAll` keeps the window in which a
    * sibling suite could observe a null (and take the leaking bootstrap branch) to the
    * length of one test.
    */
  private def withGlobals(actorSystem: AnyRef, serde: AnyRef)(body: => Unit): Unit = {
    setAmberRuntimeField("_actorSystem", actorSystem)
    setAmberRuntimeField("_serde", serde)
    try body
    finally {
      setAmberRuntimeField("_actorSystem", testSystem)
      setAmberRuntimeField("_serde", testSerde)
    }
  }

  // ---------------------------------------------------------------------------
  // serde
  // ---------------------------------------------------------------------------

  "AmberRuntime.serde" should "bootstrap its own Amber system when none is installed" in {
    // The bootstrap branch is what any engine component that serializes before
    // startActorWorker/startActorMaster has run ends up on.
    withGlobals(null, null) {
      val serde = AmberRuntime.serde
      assert(serde != null)
      assert(serde.system.name == "Amber")
      // The bootstrapped system is assigned to _serde ONLY - _actorSystem stays
      // null, so AmberRuntime.actorSystem keeps returning null afterwards. Pinning
      // the quirk because it is the reason this branch leaks a system unless the
      // caller reaches it back through `serde.system`, which is what we do below.
      assert(getAmberRuntimeField("_actorSystem") == null)
      TestKit.shutdownActorSystem(serde.system)
    }
  }

  it should "extend the installed actor system when one is present" in {
    withGlobals(testSystem, null) {
      // `eq`, not `==`: the contract is that the serde is built over the SAME system
      // the rest of the engine uses. A bootstrap here would produce a serde that
      // cannot deserialize actor refs from the installed system.
      assert(AmberRuntime.serde.system eq testSystem)
    }
  }

  it should "memoize, returning the installed Serialization untouched" in {
    // _actorSystem is null on purpose: with the memoization guard dropped, the call
    // falls into the bootstrap branch and returns a Serialization over a brand-new
    // system, so the identity check below is what fails.
    withGlobals(null, testSerde) {
      assert(AmberRuntime.serde eq testSerde)
      assert(getAmberRuntimeField("_serde") eq testSerde)
    }
  }

  // ---------------------------------------------------------------------------
  // actorSystem
  // ---------------------------------------------------------------------------

  "AmberRuntime.actorSystem" should "hand back the installed system" in {
    withGlobals(testSystem, testSerde) {
      assert(AmberRuntime.actorSystem eq testSystem)
    }
  }

  it should "be null before a system has been installed" in {
    // Not redundant with the case above: callers such as ComputingUnitMaster read
    // this before startup to decide whether the runtime is up at all. It is also the
    // only assertion here that a serde-derived implementation could not satisfy -
    // _serde is populated below, so `_serde.system` would answer testSystem.
    withGlobals(null, testSerde) {
      assert(AmberRuntime.actorSystem == null)
    }
  }

  // ---------------------------------------------------------------------------
  // scheduleCallThroughActorSystem
  // ---------------------------------------------------------------------------

  "AmberRuntime.scheduleCallThroughActorSystem" should "run the call once the delay elapses" in {
    val latch = new CountDownLatch(1)
    val cancellable = AmberRuntime.scheduleCallThroughActorSystem(100.millis)(latch.countDown())
    try assert(latch.await(5, java.util.concurrent.TimeUnit.SECONDS), "scheduled call never ran")
    finally cancellable.cancel()
  }

  it should "wait out the delay and stop firing once cancelled" in {
    // The case above alone is a one-sided pin: it stays green if the delay is
    // ignored, and green if the returned Cancellable is inert. Both halves are
    // pinned here, and the second window deliberately runs PAST the 1s delay so an
    // inert Cancellable would let the call through.
    val fired = new AtomicInteger(0)
    val cancellable = AmberRuntime.scheduleCallThroughActorSystem(1.second)(fired.incrementAndGet())
    try {
      Thread.sleep(300)
      assert(fired.get() == 0, "the call ran before its delay elapsed")
      cancellable.cancel()
      Thread.sleep(1200)
      assert(fired.get() == 0, "the call ran after its Cancellable was cancelled")
    } finally cancellable.cancel()
  }

  // ---------------------------------------------------------------------------
  // scheduleRecurringCallThroughActorSystem
  // ---------------------------------------------------------------------------

  "AmberRuntime.scheduleRecurringCallThroughActorSystem" should
    "honor the initial delay and then repeat at the fixed delay" in {
    // The two durations are deliberately UNEQUAL, so they are pinned separately:
    // with the arguments swapped (100ms initial, 1s repeat) the first assertion
    // sees a fire inside 400ms, and with no repeat at all the count stops at 1.
    val fired = new AtomicInteger(0)
    val cancellable = AmberRuntime.scheduleRecurringCallThroughActorSystem(
      initialDelay = 1.second,
      delay = 100.millis
    )(fired.incrementAndGet())
    try {
      Thread.sleep(400)
      assert(fired.get() == 0, "the recurring call ran before its initial delay elapsed")
      val deadline = System.nanoTime() + 4.seconds.toNanos
      while (fired.get() < 3 && System.nanoTime() < deadline) Thread.sleep(50)
      assert(fired.get() >= 3, s"expected repeated fires, saw ${fired.get()}")
    } finally cancellable.cancel()
  }

  // ---------------------------------------------------------------------------
  // startActorWorker(None) -> createAmberSystem
  // ---------------------------------------------------------------------------

  /**
    * Everything one `startActorWorker(None)` invocation makes observable.
    *
    * The whole startup costs about 175ms, so it is paid once and the observations
    * are captured while the created system is alive; the system is shut down inside
    * this block so its cluster membership window stays as short as production's own
    * startup, and the globals it overwrites are restored here rather than in
    * `afterAll`.
    *
    * The two child lookups are captured as `Option`s rather than asserted here so a
    * rename fails only the case that is about that name.
    */
  private case class WorkerStartup(
      systemName: String,
      config: Config,
      masterNodeAddr: Address,
      rawSerdeAfterCall: AnyRef,
      serdeSystemIsCreatedSystem: Boolean,
      clusterInfoPath: Option[String],
      deadLetterMonitorPath: Option[String],
      deadLetterResubscribeWasNew: Option[Boolean]
  )

  private lazy val workerStartup: WorkerStartup = {
    // A sentinel is mandatory: AmberConfig.masterNodeAddr's DEFAULT is already
    // Address("pekko", "Amber", "localhost", 2552), so without this the
    // masterNodeAddr case would assert a value that held before the call.
    AmberConfig.masterNodeAddr = Address("pekko", "Sentinel", "nowhere", 1)
    setAmberRuntimeField("_actorSystem", null)
    setAmberRuntimeField("_serde", null)
    var created: ActorSystem = null
    try {
      AmberRuntime.startActorWorker(None)
      // Read the raw field, not the AmberRuntime.actorSystem getter, so this fixture
      // does not depend on the accessor that other cases here are pinning.
      created = getAmberRuntimeField("_actorSystem").asInstanceOf[ActorSystem]
      WorkerStartup(
        systemName = created.name,
        config = created.settings.config,
        masterNodeAddr = AmberConfig.masterNodeAddr,
        // Captured BEFORE anything calls AmberRuntime.serde: with _actorSystem now
        // set, the getter would lazily rebuild an equivalent serde over the same
        // system, so asserting through it would pass even with the write deleted.
        rawSerdeAfterCall = getAmberRuntimeField("_serde"),
        // Identity, not the system's name: a serde built over a different system that happens to
        // be called "Amber" would satisfy a name comparison.
        serdeSystemIsCreatedSystem = getAmberRuntimeField("_serde") match {
          case serde: Serialization => serde.system eq created
          case _                    => false
        },
        clusterInfoPath = resolveChild(created, "/user/cluster-info").map(_.path.toString),
        deadLetterMonitorPath =
          resolveChild(created, "/user/dead-letter-monitor-actor").map(_.path.toString),
        deadLetterResubscribeWasNew = resolveChild(created, "/user/dead-letter-monitor-actor")
          .map(ref => created.eventStream.subscribe(ref, classOf[DeadLetter]))
      )
    } finally {
      setAmberRuntimeField("_actorSystem", testSystem)
      setAmberRuntimeField("_serde", testSerde)
      if (created != null) TestKit.shutdownActorSystem(created)
    }
  }

  private def resolveChild(system: ActorSystem, path: String): Option[ActorRef] =
    Try(Await.result(system.actorSelection(path).resolveOne(5.seconds), 5.seconds)).toOption

  "AmberRuntime.startActorWorker" should "bind artery on the local host" in {
    // cluster.conf's own default hostname is "0.0.0.0", so this catches both a dropped
    // hostname override and one that reaches for getNodeIpAddress on the None arm.
    // The sibling `canonical.port = 0` override is deliberately NOT asserted: cluster.conf
    // already defaults that key to 0, so removing the override changes no observable
    // value and any assertion on it would be unfalsifiable (verified by mutation).
    assert(workerStartup.config.getString("pekko.remote.artery.canonical.hostname") == "localhost")
  }

  it should "seed the cluster from the master node's URI" in {
    // cluster.conf ships `seed-nodes = []`, so the whole list is this method's work.
    // The host segment comes from `mainNodeAddress.getOrElse("localhost")`, which is
    // the only place the None arm's default is observable.
    assert(
      workerStartup.config
        .getStringList("pekko.cluster.seed-nodes")
        .asScala
        .toList == List("pekko://Amber@localhost:2552")
    )
  }

  it should "keep the rest of pekkoConfig underneath the overrides" in {
    // The parsed override block mentions neither of these; both come from
    // cluster.conf through withFallback. Without the fallback the worker would
    // silently start on the local (non-cluster) provider and never join anything.
    assert(workerStartup.config.getString("pekko.actor.provider") == "cluster")
    // The kryo serializer, not artery's transport: `transport = tcp` is pekko's own
    // reference.conf default, so asserting it holds even with pekkoConfig dropped from the chain.
    // This binding exists only in cluster.conf.
    assert(
      workerStartup.config.getString("pekko.actor.serializers.kryo") ==
        "io.altoo.serialization.kryo.pekko.PekkoKryoSerializer"
    )
  }

  it should "publish the master node address" in {
    // Characterization, not a live contract: `AmberConfig.masterNodeAddr` is written
    // here and by startActorMaster and read nowhere in the repo. It is asserted
    // because it is the only observable output of `createMasterAddress`, so the
    // address this method derives from `mainNodeAddress` stays pinned in case a
    // reader comes back.
    assert(workerStartup.masterNodeAddr == Address("pekko", "Amber", "localhost", 2552))
  }

  "AmberRuntime.createAmberSystem" should "name the system Amber and attach both children" in {
    // Paths, which is what callers depend on: DeployStrategies and the frontend's cluster badge
    // reach these actors by path, so a rename breaks them with no compile error.
    //
    // What this does NOT pin: which class is mounted at each path. Swapping ClusterListener for
    // DeadLetterMonitorActor while keeping the name leaves this green, because an ActorRef exposes
    // no class and nothing here exchanges a message only one of them answers. Recorded rather than
    // chased -- pinning it would mean giving the production code a seam it does not otherwise need.
    assert(workerStartup.systemName == "Amber")
    assert(workerStartup.clusterInfoPath.contains("pekko://Amber/user/cluster-info"))
    assert(
      workerStartup.deadLetterMonitorPath.contains("pekko://Amber/user/dead-letter-monitor-actor")
    )
  }

  it should "subscribe the dead-letter monitor to DeadLetter" in {
    // `subscribe` answers false when the ref is already subscribed, so re-subscribing
    // is both a no-op and the only observable proof the production call happened.
    assert(
      workerStartup.deadLetterResubscribeWasNew.contains(false),
      "dead-letter monitor was not already subscribed to DeadLetter"
    )
  }

  it should "install a serde over the system it created" in {
    assert(workerStartup.rawSerdeAfterCall != null)
    assert(
      workerStartup.serdeSystemIsCreatedSystem,
      "the installed serde is bound to the very system startActorWorker created"
    )
  }
}

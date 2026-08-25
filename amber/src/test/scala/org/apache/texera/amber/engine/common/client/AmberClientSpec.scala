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

import com.twitter.util.{Await => TwitterAwait, Duration => TwitterDuration}
import org.apache.pekko.actor.{ActorRef, ActorSystem, Address, UnhandledMessage}
import org.apache.pekko.pattern.StatusReply.Ack
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.texera.amber.core.virtualidentity.ChannelIdentity
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{NetworkAck, NetworkMessage}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  WorkflowRecoveryStatus
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.ambermessage.{
  NotifyFailedNode,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload,
  WorkflowRecoveryMessage
}
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

/**
  * Unit test for [[AmberClient]].
  *
  * This spec lives in `...engine.common.client` on purpose: `ClientActor` and its
  * companion messages are `private[client]`, so the event-delivery tests below could
  * not be written from any other package.
  *
  * Every client is built over the empty plan
  * (`PhysicalPlan(Set.empty, Set.empty)` + `CoordinatorConfig(None, None, None, None)`),
  * the recipe three `org.apache.texera.web.service` specs already use: the constructor
  * blocks on an `InitializeRequest`, which spawns a real `Coordinator` child, and only
  * an empty plan makes that complete without an engine. Clients are always shut down
  * in a `finally` -- amber's suites share one serially-run JVM, so a leaked client
  * would leave live actors behind for every later suite.
  */
class AmberClientSpec
    extends TestKit(ActorSystem("AmberClientSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  private val failedNode = Address("pekko", "AmberClientSpec", "127.0.0.1", 2552)

  /**
    * `AmberClient` declares `implicit val timeout = Timeout(1.minute)` for the ask in
    * `notifyNodeFailure`, so a reply that never arrives would stall the suite for a
    * full minute before ScalaTest reports an unnamed failure. Every await below uses
    * this short explicit bound instead.
    */
  private val awaitTimeout: TwitterDuration = TwitterDuration.fromSeconds(5)

  private def newClient(
      actorSystem: ActorSystem = system,
      errorHandler: Throwable => Unit = _ => ()
  ): AmberClient =
    new AmberClient(
      actorSystem,
      new WorkflowContext(),
      PhysicalPlan(Set.empty, Set.empty),
      CoordinatorConfig(None, None, None, None),
      errorHandler
    )

  private def withClient(body: AmberClient => Unit): Unit = {
    val client = newClient()
    try body(client)
    finally client.shutdown()
  }

  // ---------------------------------------------------------------------------
  // notifyNodeFailure
  // ---------------------------------------------------------------------------

  "notifyNodeFailure" should "forward the failed node's address to the coordinator and complete with Ack" in {
    // `ClientActor` replies Ack to ANY WorkflowRecoveryMessage, so the Ack on its own
    // says nothing about what was sent. The `Coordinator` it forwards to has no arm
    // for WorkflowRecoveryMessage (`WorkflowActor.receive` is a fixed orElse-chain
    // with no catch-all), so pekko republishes the forwarded message verbatim on the
    // event stream -- which is how the address can be observed from outside a client
    // whose `clientActor` field is class-private.
    val unhandled = TestProbe()
    system.eventStream.subscribe(unhandled.ref, classOf[UnhandledMessage])
    try {
      withClient { client =>
        TwitterAwait.result(client.notifyNodeFailure(failedNode), awaitTimeout) shouldBe Ack

        val forwarded = unhandled.fishForSpecificMessage[UnhandledMessage](10.seconds) {
          case msg @ UnhandledMessage(_: WorkflowRecoveryMessage, _, _) => msg
        }
        forwarded.message shouldBe WorkflowRecoveryMessage(CLIENT, NotifyFailedNode(failedNode))
      }
    } finally system.eventStream.unsubscribe(unhandled.ref)
  }

  it should "return an already-satisfied unit future once the client has been shut down" in {
    val client = newClient()
    client.shutdown()

    val result = client.notifyNodeFailure(failedNode)

    // Short-circuited, not asked: the actor took a PoisonPill in `shutdown()`, so an
    // ask would sit unanswered until the one-minute timeout instead of resolving now.
    result.isDefined shouldBe true
    TwitterAwait.result(result, awaitTimeout) shouldBe ((): Unit)
  }

  // ---------------------------------------------------------------------------
  // shutdown
  // ---------------------------------------------------------------------------

  "shutdown" should "stop the client actor" in {
    val ownSystem = ActorSystem("AmberClientSpecShutdown")
    try {
      val client = newClient(ownSystem)
      // Same addressing trick as `withIsolatedClient` below.
      val clientActor: ActorRef = Await.result(
        ownSystem.actorSelection("/user/$a").resolveOne(5.seconds),
        5.seconds
      )
      val watcher = TestProbe()(ownSystem)
      watcher.watch(clientActor)

      client.shutdown()

      // `shutdown()` has to actually stop the actor and not merely flip `isActive`:
      // the ClientActor owns a live Coordinator child tree, and amber's suites share
      // one JVM, so a client that only flipped the flag would leak that whole tree
      // into every later suite. The two "once the client has been shut down" tests
      // observe the flag; this one observes the stop.
      watcher.expectTerminated(clientActor, 10.seconds)
    } finally TestKit.shutdownActorSystem(ownSystem)
  }

  // ---------------------------------------------------------------------------
  // registerCallback
  // ---------------------------------------------------------------------------

  "registerCallback" should "throw once the client has been shut down" in {
    val client = newClient()
    client.shutdown()

    val thrown = intercept[RuntimeException] {
      client.registerCallback[ExecutionStateUpdate](_ => ())
    }
    thrown.getMessage shouldBe "amber runtime environment is not active"
  }

  it should "return the subscription handle, so disposing it silences that callback alone" in {
    withIsolatedClient("AmberClientSpecDispose") { (client, deliver) =>
      val disposedFired = new AtomicInteger(0)
      val liveFired = new CountDownLatch(1)

      val disposed = client.registerCallback[ExecutionStateUpdate] { _ =>
        disposedFired.incrementAndGet()
        ()
      }
      val live = client.registerCallback[ExecutionStateUpdate](_ => liveFired.countDown())

      disposed.isDisposed shouldBe false
      disposed.dispose()
      disposed.isDisposed shouldBe true
      live.isDisposed shouldBe false

      deliver(ExecutionStateUpdate(WorkflowAggregatedState.RUNNING))

      // The returned value has to BE the subscription: any freshly built Disposable
      // satisfies the isDisposed assertions above, but only the real handle
      // unsubscribes the callback. The subject emits to its observers in subscription
      // order, so the still-live callback firing means the disposed one has already
      // had its chance.
      assert(liveFired.await(10, TimeUnit.SECONDS), "the still-subscribed callback never fired")
      disposedFired.get() shouldBe 0
    }
  }

  it should "deliver each client event to every callback registered for that type" in {
    withIsolatedClient("AmberClientSpecFanOut") { (client, deliver) =>
      val bothFired = new CountDownLatch(2)
      val seen = new ConcurrentLinkedQueue[String]

      // Two callbacks for the SAME event class. The second registration must reuse
      // the observable the first one created; if it built a fresh subject and
      // registered a second partial function on the actor instead, the actor's
      // `pf orElse handlers` chain would route the event to the newer function only
      // and the first callback would never fire.
      client.registerCallback[ExecutionStateUpdate] { evt =>
        seen.add(s"first:${evt.state}")
        bothFired.countDown()
      }
      client.registerCallback[ExecutionStateUpdate] { evt =>
        seen.add(s"second:${evt.state}")
        bothFired.countDown()
      }
      // A callback for a different class must not see this event.
      client.registerCallback[WorkflowRecoveryStatus] { _ =>
        seen.add("other-type")
        ()
      }

      deliver(ExecutionStateUpdate(WorkflowAggregatedState.RUNNING))

      assert(
        bothFired.await(10, TimeUnit.SECONDS),
        s"both callbacks should have fired, saw: ${seen.asScala.toList}"
      )
      seen.asScala.toSet shouldBe Set("first:RUNNING", "second:RUNNING")
    }
  }

  it should "route an exception thrown by a callback to the error handler and keep the subscription alive" in {
    val handled = new ConcurrentLinkedQueue[Throwable]
    val seen = new ConcurrentLinkedQueue[String]
    val handlerCalledTwice = new CountDownLatch(2)

    withIsolatedClient(
      "AmberClientSpecErrorHandler",
      errorHandler = t => { handled.add(t); handlerCalledTwice.countDown() }
    ) { (client, deliver) =>
      client.registerCallback[ExecutionStateUpdate] { evt =>
        seen.add(evt.state.toString)
        throw new IllegalStateException(s"callback blew up on ${evt.state}")
      }

      // Two events on purpose. The catch has two jobs: route the failure to the
      // errorHandler AND swallow it. If it rethrew, RxJava's LambdaObserver would
      // dispose the subscription on the first escaping onNext and the callback would
      // be silently unsubscribed forever -- so the second event is what proves the
      // swallow, and a single-event test cannot see that regression at all.
      deliver(ExecutionStateUpdate(WorkflowAggregatedState.RUNNING))
      deliver(ExecutionStateUpdate(WorkflowAggregatedState.FAILED))

      assert(
        handlerCalledTwice.await(10, TimeUnit.SECONDS),
        s"error handler saw only: ${handled.asScala.toList}"
      )
      seen.asScala.toList shouldBe List("RUNNING", "FAILED")
      handled.asScala.toList.map(_.getMessage) shouldBe List(
        "callback blew up on RUNNING",
        "callback blew up on FAILED"
      )
    }
  }

  // ---------------------------------------------------------------------------
  // fixtures
  // ---------------------------------------------------------------------------

  /**
    * Runs `body` against a client that owns a private [[ActorSystem]], together with a
    * function that delivers a payload to that client's `ClientActor`.
    *
    * `AmberClient.clientActor` is class-private, so the actor has to be addressed by
    * path. It is created with an unnamed `system.actorOf`, which makes it `/user/$a` in
    * a system where nothing else has been created under `/user` (pekko's TestKit puts
    * its own probes under `/system`). A private system per test keeps that name
    * deterministic, and `resolveOne` fails the test loudly if the assumption ever
    * breaks rather than silently delivering nothing.
    */
  private def withIsolatedClient(
      systemName: String,
      errorHandler: Throwable => Unit = _ => ()
  )(body: (AmberClient, WorkflowFIFOMessagePayload => Unit) => Unit): Unit = {
    val ownSystem = ActorSystem(systemName)
    try {
      val client = newClient(ownSystem, errorHandler)
      try {
        val clientActor: ActorRef = Await.result(
          ownSystem.actorSelection("/user/$a").resolveOne(5.seconds),
          5.seconds
        )
        val probe = TestProbe()(ownSystem)
        val channelId = ChannelIdentity(COORDINATOR, CLIENT, isControl = true)
        var messageId = 0L
        val deliver: WorkflowFIFOMessagePayload => Unit = payload => {
          messageId += 1
          clientActor.tell(
            NetworkMessage(messageId, WorkflowFIFOMessage(channelId, messageId, payload)),
            probe.ref
          )
          // The actor acks before it runs the callbacks, so this only proves the
          // message was consumed; the tests still wait on their own latches.
          probe.expectMsgType[NetworkAck](5.seconds)
        }
        body(client, deliver)
      } finally client.shutdown()
    } finally TestKit.shutdownActorSystem(ownSystem)
  }
}

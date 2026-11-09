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

package org.apache.texera.amber.clustering

import org.apache.pekko.actor.{ActorRef, ActorSystem, Address, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.web.SessionState
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Future => JFuture}
import javax.websocket.{RemoteEndpoint, Session}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.concurrent.duration._

/**
  * Unit tests for the cluster membership listener.
  *
  * This is the last file in the engine that sat at 0%, and it is worth covering because the count
  * it maintains is what the frontend's cluster badge renders: `updateClusterStatus` recomputes
  * `numWorkerNodesInCluster` on every membership event and pushes a `ClusterStatusUpdateEvent` to
  * every open session. A listener that stops subscribing, or stops fanning out, leaves every
  * client showing a stale node count with nothing failing.
  *
  * The suite runs a real single-node cluster: `AmberRuntime.pekkoConfig` selects the cluster
  * provider with artery bound on port 0, so joining the node to itself produces genuine
  * `MemberUp` events. That matters because `Member` is `private[cluster]` and cannot be
  * synthesized — a real join is the only way to exercise the event path at all.
  *
  * Not covered, and deliberately: the `MemberRemoved` recovery arm. It walks
  * `WorkflowService.getAllWorkflowServices` and calls `client.notifyNodeFailure` or
  * `forcefullyStop` on each live execution, so it needs real executions with real Amber clients —
  * integration scope, not this.
  */
class ClusterListenerSpec
    extends TestKit(ActorSystem("ClusterListenerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  private val registeredSessions = ArrayBuffer[String]()
  private var previousNodeCount: Int = 0

  override def beforeAll(): Unit = {
    // `numWorkerNodesInCluster` is a JVM-global var other suites may read; put it back afterwards.
    previousNodeCount = ClusterListener.numWorkerNodesInCluster
    // A single node joining itself becomes the cluster leader and reaches Up on its own, with no
    // seed nodes and no second JVM.
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override def afterAll(): Unit = {
    registeredSessions.foreach(id => scala.util.Try(SessionState.removeState(id)))
    ClusterListener.numWorkerNodesInCluster = previousNodeCount
    TestKit.shutdownActorSystem(system)
  }

  /**
    * Registers a session whose outbound frames are collected, runs `body`, then removes it.
    *
    * The removal has to happen inside the test: ScalaMock scopes expectations per test, and the
    * SessionState registry is JVM-global, so a session left behind is called by a LATER test's
    * listener against an expired mock ("Unexpected call: Session.getAsyncRemote"). Observed.
    */
  private def withSession[A](body: (() => Seq[String]) => A): A = {
    val (id, sent) = mockSession()
    // The body receives a snapshot function rather than the live queue, so every assertion reads a
    // consistent point-in-time view instead of iterating a collection the actor thread is appending to.
    try body(() => sent.asScala.toList)
    finally scala.util.Try(SessionState.removeState(id))
  }

  /**
    * A session whose outbound frames are collected, so the fan-out is observable.
    *
    * The queue is concurrent on purpose: `sendText` is invoked on the listener's actor thread while
    * the assertions read it from the test thread inside `awaitAssert`. A plain ArrayBuffer would be
    * an unsynchronised hand-off across those two threads - the very hazard this suite exists to
    * document on the production side.
    */
  private def mockSession(): (String, ConcurrentLinkedQueue[String]) = {
    val sent = new ConcurrentLinkedQueue[String]()
    val async = mock[RemoteEndpoint.Async]
    (async
      .sendText(_: String))
      .expects(*)
      .onCall { (text: String) =>
        sent.add(text)
        null.asInstanceOf[JFuture[Void]]
      }
      .anyNumberOfTimes()

    val session = mock[Session]
    val id = UUID.randomUUID().toString
    (() => session.getId).expects().returning(id).anyNumberOfTimes()
    (() => session.getAsyncRemote).expects().returning(async).anyNumberOfTimes()
    (() => session.getUserProperties)
      .expects()
      .returning(new java.util.HashMap[String, Object]())
      .anyNumberOfTimes()

    SessionState.setState(id, new SessionState(session))
    registeredSessions += id
    (id, sent)
  }

  private def selfAddress: Address = Cluster(system).selfAddress

  /**
    * Runs `body` with a live listener and stops it afterwards.
    *
    * Stopping matters: a listener left running stays subscribed and keeps iterating
    * `SessionState.getAllSessionStates` on every membership event. That registry is a plain
    * unsynchronised mutable.HashMap, so a leftover listener from an earlier case will throw
    * ConcurrentModificationException the moment a later case registers a session. (Observed - an
    * earlier draft of this suite failed exactly that way.)
    */
  private def withListener[A](body: ActorRef => A): A = {
    val listener = system.actorOf(Props[ClusterListener]())
    try body(listener)
    finally {
      watch(listener)
      system.stop(listener)
      expectTerminated(listener, 10.seconds)
      unwatch(listener)
    }
  }

  behavior of "ClusterListener"

  it should "report the cluster's member addresses on request" in {
    // The node joined itself in beforeAll, so once it reaches Up the listener must report exactly
    // that one address. Asserting the address (not just a count) is what pins getAllAddress to
    // `cluster.state.members.map(_.address)` rather than, say, the seed-node list.
    withListener { listener =>
      awaitAssert(
        {
          listener ! ClusterListener.GetAvailableNodeAddresses()
          expectMsgType[Array[Address]](2.seconds).toSeq shouldBe Seq(selfAddress)
        },
        15.seconds,
        500.millis
      )
    }
  }

  it should "recompute the node count and push it to every open session on a membership event" in {
    // Register the session BEFORE the listener exists, so nothing mutates the registry while a
    // listener is iterating it (see withListener's note), and remove it after the listener stops.
    withSession { sentFrames =>
      // Restore the sentinel in a local finally, not just in afterAll: amber runs suites
      // concurrently in one JVM, so a sibling reading this global should not be able to observe -1
      // for any longer than this case needs it.
      val previous = ClusterListener.numWorkerNodesInCluster
      ClusterListener.numWorkerNodesInCluster = -1
      try {
        // Creating the listener subscribes it with InitialStateAsEvents, so the already-Up member is
        // replayed to it as a MemberUp. That is what drives updateClusterStatus here - no synthetic
        // Member is needed, and none could be built (Member is private[cluster]).
        withListener { _ =>
          awaitAssert(
            {
              // The count is recomputed from live membership, not left at the sentinel.
              ClusterListener.numWorkerNodesInCluster shouldBe 1

              val counts = sentFrames()
                .map(objectMapper.readTree)
                .filter(_.get("type").asText() == "ClusterStatusUpdateEvent")
                .map(_.get("numWorkers").asInt())
              // Every open session is told, and told the recomputed number.
              counts should not be empty
              counts.last shouldBe 1
            },
            15.seconds,
            500.millis
          )
        }
      } finally ClusterListener.numWorkerNodesInCluster = previous
    }
  }

  it should "swallow an unrecognised message rather than failing on it" in {
    // `TestActorRef.receive` invokes the actor's receive directly and lets the exception escape to
    // the caller, so "did not throw" genuinely means the catch-all arm handled the message.
    //
    // Neither `!` nor a plain ActorRef would discriminate here: an exception thrown from receive
    // is taken by the supervisor, the actor is restarted, and a restarted listener answers the
    // next request exactly like one that never failed. (Checked - a version of this test written
    // with `!` stayed green with the catch-all replaced by a `throw`.)
    val listener = TestActorRef[ClusterListener](Props[ClusterListener]())
    try {
      noException should be thrownBy listener.receive("not a cluster event")
    } finally {
      // Stopped for the same reason the others are: it subscribed in preStart, and a listener left
      // running keeps iterating the shared SessionState registry on every membership event.
      watch(listener)
      system.stop(listener)
      expectTerminated(listener, 10.seconds)
      unwatch(listener)
    }
  }

  // Not asserted separately: that postStop unsubscribes. Every case above stops its listener via
  // `withListener` and the suite stays green across repeated runs, which is the observable
  // consequence - a listener that stayed subscribed after stop would trip the registry race
  // described there. A direct assertion would need to reach into Cluster's subscriber set.
}

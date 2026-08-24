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

import org.apache.pekko.actor.{Actor, ActorContext, ActorRef, ActorSystem, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit, TestProbe}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  GetActorRef,
  NetworkMessage,
  RegisterActorRef
}
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

/**
  * Unit tests for [[PekkoActorRefMappingService]].
  *
  * These tests use a minimal actor spawned by Pekko TestKit to supply the
  * live [[ActorContext]] that [[PekkoActorService]] eagerly dereferences.
  * Spawning it as a child of a [[TestProbe]] also makes the service's parent
  * lookup observable.
  */
class PekkoActorRefMappingServiceSpec
    extends TestKit(ActorSystem("PekkoActorRefMappingServiceSpec"))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val upstreamId = ActorVirtualIdentity("upstream")
  private val workerId = ActorVirtualIdentity("mapping-service-worker")
  private val contextCounter = new java.util.concurrent.atomic.AtomicInteger(0)

  private def channelTo(destination: ActorVirtualIdentity): ChannelIdentity =
    ChannelIdentity(
      fromWorkerId = upstreamId,
      toWorkerId = destination,
      isControl = true
    )

  private def networkMessageTo(
      destination: ActorVirtualIdentity,
      messageId: Long,
      sequenceNumber: Long
  ): NetworkMessage =
    NetworkMessage(
      messageId,
      WorkflowFIFOMessage(channelTo(destination), sequenceNumber, DataFrame(Array.empty))
    )

  private def newContext(parent: TestProbe): ActorContext =
    TestActorRef[ActorRefMappingServiceContextHolder](
      Props(new ActorRefMappingServiceContextHolder),
      parent.ref,
      s"actor-ref-mapping-context-${contextCounter.incrementAndGet()}"
    ).underlyingActor.context

  private def newActorService(
      id: ActorVirtualIdentity,
      parent: TestProbe
  ): PekkoActorService = new PekkoActorService(id, newContext(parent))

  "askForCredit" should "forward a request only when the destination ref is known" in {
    val parent = TestProbe()
    val destination = TestProbe()
    val unknownDestination = ActorVirtualIdentity("unknown-credit-destination")
    val knownDestination = ActorVirtualIdentity("known-credit-destination")
    val service = new PekkoActorRefMappingService(newActorService(workerId, parent))

    service.askForCredit(channelTo(unknownDestination))
    parent.expectNoMessage(100.millis)

    service.registerActorRef(knownDestination, destination.ref)
    val channel = channelTo(knownDestination)
    service.askForCredit(channel)

    destination.expectMsg(CreditRequest(channel))
  }

  "forwardToActor" should "stash unknown messages and ask the parent for their ref once" in {
    val parent = TestProbe()
    val destination = ActorVirtualIdentity("unknown-message-destination")
    val service = new PekkoActorRefMappingService(newActorService(workerId, parent))
    val first = networkMessageTo(destination, messageId = 1L, sequenceNumber = 0L)
    val second = networkMessageTo(destination, messageId = 2L, sequenceNumber = 1L)

    service.forwardToActor(first)
    val lookup = parent.expectMsgType[GetActorRef]
    assert(lookup.id == destination)
    assert(lookup.replyTo == Set(service.self))

    service.forwardToActor(second)
    parent.expectNoMessage(100.millis)

    service.clearQueriedActorRefs()
    service.forwardToActor(networkMessageTo(destination, messageId = 3L, sequenceNumber = 2L))
    assert(parent.expectMsgType[GetActorRef].id == destination)
  }

  "registerActorRef" should "drain stashed messages in FIFO order and notify controller waiters" in {
    val parent = TestProbe()
    val destination = ActorVirtualIdentity("controller-wait-destination")
    val waiterOne = TestProbe()
    val waiterTwo = TestProbe()
    val registered = TestProbe()
    val service = new PekkoActorRefMappingService(newActorService(CONTROLLER, parent))
    val first = networkMessageTo(destination, messageId = 10L, sequenceNumber = 0L)
    val second = networkMessageTo(destination, messageId = 11L, sequenceNumber = 1L)

    service.forwardToActor(first)
    service.forwardToActor(second)
    service.retrieveActorRef(destination, Set(waiterOne.ref, waiterTwo.ref))

    service.registerActorRef(destination, registered.ref)

    registered.expectMsg(first)
    registered.expectMsg(second)
    waiterOne.expectMsg(RegisterActorRef(destination, registered.ref))
    waiterTwo.expectMsg(RegisterActorRef(destination, registered.ref))
  }

  "retrieveActorRef and removeActorRef" should "reply for known refs and remove their reverse lookup" in {
    val parent = TestProbe()
    val destination = ActorVirtualIdentity("registered-destination")
    val registered = TestProbe()
    val waiter = TestProbe()
    val service = new PekkoActorRefMappingService(newActorService(workerId, parent))

    service.registerActorRef(destination, registered.ref)
    service.retrieveActorRef(destination, Set(waiter.ref))

    waiter.expectMsg(RegisterActorRef(destination, registered.ref))
    assert(service.hasActorRef(destination))
    assert(service.getActorRef(destination) == registered.ref)
    assert(service.findActorVirtualIdentity(registered.ref).contains(destination))

    service.removeActorRef(destination)

    assert(!service.hasActorRef(destination))
    assert(service.findActorVirtualIdentity(registered.ref).isEmpty)
  }

  "retrieveActorRef" should "swallow a failed parent lookup and still ask again for the same id" in {
    val parent = TestProbe()
    val destination = ActorVirtualIdentity("unreachable-parent-destination")
    val waiter = TestProbe()
    // Pekko itself does not fail here -- `context.parent` reads a field and `!` never throws -- so
    // the failure is injected, for the first read only.
    val actorService = new FailingParentActorService(workerId, newContext(parent))
    val service = new PekkoActorRefMappingService(actorService)
    actorService.failuresLeft = 1

    service.retrieveActorRef(destination, Set(waiter.ref))

    // Nothing was asked and nobody was told: the lookup simply did not happen.
    parent.expectNoMessage(100.millis)
    waiter.expectNoMessage(100.millis)

    // ...and the id was not recorded as queried, so the next message bound for it re-asks. Marking
    // it would strand every message for that destination: the reply that clears the stash only ever
    // arrives in response to a `GetActorRef` that was actually sent.
    service.retrieveActorRef(destination, Set(waiter.ref))

    assert(parent.expectMsgType[GetActorRef].id == destination)
  }

  it should "contain a parent lookup that keeps failing, not just its first failure" in {
    val parent = TestProbe()
    val destination = ActorVirtualIdentity("persistently-unreachable-parent-destination")
    val waiter = TestProbe()
    val actorService = new FailingParentActorService(workerId, newContext(parent))
    val service = new PekkoActorRefMappingService(actorService)
    // Every read of `parent` throws, which is the realistic shape of an unreachable parent: the
    // condition is a property of the actor, not of one attempt. A handler that reads the failing
    // value a second time (e.g. to name the parent ref in its own log line) therefore throws out
    // of the very try/catch that exists to contain the failure.
    actorService.failuresLeft = Int.MaxValue

    service.retrieveActorRef(destination, Set(waiter.ref))

    parent.expectNoMessage(100.millis)
    waiter.expectNoMessage(100.millis)

    // Once the parent recovers, the id must still be askable -- the contained failure may not have
    // marked it queried.
    actorService.failuresLeft = 0
    service.retrieveActorRef(destination, Set(waiter.ref))

    assert(parent.expectMsgType[GetActorRef].id == destination)
  }
}

/** Minimal actor used only to obtain a live [[ActorContext]] from Pekko TestKit. */
class ActorRefMappingServiceContextHolder extends Actor {
  override def receive: Receive = { case _ => () }
}

/** A [[PekkoActorService]] whose first `failuresLeft` parent lookups throw. */
class FailingParentActorService(id: ActorVirtualIdentity, actorContext: ActorContext)
    extends PekkoActorService(id, actorContext) {

  var failuresLeft: Int = 0

  override def parent: ActorRef = {
    if (failuresLeft > 0) {
      failuresLeft -= 1
      throw new IllegalStateException("parent is unreachable")
    }
    super.parent
  }
}

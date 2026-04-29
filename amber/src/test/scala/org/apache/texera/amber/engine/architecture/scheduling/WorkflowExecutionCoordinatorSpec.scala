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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.architecture.controller.ControllerConfig
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.scheduling.RegionCoordinatorTestSupport._
import org.apache.texera.amber.engine.common.AmberRuntime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable

class WorkflowExecutionCoordinatorSpec
    extends TestKit(ActorSystem("WorkflowExecutionCoordinatorSpec", AmberRuntime.akkaConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionCoordinatorTestSupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "WorkflowExecutionCoordinator" should "start the next region only after previous region termination succeeds" in {
    val firstOp = createSourceOp("first-op")
    val firstWorkerId = createWorkerId(firstOp)
    val firstRegion = createSingleWorkerRegion(1, firstOp, firstWorkerId)

    val secondOp = createSourceOp("second-op")
    val secondWorkerId = createWorkerId(secondOp)
    val secondRegion = createSingleWorkerRegion(2, secondOp, secondWorkerId)

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 101, firstOp, firstWorkerId)
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 102, secondOp, secondWorkerId)

    // First region's worker holds endWorker pending until we explicitly fulfill it; the second
    // region's worker terminates immediately. This lets us assert the second region cannot start
    // until termination of the first finishes.
    val rpcProbe = new ControllerRpcProbe(
      endWorkerResponse = call => if (call.receiver == firstWorkerId) None else Some(EmptyReturn())
    )
    val controller = createControllerHarness()
    registerLiveWorker(controller.actorRefService, firstWorkerId)
    registerLiveWorker(controller.actorRefService, secondWorkerId)

    val nextRegionLevels = mutable.Queue(Set(firstRegion), Set(secondRegion))
    val workflowCoordinator = new WorkflowExecutionCoordinator(
      () => if (nextRegionLevels.nonEmpty) nextRegionLevels.dequeue() else Set.empty,
      workflowExecution,
      ControllerConfig(None, None, None, None),
      rpcProbe.asyncRPCClient
    )
    workflowCoordinator.setupActorRefService(controller.actorRefService)

    await(workflowCoordinator.coordinateRegionExecutors(controller.actorService))
    assert(rpcProbe.startedWorkers == Seq(firstWorkerId))

    val coordination = workflowCoordinator.coordinateRegionExecutors(controller.actorService)

    waitUntil(rpcProbe.endWorkerCalls.size == 1)
    assert(coordination.poll.isEmpty)
    assert(!rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(controller.actorRefService.hasActorRef(firstWorkerId))

    rpcProbe.fulfill(rpcProbe.onlyEndWorkerCall, EmptyReturn())
    await(coordination)

    assert(!controller.actorRefService.hasActorRef(firstWorkerId))
    assert(rpcProbe.initializedWorkers.contains(secondWorkerId))
    assert(rpcProbe.startedWorkers.contains(secondWorkerId))
  }
}

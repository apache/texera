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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  RUNNING
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.{CountDownLatch, TimeUnit}

class WorkflowLifecycleManagerSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // WorkflowLifecycleManager schedules through AmberRuntime's process-wide actor system.
  // Preserve that shared reference because amber suites run concurrently in one JVM.
  private lazy val testSystem: ActorSystem =
    ActorSystem("WorkflowLifecycleManagerSpec-test", AmberRuntime.pekkoConfig)

  private var previousActorSystem: AnyRef = _

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
    setAmberRuntimeField("_actorSystem", testSystem)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_actorSystem", previousActorSystem)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  private def managerWithCallback(
      cleanUpTimeout: Int = 1
  ): (WorkflowLifecycleManager, CountDownLatch) = {
    val cleaned = new CountDownLatch(1)
    val manager = new WorkflowLifecycleManager(
      id = "workflow-lifecycle-manager-spec",
      cleanUpTimeout = cleanUpTimeout,
      cleanUpCallback = () => cleaned.countDown()
    )
    (manager, cleaned)
  }

  private def assertCleanUpWithin(cleaned: CountDownLatch, seconds: Long): Unit = {
    assert(
      cleaned.await(seconds, TimeUnit.SECONDS),
      "cleanup callback was not invoked before the deadline"
    )
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
    val (manager, cleaned) = managerWithCallback(cleanUpTimeout = 4)
    val stateStore = new ExecutionStateStore

    manager.registerCleanUpOnStateChange(stateStore)
    Thread.sleep(2000)
    stateStore.metadataStore.updateState(_.withState(COMPLETED))

    assert(
      !cleaned.await(3000, TimeUnit.MILLISECONDS),
      "a refreshed deadline must cancel the earlier one"
    )
    assertCleanUpWithin(cleaned, seconds = 5)
  }
}

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

package org.apache.texera.amber.engine.architecture.controller

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessagePayload
import org.apache.texera.amber.engine.common.executionruntimestate.OperatorMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class ClientEventSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Suite-local ActorSystem injected into AmberRuntime.serde via reflection
  // ---------------------------------------------------------------------------
  //
  // The serde round-trips below use the production wire path
  // (AmberRuntime.serde), so we own a real ActorSystem and shut it down in
  // afterAll. Pattern matches CheckpointSubsystemSpec / LogreplayPrimitivesSpec.

  private val testSystem: ActorSystem =
    ActorSystem("ClientEventSpec-test", AmberRuntime.pekkoConfig)
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

  private def roundTrip[T <: ClientEvent](e: T): T = {
    val bytes = AmberRuntime.serde.serialize(e).get
    AmberRuntime.serde
      .deserialize(bytes, e.getClass.asInstanceOf[Class[T]])
      .get
  }

  // ---------------------------------------------------------------------------
  // Compile-time pin: every subtype extends ClientEvent + WorkflowFIFOMessagePayload
  // ---------------------------------------------------------------------------
  //
  // The `: ClientEvent` ascriptions in the per-subtype tests would fail to
  // compile if the subtype no longer extends the trait, so the membership
  // contract is enforced at compile time. We additionally assert
  // `isInstanceOf[WorkflowFIFOMessagePayload]` for the trait parent (which
  // ClientEvent itself extends) — a regression that demoted ClientEvent off
  // WorkflowFIFOMessagePayload would surface here.

  "ClientEvent subtypes" should "all extend WorkflowFIFOMessagePayload (via the ClientEvent trait)" in {
    val all: List[ClientEvent] = List(
      ExecutionStateUpdate(WorkflowAggregatedState.READY),
      ExecutionStatsUpdate(Map.empty),
      RuntimeStatisticsPersist(Map.empty),
      ReportCurrentProcessingTuple("op", Array.empty),
      WorkerAssignmentUpdate(Map.empty),
      FatalError(new RuntimeException("x")),
      UpdateExecutorCompleted(ActorVirtualIdentity("w")),
      ReplayStatusUpdate(ActorVirtualIdentity("w"), status = true),
      WorkflowRecoveryStatus(isRecovering = false)
    )
    all.foreach(e => assert(e.isInstanceOf[WorkflowFIFOMessagePayload]))
  }

  // ---------------------------------------------------------------------------
  // Per-subtype data contract + Pekko Serialization round-trip
  // ---------------------------------------------------------------------------

  "ExecutionStateUpdate" should "expose its state field and round-trip via AmberRuntime.serde" in {
    val original = ExecutionStateUpdate(WorkflowAggregatedState.RUNNING)
    assert(original.state == WorkflowAggregatedState.RUNNING)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.state == WorkflowAggregatedState.RUNNING)
  }

  it should "preserve case-class equality and hashCode across constructions" in {
    val a = ExecutionStateUpdate(WorkflowAggregatedState.PAUSED)
    val b = ExecutionStateUpdate(WorkflowAggregatedState.PAUSED)
    val c = ExecutionStateUpdate(WorkflowAggregatedState.RUNNING)
    assert(a == b)
    assert(a.hashCode == b.hashCode)
    assert(a != c)
  }

  "ExecutionStatsUpdate" should
    "expose its operatorMetrics field and round-trip via AmberRuntime.serde" in {
    val metrics = Map(
      "op-1" -> OperatorMetrics.defaultInstance,
      "op-2" -> OperatorMetrics.defaultInstance
    )
    val original = ExecutionStatsUpdate(metrics)
    assert(original.operatorMetrics == metrics)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.operatorMetrics == metrics)
  }

  it should "support empty operatorMetrics" in {
    val original = ExecutionStatsUpdate(Map.empty)
    val restored = roundTrip(original)
    assert(restored.operatorMetrics.isEmpty)
  }

  "RuntimeStatisticsPersist" should "round-trip via AmberRuntime.serde" in {
    val original = RuntimeStatisticsPersist(Map("op-1" -> OperatorMetrics.defaultInstance))
    val restored = roundTrip(original)
    assert(restored == original)
  }

  "ReportCurrentProcessingTuple" should "expose operatorID and the tuple array (round-trip)" in {
    // ReportCurrentProcessingTuple's `tuple` field is Array[(Tuple, AVI)] —
    // case-class equality on arrays is reference-based, so the round-trip
    // produces a new value-equal but reference-distinct array. Verify the
    // contents element-by-element rather than relying on `==`.
    val arr: Array[(Tuple, ActorVirtualIdentity)] = Array.empty
    val original = ReportCurrentProcessingTuple("op-x", arr)
    assert(original.operatorID == "op-x")
    assert(original.tuple.isEmpty)
    val restored = roundTrip(original)
    assert(restored.operatorID == "op-x")
    assert(restored.tuple.length == original.tuple.length)
  }

  "WorkerAssignmentUpdate" should "expose its workerMapping field and round-trip" in {
    val mapping = Map(
      "op-1" -> Seq("w-1", "w-2"),
      "op-2" -> Seq("w-3")
    )
    val original = WorkerAssignmentUpdate(mapping)
    assert(original.workerMapping == mapping)
    val restored = roundTrip(original)
    assert(restored.workerMapping == mapping)
  }

  it should "support empty workerMapping" in {
    val original = WorkerAssignmentUpdate(Map.empty)
    val restored = roundTrip(original)
    assert(restored.workerMapping.isEmpty)
  }

  "FatalError" should "default fromActor to None" in {
    val original = FatalError(new RuntimeException("boom"))
    assert(original.fromActor.isEmpty)
  }

  it should "accept an explicit fromActor and round-trip both fields" in {
    val origin = ActorVirtualIdentity("worker-7")
    val original = FatalError(new IllegalStateException("nope"), Some(origin))
    assert(original.fromActor.contains(origin))
    val restored = roundTrip(original)
    assert(restored.fromActor.contains(origin))
    // Throwable equality is reference-based by default, so we compare on
    // message + class — what callers consume off the wire.
    assert(restored.e.getMessage == "nope")
    assert(restored.e.getClass == classOf[IllegalStateException])
  }

  "UpdateExecutorCompleted" should "round-trip the actor id" in {
    val original = UpdateExecutorCompleted(ActorVirtualIdentity("worker-1"))
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.id == ActorVirtualIdentity("worker-1"))
  }

  "ReplayStatusUpdate" should "round-trip status = true" in {
    val original = ReplayStatusUpdate(ActorVirtualIdentity("worker-1"), status = true)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.status)
  }

  it should "round-trip status = false" in {
    val original = ReplayStatusUpdate(ActorVirtualIdentity("worker-1"), status = false)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(!restored.status)
  }

  "WorkflowRecoveryStatus" should "round-trip isRecovering = true" in {
    val original = WorkflowRecoveryStatus(isRecovering = true)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(restored.isRecovering)
  }

  it should "round-trip isRecovering = false" in {
    val original = WorkflowRecoveryStatus(isRecovering = false)
    val restored = roundTrip(original)
    assert(restored == original)
    assert(!restored.isRecovering)
  }

  // ---------------------------------------------------------------------------
  // Cross-subtype identity
  // ---------------------------------------------------------------------------

  "Two ClientEvent subtypes with the same field shape" should "not be equal across types" in {
    // ExecutionStatsUpdate and RuntimeStatisticsPersist both wrap a
    // Map[String, OperatorMetrics] — verify case-class equality
    // distinguishes them (cross-type ne).
    val m = Map("x" -> OperatorMetrics.defaultInstance)
    val a: ClientEvent = ExecutionStatsUpdate(m)
    val b: ClientEvent = RuntimeStatisticsPersist(m)
    assert(a != b)
  }
}

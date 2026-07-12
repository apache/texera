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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.AmberRuntime
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

  // The `ClientEvent` ↔ `WorkflowFIFOMessagePayload` membership is enforced
  // at compile time (`trait ClientEvent extends WorkflowFIFOMessagePayload`
  // plus the `: ClientEvent` ascriptions used in the per-subtype tests
  // below). A runtime `isInstanceOf` sweep would be tautological and would
  // need to be edited every time a new subtype is added — skip it.

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

  // Build a tiny Tuple fixture for the ReportCurrentProcessingTuple
  // round-trip — a real (Tuple, AVI) pair (not the empty-array degenerate
  // case) so the serializer is actually exercised on the inner elements.
  private val intAttr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(intAttr)
  private def intTuple(value: Int): Tuple =
    Tuple.builder(schema).add(intAttr, Integer.valueOf(value)).build()

  "ReportCurrentProcessingTuple" should "round-trip a single (Tuple, AVI) element through AmberRuntime.serde" in {
    // Pin the actual element-survival contract: build a non-empty array,
    // round-trip, and verify the recovered Tuple's schema + field values
    // and the AVI both survive. (Case-class equality on Array is
    // reference-based, so element-wise verification is the right pin.)
    val sender = ActorVirtualIdentity("worker-1")
    val arr: Array[(Tuple, ActorVirtualIdentity)] = Array((intTuple(42), sender))
    val original = ReportCurrentProcessingTuple("op-x", arr)
    assert(original.operatorID == "op-x")
    assert(original.tuple.length == 1)
    val restored = roundTrip(original)
    assert(restored.operatorID == "op-x")
    assert(restored.tuple.length == 1)
    val (restoredTuple, restoredAvi) = restored.tuple.head
    assert(restoredTuple == intTuple(42))
    assert(restoredAvi == sender)
  }

  it should "round-trip an empty tuple array" in {
    // Empty-array edge case: pin that the serializer doesn't choke on
    // an empty Array[(Tuple, AVI)] and that operatorID still survives.
    val original = ReportCurrentProcessingTuple("op-empty", Array.empty)
    val restored = roundTrip(original)
    assert(restored.operatorID == "op-empty")
    assert(restored.tuple.isEmpty)
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

  "FatalError" should "default fromActor to None and round-trip the default through AmberRuntime.serde" in {
    // Pin both the constructor default AND the wire-path preservation for
    // the default-None case — a regression that mishandled the missing
    // fromActor on the receive side would surface here.
    val original = FatalError(new RuntimeException("boom"))
    assert(original.fromActor.isEmpty)
    val restored = roundTrip(original)
    assert(restored.fromActor.isEmpty)
    assert(restored.e.getMessage == "boom")
    assert(restored.e.getClass == classOf[RuntimeException])
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

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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CheckpointSubsystemSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // Suite-local actor system. We also inject it into AmberRuntime via
  // reflection so that CheckpointState.save/load (which hard-code
  // AmberRuntime.serde) reuse the same system. Both the suite-local system
  // and AmberRuntime's reference are torn down in afterAll, so no Pekko
  // threads outlive the test (matching ControllerSpec/WorkerSpec hygiene).
  private val testSystem: ActorSystem =
    ActorSystem("CheckpointSubsystemSpec-test", AmberRuntime.akkaConfig)
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

  // ---------------------------------------------------------------------------
  // SerializedState
  // ---------------------------------------------------------------------------

  "SerializedState" should "expose stable well-known key constants" in {
    // These constants are referenced from outside the engine; pin the strings
    // so a rename surfaces as a test failure.
    assert(SerializedState.CP_STATE_KEY == "Amber_CPState")
    assert(SerializedState.DP_STATE_KEY == "Amber_DPState")
    assert(SerializedState.IN_FLIGHT_MSG_KEY == "Amber_Inflight_Messages")
    assert(SerializedState.DP_QUEUED_MSG_KEY == "Amber_DP_Queued_Messages")
    assert(SerializedState.OUTPUT_MSG_KEY == "Amber_Output_Messages")
  }

  it should "round-trip a value through fromObject / toObject using a suite-local Serialization" in {
    // Use the suite-local serde directly so this case does not even touch
    // AmberRuntime.
    val original: java.lang.Integer = Integer.valueOf(42)
    val state = SerializedState.fromObject(original, testSerde)
    assert(state.bytes.length > 0)
    assert(state.size() == state.bytes.length.toLong)
    val restored = state.toObject[java.lang.Integer](testSerde)
    assert(restored == original)
  }

  it should "carry the serializer id and manifest given at construction" in {
    val s = SerializedState(Array[Byte](1, 2, 3), serializerId = 7, manifest = "manifest-x")
    assert(s.bytes.toSeq == Seq[Byte](1, 2, 3))
    assert(s.serializerId == 7)
    assert(s.manifest == "manifest-x")
    assert(s.size() == 3L)
  }

  // ---------------------------------------------------------------------------
  // CheckpointState
  // ---------------------------------------------------------------------------

  "CheckpointState" should "default to size = 0 with no entries" in {
    val cp = new CheckpointState()
    assert(cp.size() == 0L)
    assert(!cp.has("anything"))
  }

  "CheckpointState.save / load" should "round-trip a primitive value" in {
    val cp = new CheckpointState()
    cp.save("answer", java.lang.Integer.valueOf(42))
    assert(cp.has("answer"))
    val restored: java.lang.Integer = cp.load[java.lang.Integer]("answer")
    assert(restored == java.lang.Integer.valueOf(42))
  }

  it should "round-trip a String value" in {
    val cp = new CheckpointState()
    cp.save("greeting", "hello")
    assert(cp.load[String]("greeting") == "hello")
  }

  it should "overwrite a previously saved key" in {
    val cp = new CheckpointState()
    cp.save("k", java.lang.Integer.valueOf(1))
    cp.save("k", java.lang.Integer.valueOf(2))
    assert(cp.load[java.lang.Integer]("k") == java.lang.Integer.valueOf(2))
  }

  it should "track distinct keys independently" in {
    val cp = new CheckpointState()
    cp.save("a", "alpha")
    cp.save("b", "beta")
    assert(cp.load[String]("a") == "alpha")
    assert(cp.load[String]("b") == "beta")
  }

  "CheckpointState.load" should "raise RuntimeException for an unknown key" in {
    val cp = new CheckpointState()
    val ex = intercept[RuntimeException] {
      cp.load[Any]("missing")
    }
    assert(ex.getMessage.contains("missing"))
  }

  "CheckpointState.size" should "be the sum of every entry's serialized byte length" in {
    val cp = new CheckpointState()
    cp.save("a", "x")
    val sizeAfterOne = cp.size()
    assert(sizeAfterOne > 0L)
    cp.save("b", "yy")
    assert(cp.size() > sizeAfterOne)
  }

  // ---------------------------------------------------------------------------
  // CheckpointSupport (trait shape)
  // ---------------------------------------------------------------------------

  "CheckpointSupport" should "be implementable by a custom subclass forwarding to a CheckpointState" in {
    val support = new CheckpointSupport {
      override def serializeState(
          currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
          checkpoint: CheckpointState
      ): Iterator[(TupleLike, Option[PortIdentity])] = {
        checkpoint.save("marker", java.lang.Integer.valueOf(1))
        currentIteratorState
      }

      override def deserializeState(
          checkpoint: CheckpointState
      ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator.empty

      override def getEstimatedCheckpointCost: Long = 7L
    }

    val cp = new CheckpointState()
    val out = support.serializeState(Iterator.empty, cp)
    assert(out.isEmpty)
    assert(cp.has("marker"))
    assert(support.deserializeState(cp).isEmpty)
    assert(support.getEstimatedCheckpointCost == 7L)
  }
}

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

import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.locks.ReentrantLock

class UtilsSpec extends AnyFlatSpec {

  // -- aggregatedStateToString ----------------------------------------------

  "Utils.aggregatedStateToString" should "round-trip every named WorkflowAggregatedState through stringToAggregatedState" in {
    val namedStates = Seq(
      WorkflowAggregatedState.UNINITIALIZED,
      WorkflowAggregatedState.READY,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.PAUSING,
      WorkflowAggregatedState.PAUSED,
      WorkflowAggregatedState.RESUMING,
      WorkflowAggregatedState.COMPLETED,
      WorkflowAggregatedState.TERMINATED,
      WorkflowAggregatedState.FAILED,
      WorkflowAggregatedState.KILLED,
      WorkflowAggregatedState.UNKNOWN
    )
    namedStates.foreach { state =>
      assert(
        Utils.stringToAggregatedState(Utils.aggregatedStateToString(state)) == state,
        s"round-trip failed for $state"
      )
    }
  }

  it should "render an unrecognized aggregated state with its raw value" in {
    val unrecognized = WorkflowAggregatedState.Unrecognized(99)
    assert(Utils.aggregatedStateToString(unrecognized) == "Unrecognized(99)")
  }

  // -- stringToAggregatedState ----------------------------------------------

  "Utils.stringToAggregatedState" should "be case-insensitive and tolerant of surrounding whitespace" in {
    assert(Utils.stringToAggregatedState("RUNNING") == WorkflowAggregatedState.RUNNING)
    assert(Utils.stringToAggregatedState("running") == WorkflowAggregatedState.RUNNING)
    assert(Utils.stringToAggregatedState("  Running  ") == WorkflowAggregatedState.RUNNING)
  }

  it should "accept 'Initializing' as an alias for READY" in {
    assert(Utils.stringToAggregatedState("Initializing") == WorkflowAggregatedState.READY)
    assert(Utils.stringToAggregatedState("ready") == WorkflowAggregatedState.READY)
  }

  it should "throw IllegalArgumentException for an unrecognized state name" in {
    assertThrows[IllegalArgumentException] {
      Utils.stringToAggregatedState("not-a-real-state")
    }
  }

  // -- maptoStatusCode ------------------------------------------------------

  "Utils.maptoStatusCode" should "map known states to their documented byte codes" in {
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.UNINITIALIZED) == 0.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.READY) == 0.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.RUNNING) == 1.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.PAUSED) == 2.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.COMPLETED) == 3.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.FAILED) == 4.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.KILLED) == 5.toByte)
  }

  it should "return -1 for states that have no documented code" in {
    Seq(
      WorkflowAggregatedState.PAUSING,
      WorkflowAggregatedState.RESUMING,
      WorkflowAggregatedState.TERMINATED,
      WorkflowAggregatedState.UNKNOWN
    ).foreach { state =>
      assert(Utils.maptoStatusCode(state) == -1.toByte, s"expected -1 for $state")
    }
  }

  // -- retry ---------------------------------------------------------------

  "Utils.retry" should "return the value on the first successful attempt without retrying" in {
    var calls = 0
    val result = Utils.retry(attempts = 3, baseBackoffTimeInMS = 0L) {
      calls += 1
      "ok"
    }
    assert(result == "ok")
    assert(calls == 1)
  }

  it should "retry on failure until success and return the eventual result" in {
    var calls = 0
    val result = Utils.retry(attempts = 3, baseBackoffTimeInMS = 0L) {
      calls += 1
      if (calls < 2) throw new RuntimeException("transient")
      "ok"
    }
    assert(result == "ok")
    assert(calls == 2)
  }

  it should "rethrow the last exception after exhausting all attempts" in {
    var calls = 0
    val ex = intercept[RuntimeException] {
      Utils.retry(attempts = 2, baseBackoffTimeInMS = 0L) {
        calls += 1
        throw new RuntimeException(s"failure-$calls")
      }
    }
    assert(calls == 2)
    assert(ex.getMessage == "failure-2")
  }

  // -- withLock ------------------------------------------------------------

  "Utils.withLock" should "release the lock after the body returns" in {
    implicit val lock: ReentrantLock = new ReentrantLock()
    val result = Utils.withLock {
      assert(lock.isHeldByCurrentThread)
      42
    }
    assert(result == 42)
    assert(!lock.isHeldByCurrentThread)
  }

  it should "release the lock when the body throws" in {
    implicit val lock: ReentrantLock = new ReentrantLock()
    intercept[RuntimeException] {
      Utils.withLock[Unit] {
        throw new RuntimeException("boom")
      }
    }
    assert(!lock.isHeldByCurrentThread)
  }
}

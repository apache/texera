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

package org.apache.texera.web.resource

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics
}
import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionStatsStore,
  OperatorMetrics,
  OperatorStatistics
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

class SyncExecutionResourceSpec extends AnyFlatSpec with Matchers {

  // A virtual clock so the bounded poll's timeout/early-exit can be asserted without
  // real waiting. Each sleep advances the clock by the requested interval.
  private class FakeClock {
    var nowMillis: Long = 0L
    var sleepCount: Int = 0
    def now(): Long = nowMillis
    def sleep(ms: Long): Unit = {
      nowMillis += ms
      sleepCount += 1
    }
  }

  private val resource = new SyncExecutionResource

  "awaitUntil" should "return immediately when there are no target operators" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List.empty,
      expectedCountOf = _ => 10L,
      committedCountOf = _ => Some(0L),
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 0
  }

  it should "treat a non-positive expected count as already ready" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 0L,
      committedCountOf = _ => Some(0L),
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 0
  }

  it should "treat an operator with no result storage as ready" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 10L,
      committedCountOf = _ => None,
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 0
  }

  it should "return on the first check when results are already committed" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 10L,
      committedCountOf = _ => Some(10L),
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 0
  }

  it should "poll until the committed count reaches the expected count" in {
    val clock = new FakeClock
    var checks = 0
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 10L,
      committedCountOf = _ => {
        checks += 1
        Some(if (checks >= 3) 10L else 0L)
      },
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    // Not ready on checks 1 and 2 (two sleeps), ready on check 3.
    clock.sleepCount shouldBe 2
    clock.nowMillis shouldBe 50L
  }

  it should "treat a committed count above the expected count as ready" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 10L,
      committedCountOf = _ => Some(15L), // storage already holds more rows than stats report
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 0
  }

  it should "not block on a storage-less operator while waiting on another" in {
    val clock = new FakeClock
    var bChecks = 0
    resource.awaitUntil(
      targetOperatorIds = List("a", "b"),
      expectedCountOf = _ => 5L,
      committedCountOf = {
        case "a" => None // a has no result storage, must not hold up the poll
        case _ =>
          bChecks += 1
          Some(if (bChecks >= 2) 5L else 0L) // b lands on the second check
      },
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 1
  }

  it should "require every target operator to be ready" in {
    val clock = new FakeClock
    var bChecks = 0
    resource.awaitUntil(
      targetOperatorIds = List("a", "b"),
      expectedCountOf = _ => 5L,
      committedCountOf = {
        case "a" => Some(5L) // a is ready from the start
        case _ =>
          bChecks += 1
          Some(if (bChecks >= 2) 5L else 0L) // b lands on the second check
      },
      timeoutMillis = 1000L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    clock.sleepCount shouldBe 1
  }

  it should "give up at the timeout cap when results never land" in {
    val clock = new FakeClock
    resource.awaitUntil(
      targetOperatorIds = List("op"),
      expectedCountOf = _ => 10L,
      committedCountOf = _ => Some(0L),
      timeoutMillis = 100L,
      pollIntervalMillis = 25L,
      now = clock.now,
      sleep = clock.sleep
    )
    // Polls at t=0,25,50,75 then the t=100 check fails the deadline guard.
    clock.sleepCount shouldBe 4
    clock.nowMillis shouldBe 100L
  }

  // Stats store with one operator whose output ports are the given (port, count) pairs.
  private def statsWith(opId: String, ports: (PortIdentity, Long)*): ExecutionStatsStore = {
    val outputMetrics = ports.map {
      case (portId, count) =>
        PortTupleMetricsMapping(portId, TupleMetrics(count = count, size = 0L))
    }
    ExecutionStatsStore(operatorInfo =
      Map(
        opId -> OperatorMetrics(
          operatorState = WorkflowAggregatedState.COMPLETED,
          operatorStatistics = OperatorStatistics(outputMetrics = outputMetrics)
        )
      )
    )
  }

  "expectedDefaultPortOutputCount" should "return the count of the default external output port" in {
    val stats = statsWith("op", PortIdentity() -> 42L)
    resource.expectedDefaultPortOutputCount(stats, "op") shouldBe 42L
  }

  it should "return 0 when the operator has no stats entry" in {
    val stats = statsWith("op", PortIdentity() -> 42L)
    resource.expectedDefaultPortOutputCount(stats, "missing") shouldBe 0L
  }

  it should "return 0 when the operator reports no default external output port" in {
    val stats = statsWith("op", PortIdentity(1) -> 7L, PortIdentity(0, internal = true) -> 9L)
    resource.expectedDefaultPortOutputCount(stats, "op") shouldBe 0L
  }

  it should "pick the default external port when several output ports are reported" in {
    val stats = statsWith("op", PortIdentity(1) -> 7L, PortIdentity() -> 5L)
    resource.expectedDefaultPortOutputCount(stats, "op") shouldBe 5L
  }

  "committedDefaultPortCount" should "return None when the operator has no result storage" in {
    val committed = resource.committedDefaultPortCount(
      resultUriOf = _ => None,
      countOf = _ => fail("count should not be read when there is no result URI")
    ) _
    committed("op") shouldBe None
  }

  it should "return the committed row count when the result document is readable" in {
    val committed = resource.committedDefaultPortCount(
      resultUriOf = _ => Some(URI.create("mock://results/op")),
      countOf = _ => 123L
    ) _
    committed("op") shouldBe Some(123L)
  }

  it should "report 0 when the registered result document cannot be opened" in {
    val committed = resource.committedDefaultPortCount(
      resultUriOf = _ => Some(URI.create("mock://results/op")),
      countOf = _ => throw new RuntimeException("document not yet openable")
    ) _
    committed("op") shouldBe Some(0L)
  }
}

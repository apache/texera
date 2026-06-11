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

package org.apache.texera.amber.engine.architecture.worker.managers

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec

class StatisticsManagerSpec extends AnyFlatSpec {

  // Empty-iterator executor fixture so getStatistics can be invoked without
  // any side effects. Top-level (private object outside the class) so Kryo
  // / Scala don't capture an enclosing-instance reference.
  private object EmptyExec extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  "StatisticsManager (default state)" should "initialize all counters and accumulators to zero" in {
    val mgr = new StatisticsManager
    assert(mgr.getInputTupleCount == 0L)
    assert(mgr.getOutputTupleCount == 0L)
    val stats = mgr.getStatistics(EmptyExec)
    assert(stats.inputTupleMetrics.isEmpty)
    assert(stats.outputTupleMetrics.isEmpty)
    assert(stats.dataProcessingTime == 0L)
    assert(stats.controlProcessingTime == 0L)
    assert(stats.idleTime == 0L)
  }

  "StatisticsManager.increaseInputStatistics" should "accumulate count and size per port" in {
    val mgr = new StatisticsManager
    val p0 = PortIdentity(0)
    val p1 = PortIdentity(1)
    mgr.increaseInputStatistics(p0, size = 100L)
    mgr.increaseInputStatistics(p0, size = 50L)
    mgr.increaseInputStatistics(p1, size = 200L)
    assert(mgr.getInputTupleCount == 3L)
    val stats = mgr.getStatistics(EmptyExec)
    val p0Metrics = stats.inputTupleMetrics.find(_.portId == p0).get.tupleMetrics
    val p1Metrics = stats.inputTupleMetrics.find(_.portId == p1).get.tupleMetrics
    assert(p0Metrics.count == 2L && p0Metrics.size == 150L)
    assert(p1Metrics.count == 1L && p1Metrics.size == 200L)
  }

  it should "reject negative tuple sizes with IllegalArgumentException" in {
    val mgr = new StatisticsManager
    intercept[IllegalArgumentException] {
      mgr.increaseInputStatistics(PortIdentity(0), size = -1L)
    }
  }

  "StatisticsManager.increaseOutputStatistics" should "accumulate count and size per port" in {
    val mgr = new StatisticsManager
    val p0 = PortIdentity(0)
    mgr.increaseOutputStatistics(p0, size = 10L)
    mgr.increaseOutputStatistics(p0, size = 20L)
    assert(mgr.getOutputTupleCount == 2L)
    val stats = mgr.getStatistics(EmptyExec)
    val out = stats.outputTupleMetrics.find(_.portId == p0).get.tupleMetrics
    assert(out.count == 2L && out.size == 30L)
  }

  it should "reject negative tuple sizes with IllegalArgumentException" in {
    val mgr = new StatisticsManager
    intercept[IllegalArgumentException] {
      mgr.increaseOutputStatistics(PortIdentity(0), size = -7L)
    }
  }

  "StatisticsManager.increaseDataProcessingTime" should "accumulate non-negative time" in {
    val mgr = new StatisticsManager
    mgr.increaseDataProcessingTime(100L)
    mgr.increaseDataProcessingTime(50L)
    assert(mgr.getStatistics(EmptyExec).dataProcessingTime == 150L)
  }

  it should "reject negative time with IllegalArgumentException" in {
    val mgr = new StatisticsManager
    intercept[IllegalArgumentException] {
      mgr.increaseDataProcessingTime(-1L)
    }
  }

  "StatisticsManager.increaseControlProcessingTime" should "accumulate non-negative time" in {
    val mgr = new StatisticsManager
    mgr.increaseControlProcessingTime(33L)
    mgr.increaseControlProcessingTime(22L)
    assert(mgr.getStatistics(EmptyExec).controlProcessingTime == 55L)
  }

  it should "reject negative time with IllegalArgumentException" in {
    val mgr = new StatisticsManager
    intercept[IllegalArgumentException] {
      mgr.increaseControlProcessingTime(-1L)
    }
  }

  "StatisticsManager.updateTotalExecutionTime" should
    "compute elapsed since the start time and project to idle (total − data − control)" in {
    val mgr = new StatisticsManager
    mgr.initializeWorkerStartTime(1000L)
    mgr.increaseDataProcessingTime(100L)
    mgr.increaseControlProcessingTime(50L)
    mgr.updateTotalExecutionTime(1500L)
    val stats = mgr.getStatistics(EmptyExec)
    // total = 1500 - 1000 = 500; idle = 500 - 100 - 50 = 350
    assert(stats.dataProcessingTime == 100L)
    assert(stats.controlProcessingTime == 50L)
    assert(stats.idleTime == 350L)
  }

  it should "reject a `time` argument earlier than the recorded workerStartTime" in {
    val mgr = new StatisticsManager
    mgr.initializeWorkerStartTime(1000L)
    intercept[IllegalArgumentException] {
      mgr.updateTotalExecutionTime(999L)
    }
  }

  it should "accept time equal to workerStartTime (zero elapsed)" in {
    val mgr = new StatisticsManager
    mgr.initializeWorkerStartTime(1000L)
    mgr.updateTotalExecutionTime(1000L)
    val stats = mgr.getStatistics(EmptyExec)
    assert(stats.idleTime == 0L)
  }
}

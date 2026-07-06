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

package org.apache.texera.amber.engine.architecture.coordinator.execution

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics,
  WorkerState,
  WorkerStatistics
}
import org.scalatest.flatspec.AnyFlatSpec

class OperatorExecutionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures — small builders that keep tests readable
  // ---------------------------------------------------------------------------

  private def workerId(name: String): ActorVirtualIdentity = ActorVirtualIdentity(name)

  private def portTupleMetrics(portIdx: Int, count: Long, size: Long): PortTupleMetricsMapping =
    PortTupleMetricsMapping(PortIdentity(portIdx), TupleMetrics(count, size))

  /**
    * Push `(state, stats)` onto an existing `WorkerExecution`. Production
    * code applies updates only if the timestamp is newer than the
    * previously-recorded one; we use a monotonically increasing nano-clock
    * surrogate so each call wins.
    */
  private var clock: Long = 0L
  private def applyUpdate(
      worker: org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution,
      state: WorkerState,
      stats: WorkerStatistics
  ): Unit = {
    clock += 1
    worker.update(clock, state, stats)
  }
  private def setState(
      worker: org.apache.texera.amber.engine.architecture.deploysemantics.layer.WorkerExecution,
      state: WorkerState
  ): Unit = {
    clock += 1
    worker.update(clock, state)
  }

  // ---------------------------------------------------------------------------
  // initWorkerExecution + getWorkerExecution + getWorkerIds
  // ---------------------------------------------------------------------------

  "OperatorExecution.initWorkerExecution" should
    "register a fresh WorkerExecution and return it" in {
    val opExec = OperatorExecution()
    val w = workerId("w-1")
    val workerExec = opExec.initWorkerExecution(w)
    assert(workerExec != null)
    assert(opExec.getWorkerExecution(w) eq workerExec)
  }

  // The class docstring claims `initWorkerExecution` throws
  // `AssertionError` on a duplicate worker id, but the implementation's
  // `workerExecutions.contains(workerId)` call resolves to Java
  // `ConcurrentHashMap.contains(Object)`, which checks VALUES rather than
  // KEYS — so the assertion never fires and the second call silently
  // overwrites the prior WorkerExecution. We pin the CURRENT (broken)
  // behavior here so a future fix is noticed in CI, and document the
  // intended contract with `pendingUntilFixed` so the failure surfaces
  // the day the implementation is corrected.

  it should
    "currently overwrite the previous WorkerExecution on a second init for the same id " +
      "(characterization of the contains-by-value bug)" in {
    val opExec = OperatorExecution()
    val w = workerId("w-1")
    val firstExec = opExec.initWorkerExecution(w)
    val secondExec = opExec.initWorkerExecution(w)
    assert(firstExec ne secondExec, "current impl replaces the prior WorkerExecution instance")
    assert(opExec.getWorkerExecution(w) eq secondExec)
  }

  it should "(desired) throw AssertionError when initWorkerExecution is called twice for the same id" in pendingUntilFixed {
    val opExec = OperatorExecution()
    val w = workerId("w-1")
    opExec.initWorkerExecution(w)
    assertThrows[AssertionError] {
      opExec.initWorkerExecution(w)
    }
  }

  "OperatorExecution.getWorkerIds" should "be empty on a freshly constructed operator" in {
    val opExec = OperatorExecution()
    assert(opExec.getWorkerIds.isEmpty)
  }

  it should "contain every initialized worker id" in {
    val opExec = OperatorExecution()
    val w1 = workerId("w-1")
    val w2 = workerId("w-2")
    val w3 = workerId("w-3")
    opExec.initWorkerExecution(w1)
    opExec.initWorkerExecution(w2)
    opExec.initWorkerExecution(w3)
    assert(opExec.getWorkerIds == Set(w1, w2, w3))
  }

  // ---------------------------------------------------------------------------
  // getState — aggregation via ExecutionUtils.aggregateStates
  // ---------------------------------------------------------------------------

  "OperatorExecution.getState (no workers)" should
    "return UNINITIALIZED — empty Iterable falls through to the default branch" in {
    val opExec = OperatorExecution()
    assert(opExec.getState == WorkflowAggregatedState.UNINITIALIZED)
  }

  "OperatorExecution.getState (all workers COMPLETED)" should "return COMPLETED" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    setState(w1, WorkerState.COMPLETED)
    setState(w2, WorkerState.COMPLETED)
    assert(opExec.getState == WorkflowAggregatedState.COMPLETED)
  }

  "OperatorExecution.getState (any worker RUNNING)" should "return RUNNING" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    setState(w1, WorkerState.RUNNING)
    setState(w2, WorkerState.COMPLETED)
    assert(opExec.getState == WorkflowAggregatedState.RUNNING)
  }

  "OperatorExecution.getState (all workers UNINITIALIZED)" should "return UNINITIALIZED" in {
    val opExec = OperatorExecution()
    opExec.initWorkerExecution(workerId("w-1"))
    opExec.initWorkerExecution(workerId("w-2"))
    // Newly-constructed WorkerExecution is UNINITIALIZED by default; no
    // update needed.
    assert(opExec.getState == WorkflowAggregatedState.UNINITIALIZED)
  }

  // ---------------------------------------------------------------------------
  // getStats — port-metric aggregation + time sums
  // ---------------------------------------------------------------------------

  "OperatorExecution.getStats" should
    "aggregate per-port input/output metrics across workers (counts + sizes sum per portId)" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    // worker-1 sees 10 tuples / 100 bytes on input port 0; 5 / 50 on output port 0
    applyUpdate(
      w1,
      WorkerState.RUNNING,
      WorkerStatistics(
        inputTupleMetrics = Seq(portTupleMetrics(0, 10L, 100L)),
        outputTupleMetrics = Seq(portTupleMetrics(0, 5L, 50L)),
        dataProcessingTime = 7L,
        controlProcessingTime = 3L,
        idleTime = 1L
      )
    )
    // worker-2 sees 4 tuples / 40 bytes on input port 0; 2 / 20 on output port 0
    applyUpdate(
      w2,
      WorkerState.RUNNING,
      WorkerStatistics(
        inputTupleMetrics = Seq(portTupleMetrics(0, 4L, 40L)),
        outputTupleMetrics = Seq(portTupleMetrics(0, 2L, 20L)),
        dataProcessingTime = 11L,
        controlProcessingTime = 13L,
        idleTime = 17L
      )
    )

    val metrics = opExec.getStats
    // operatorState mirrors getState — both workers RUNNING → RUNNING
    assert(metrics.operatorState == WorkflowAggregatedState.RUNNING)

    val stats = metrics.operatorStatistics
    val inAgg = stats.inputMetrics.find(_.portId == PortIdentity(0)).get
    val outAgg = stats.outputMetrics.find(_.portId == PortIdentity(0)).get
    assert(inAgg.tupleMetrics.count == 14L)
    assert(inAgg.tupleMetrics.size == 140L)
    assert(outAgg.tupleMetrics.count == 7L)
    assert(outAgg.tupleMetrics.size == 70L)

    // Time fields are summed across all workers
    assert(stats.dataProcessingTime == 18L)
    assert(stats.controlProcessingTime == 16L)
    assert(stats.idleTime == 18L)
  }

  it should
    "keep distinct ports separate when workers report metrics on different ports" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    applyUpdate(
      w1,
      WorkerState.RUNNING,
      WorkerStatistics(
        inputTupleMetrics = Seq(portTupleMetrics(0, 3L, 30L)),
        outputTupleMetrics = Seq.empty,
        dataProcessingTime = 0L,
        controlProcessingTime = 0L,
        idleTime = 0L
      )
    )
    applyUpdate(
      w2,
      WorkerState.RUNNING,
      WorkerStatistics(
        inputTupleMetrics = Seq(portTupleMetrics(1, 5L, 50L)),
        outputTupleMetrics = Seq.empty,
        dataProcessingTime = 0L,
        controlProcessingTime = 0L,
        idleTime = 0L
      )
    )
    val stats = opExec.getStats.operatorStatistics
    val byPort = stats.inputMetrics.map(m => m.portId -> m.tupleMetrics).toMap
    assert(byPort.keySet == Set(PortIdentity(0), PortIdentity(1)))
    assert(byPort(PortIdentity(0)).count == 3L && byPort(PortIdentity(0)).size == 30L)
    assert(byPort(PortIdentity(1)).count == 5L && byPort(PortIdentity(1)).size == 50L)
  }

  it should "report zero counts / empty metrics for a freshly-constructed operator with no workers" in {
    val opExec = OperatorExecution()
    val stats = opExec.getStats.operatorStatistics
    assert(stats.inputMetrics.isEmpty)
    assert(stats.outputMetrics.isEmpty)
    assert(stats.dataProcessingTime == 0L)
    assert(stats.controlProcessingTime == 0L)
    assert(stats.idleTime == 0L)
  }

  // ---------------------------------------------------------------------------
  // isInputPortCompleted / isOutputPortCompleted
  // ---------------------------------------------------------------------------

  "OperatorExecution.isInputPortCompleted" should
    "return true only when every worker reports the port as completed" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    val port = PortIdentity(0)
    // Initially no port executions touched — getInputPortExecution
    // lazily creates them with completed = false.
    assert(!opExec.isInputPortCompleted(port))
    // Only worker-1 completes the port — operator still not complete.
    w1.getInputPortExecution(port).setCompleted()
    assert(!opExec.isInputPortCompleted(port))
    // Both workers complete → operator completes.
    w2.getInputPortExecution(port).setCompleted()
    assert(opExec.isInputPortCompleted(port))
  }

  "OperatorExecution.isOutputPortCompleted" should
    "return true only when every worker reports the port as completed" in {
    val opExec = OperatorExecution()
    val w1 = opExec.initWorkerExecution(workerId("w-1"))
    val w2 = opExec.initWorkerExecution(workerId("w-2"))
    val port = PortIdentity(0)
    assert(!opExec.isOutputPortCompleted(port))
    w1.getOutputPortExecution(port).setCompleted()
    assert(!opExec.isOutputPortCompleted(port))
    w2.getOutputPortExecution(port).setCompleted()
    assert(opExec.isOutputPortCompleted(port))
  }

  it should "distinguish input port completion from output port completion (same portId)" in {
    // Same portId on input vs output is tracked by independent
    // WorkerPortExecution instances. Completing the input port must NOT
    // also flip the output port — and vice versa.
    val opExec = OperatorExecution()
    val w = opExec.initWorkerExecution(workerId("w-1"))
    val port = PortIdentity(0)
    w.getInputPortExecution(port).setCompleted()
    assert(opExec.isInputPortCompleted(port))
    assert(!opExec.isOutputPortCompleted(port))
  }
}

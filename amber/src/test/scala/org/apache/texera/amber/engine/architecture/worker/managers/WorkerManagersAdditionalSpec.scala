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

import org.apache.texera.amber.core.executor.{
  OpExecInitInfo,
  OpExecWithClassName,
  OpExecWithCode,
  OperatorExecutor
}
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest
import org.apache.texera.amber.engine.common.{CheckpointState, CheckpointSupport}
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

class WorkerManagersAdditionalSpec extends AnyFlatSpec {

  // ===========================================================================
  // StatisticsManager
  // ===========================================================================

  "StatisticsManager (default state)" should "initialize all counters and accumulators to zero" in {
    val mgr = new StatisticsManager
    assert(mgr.getInputTupleCount == 0L)
    assert(mgr.getOutputTupleCount == 0L)
    // Empty executor fixture so getStatistics can be invoked; only the
    // counters / time accumulators are exercised here.
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

  // ===========================================================================
  // SerializationManager
  // ===========================================================================

  // Build a real worker actor id via the same utility production uses, so
  // VirtualIdentityUtils.getWorkerIndex returns Some(idx) and the
  // "expected worker actor id" guard doesn't fire.
  private val workflowIdent = org.apache.texera.amber.core.virtualidentity.WorkflowIdentity(1L)
  private val opId = org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity(
    org.apache.texera.amber.core.virtualidentity.OperatorIdentity("op-a"),
    "main"
  )
  private val workerActorId: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(workflowIdent, opId, workerId = 0)
  // A non-worker actor id (created via the plain string constructor, not the
  // worker-identity factory) — VirtualIdentityUtils.getWorkerIndex will
  // return None for this, triggering the IllegalStateException guard.
  private val controllerActorId: ActorVirtualIdentity = ActorVirtualIdentity("controller")

  private def mkRequest(info: OpExecInitInfo, totalWorkers: Int = 1): InitializeExecutorRequest =
    InitializeExecutorRequest(
      totalWorkerCount = totalWorkers,
      opExecInitInfo = info,
      isSource = false
    )

  "SerializationManager.restoreExecutorState" should
    "throw IllegalStateException when actorId is not a worker identity" in {
    val mgr = new SerializationManager(controllerActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[WorkerManagersAdditionalSpec.NoArgExec].getName,
          descString = ""
        )
      )
    )
    val ex = intercept[IllegalStateException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
    assert(ex.getMessage.contains("worker"))
  }

  it should "instantiate via ExecFactory.newExecFromJavaClassName for OpExecWithClassName" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[WorkerManagersAdditionalSpec.NoArgExec].getName,
          descString = ""
        )
      )
    )
    val (executor, iter) = mgr.restoreExecutorState(new CheckpointState())
    assert(executor.isInstanceOf[WorkerManagersAdditionalSpec.NoArgExec])
    // Non-CheckpointSupport executor → empty restoration iterator.
    val restoredList = iter.toList
    assert(restoredList.isEmpty)
  }

  it should "throw UnsupportedOperationException on OpExecInitInfo.Empty (unsupported variant)" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(mkRequest(OpExecInitInfo.Empty))
    intercept[UnsupportedOperationException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
  }

  it should "delegate to executor.deserializeState when the constructed executor is CheckpointSupport" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[WorkerManagersAdditionalSpec.CheckpointAwareExec].getName,
          descString = ""
        )
      )
    )
    val (executor, iter) = mgr.restoreExecutorState(new CheckpointState())
    assert(executor.isInstanceOf[WorkerManagersAdditionalSpec.CheckpointAwareExec])
    // The fixture returns a sentinel via deserializeState; if the
    // SerializationManager mistakenly used the non-CheckpointSupport
    // path (Iterator.empty), this would fail.
    val restored = iter.toList
    assert(restored.size == 1, s"expected one sentinel element, got: $restored")
  }

  it should "raise RuntimeException via the diagnostic path when OpExecWithCode is broken Java" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(OpExecWithCode(code = "public class JavaUDFOpExec { not valid }", language = ""))
    )
    val ex = intercept[RuntimeException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
    assert(ex.getMessage.toLowerCase.contains("error"))
  }

  "SerializationManager.registerSerialization + applySerialization" should
    "invoke the registered callback exactly once and clear it afterward" in {
    val mgr = new SerializationManager(workerActorId)
    var calls = 0
    mgr.registerSerialization(() => calls += 1)
    mgr.applySerialization()
    assert(calls == 1)
    // A second applySerialization with no re-register must NOT re-invoke
    // the cleared callback (idempotency under the "fire once" contract).
    mgr.applySerialization()
    assert(calls == 1, "applySerialization must clear the callback after the first invocation")
  }

  it should "be a safe no-op when no callback has been registered" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.applySerialization() // must not throw NPE
    succeed
  }

  it should "honor a re-registered callback after a previous applySerialization cleared it" in {
    val mgr = new SerializationManager(workerActorId)
    var first = 0
    var second = 0
    mgr.registerSerialization(() => first += 1)
    mgr.applySerialization()
    mgr.registerSerialization(() => second += 1)
    mgr.applySerialization()
    assert(first == 1)
    assert(second == 1)
  }
}

object WorkerManagersAdditionalSpec {
  // No-arg executor fixture for the ExecFactory reflection path. Lives on
  // the companion (top-level binary name) so Class.forName + the no-arg
  // constructor reach it without an enclosing-instance reference.
  class NoArgExec extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  /** Mixes CheckpointSupport so the SerializationManager's
    * deserializeState branch is exercised. The fixture returns a single
    * sentinel element so the assertion can distinguish this branch from
    * the empty-iterator non-CheckpointSupport branch.
    */
  class CheckpointAwareExec extends OperatorExecutor with CheckpointSupport {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
    override def serializeState(
        currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = currentIteratorState
    override def deserializeState(
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = {
      val sentinel: TupleLike = Tuple
        .builder(
          new org.apache.texera.amber.core.tuple.Schema()
        )
        .build()
      Iterator((sentinel, None))
    }
    override def getEstimatedCheckpointCost: Long = 0L
  }
}

// Empty-iterator executor fixture for the StatisticsManager tests. Lives at
// top level (separate file-private object) so `mgr.getStatistics(EmptyExec)`
// can be called without dragging an enclosing instance.
private object EmptyExec extends OperatorExecutor {
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
}

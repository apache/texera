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

package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.controller.execution.ExecutionUtils.aggregateStates
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerExecution
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  WorkerState
}
import edu.uci.ics.amber.engine.common.executionruntimestate.{OperatorMetrics, OperatorStatistics}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.util
import scala.jdk.CollectionConverters._

case class OperatorExecution() {

  private val workerExecutions =
    new util.concurrent.ConcurrentHashMap[ActorVirtualIdentity, WorkerExecution]()

  /**
    * Initializes a `WorkerExecution` for the specified workerId and adds it to the workerExecutions map.
    * If a `WorkerExecution` for the given workerId already exists, an AssertionError is thrown.
    * After successfully adding the new `WorkerExecution`, it retrieves and returns the newly added instance.
    *
    * @param workerId The `ActorVirtualIdentity` representing the unique identity of the worker.
    * @return The `WorkerExecution` instance associated with the specified workerId.
    * @throws AssertionError if a `WorkerExecution` already exists for the given workerId.
    */
  def initWorkerExecution(workerId: ActorVirtualIdentity): WorkerExecution = {
    assert(
      !workerExecutions.contains(workerId),
      s"WorkerExecution already exists for workerId: $workerId"
    )
    workerExecutions.put(workerId, WorkerExecution())
    getWorkerExecution(workerId)
  }

  /**
    * Retrieves the `WorkerExecution` instance associated with the specified workerId.
    */
  def getWorkerExecution(workerId: ActorVirtualIdentity): WorkerExecution =
    workerExecutions.get(workerId)

  /**
    * Retrieves the set of all workerIds for which `WorkerExecution` instances have been initialized.
    */
  def getWorkerIds: Set[ActorVirtualIdentity] = workerExecutions.keys.asScala.toSet

  def getState: WorkflowAggregatedState = {
    val workerStates = workerExecutions.values.asScala.map(_.getState)
    aggregateStates(
      workerStates,
      WorkerState.COMPLETED,
      WorkerState.TERMINATED,
      WorkerState.RUNNING,
      WorkerState.UNINITIALIZED,
      WorkerState.PAUSED,
      WorkerState.READY
    )
  }

  private[this] def computeOperatorPortStats(
      workerPortStats: Iterable[PortTupleMetricsMapping]
  ): Seq[PortTupleMetricsMapping] = {
    ExecutionUtils.aggregatePortMetrics(workerPortStats)
  }

  def getStats: OperatorMetrics = {
    val workerRawStats = workerExecutions.values.asScala.map(_.getStats)
    val inputMetrics = workerRawStats.flatMap(_.inputTupleMetrics)
    val outputMetrics = workerRawStats.flatMap(_.outputTupleMetrics)
    OperatorMetrics(
      getState,
      OperatorStatistics(
        inputMetrics = computeOperatorPortStats(inputMetrics),
        outputMetrics = computeOperatorPortStats(outputMetrics),
        getWorkerIds.size,
        dataProcessingTime = workerRawStats.map(_.dataProcessingTime).sum,
        controlProcessingTime = workerRawStats.map(_.controlProcessingTime).sum,
        idleTime = workerRawStats.map(_.idleTime).sum
      )
    )
  }

  def isInputPortCompleted(portId: PortIdentity): Boolean = {
    workerExecutions
      .values()
      .asScala
      .map(workerExecution => workerExecution.getInputPortExecution(portId))
      .forall(_.completed)
  }

  def isOutputPortCompleted(portId: PortIdentity): Boolean = {
    workerExecutions
      .values()
      .asScala
      .map(workerExecution => workerExecution.getOutputPortExecution(portId))
      .forall(_.completed)
  }
}

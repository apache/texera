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

package org.apache.texera.amber.engine.architecture.deploysemantics.layer

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.controller.execution.WorkerPortExecution
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  TERMINATED,
  UNINITIALIZED
}
import org.apache.texera.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}

import scala.collection.mutable

case class WorkerExecution() extends Serializable {

  private val inputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] =
    mutable.HashMap()
  private val outputPortExecutions: mutable.HashMap[PortIdentity, WorkerPortExecution] =
    mutable.HashMap()

  private var state: WorkerState = UNINITIALIZED
  private var stats: WorkerStatistics = {
    WorkerStatistics(Seq.empty, Seq.empty, 0, 0, 0)
  }
  // Logical version of the last applied state, sourced from the worker's
  // WorkerStateManager. Starts below any real version so the first report applies.
  private var lastStateVersion = -1L
  // Controller-side receipt time (System.nanoTime) of the last applied stats snapshot.
  private var lastStatsTimeStamp = 0L

  private def isTerminal(s: WorkerState): Boolean = s == COMPLETED || s == TERMINATED

  /**
    * Applies a worker state report, ordered causally by the worker's monotonic
    * `stateVersion` rather than by receipt time. A report is applied only when it
    * is strictly newer than the last applied one, so a stale state that arrives late
    * (e.g. the RUNNING snapshot carried by a slow startWorker response) cannot clobber
    * a newer state. In addition, terminal states (COMPLETED/TERMINATED) are absorbing:
    * once reached, no later report can move the worker out of them.
    *
    * @param stateVersion the worker-side monotonic version of this state
    * @param newState the reported WorkerState
    */
  def updateState(stateVersion: Long, newState: WorkerState): Unit = {
    if (isTerminal(this.state)) {
      return
    }
    if (this.lastStateVersion < stateVersion) {
      this.state = newState
      this.lastStateVersion = stateVersion
    }
  }

  /**
    * Forces the worker into TERMINATED, e.g. when the controller kills a region. This
    * still respects terminal-state absorption: a worker that already COMPLETED on its
    * own is left as COMPLETED. Uses the maximum version so it wins over any in-flight
    * non-terminal report for a worker that had not yet reached a terminal state.
    */
  def forceTerminate(): Unit = updateState(Long.MaxValue, TERMINATED)

  /**
    * Updates only the worker statistics if the provided timestamp is newer than the
    * last recorded stats timestamp. Stats are monotonic snapshots, so newest-wins by
    * receipt time is sufficient (and necessary, since two snapshots taken within the
    * same state share a state version).
    *
    * @param timeStamp the nanosecond-timestamp of this update
    * @param newStats the new WorkerStatistics to set
    */
  def updateStats(timeStamp: Long, newStats: WorkerStatistics): Unit = {
    if (this.lastStatsTimeStamp < timeStamp) {
      this.stats = newStats
      this.lastStatsTimeStamp = timeStamp
    }
  }

  def getState: WorkerState = state

  def getStats: WorkerStatistics = stats

  def getInputPortExecution(portId: PortIdentity): WorkerPortExecution = {
    if (!inputPortExecutions.contains(portId)) {
      inputPortExecutions(portId) = new WorkerPortExecution()
    }
    inputPortExecutions(portId)

  }

  def getOutputPortExecution(portId: PortIdentity): WorkerPortExecution = {
    if (!outputPortExecutions.contains(portId)) {
      outputPortExecutions(portId) = new WorkerPortExecution()
    }
    outputPortExecutions(portId)

  }
}

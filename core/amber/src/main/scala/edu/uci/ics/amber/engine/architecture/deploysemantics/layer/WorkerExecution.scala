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

package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.controller.execution.WorkerPortExecution
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.UNINITIALIZED
import edu.uci.ics.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import edu.uci.ics.amber.core.workflow.PortIdentity

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
  private var lastUpdateTimeStamp = 0L

  /**
    * Updates both the worker state and statistics if the provided timestamp is newer
    * than the last recorded update timestamp. This ensures that only the most recent
    * data is reflected in the execution state.
    *
    * @param timeStamp the nanosecond-timestamp of this update
    * @param state the new WorkerState to set
    * @param stats the new WorkerStatistics to set
    */
  def update(timeStamp: Long, state: WorkerState, stats: WorkerStatistics): Unit = {
    if (this.lastUpdateTimeStamp < timeStamp) {
      this.stats = stats
      this.state = state
      this.lastUpdateTimeStamp = timeStamp
    }
  }

  /**
    * Updates only the worker state if the provided timestamp is newer than the
    * last recorded update timestamp.
    *
    * @param timeStamp the nanosecond-timestamp of this update
    * @param state the new WorkerState to set
    */
  def update(timeStamp: Long, state: WorkerState): Unit = {
    if (this.lastUpdateTimeStamp < timeStamp) {
      this.state = state
      this.lastUpdateTimeStamp = timeStamp
    }
  }

  /**
    * Updates only the worker statistics if the provided timestamp is newer than the
    * last recorded update timestamp.
    *
    * @param timeStamp the nanosecond-timestamp of this update
    * @param stats the new WorkerStatistics to set
    */
  def update(timeStamp: Long, stats: WorkerStatistics): Unit = {
    if (this.lastUpdateTimeStamp < timeStamp) {
      this.stats = stats
      this.lastUpdateTimeStamp = timeStamp
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

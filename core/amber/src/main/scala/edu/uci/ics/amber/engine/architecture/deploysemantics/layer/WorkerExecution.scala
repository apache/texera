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
  private var stats: WorkerStatistics =
    WorkerStatistics(Seq.empty, Seq.empty, 0, 0, 0)

  def getState: WorkerState = state

  def setState(state: WorkerState): Unit = {
    this.state = state
  }

  def getStats: WorkerStatistics = stats

  def setStats(stats: WorkerStatistics): Unit = {
    this.stats = stats
  }

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

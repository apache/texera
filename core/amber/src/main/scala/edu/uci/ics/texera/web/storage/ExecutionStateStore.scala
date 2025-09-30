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

package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.common.Utils.maptoStatusCode
import edu.uci.ics.amber.engine.common.executionruntimestate.{
  ExecutionBreakpointStore,
  ExecutionConsoleStore,
  ExecutionMetadataStore,
  ExecutionStatsStore
}
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService

import java.sql.Timestamp

object ExecutionStateStore {

  // Update the state of the specified execution if user system is enabled.
  // Update the execution only from backend
  def updateWorkflowState(
      state: WorkflowAggregatedState,
      metadataStore: ExecutionMetadataStore
  ): ExecutionMetadataStore = {
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(metadataStore.executionId) {
      execution =>
        execution.setStatus(maptoStatusCode(state))
        execution.setLastUpdateTime(new Timestamp(System.currentTimeMillis()))
    }
    metadataStore.withState(state)
  }
}

// states that within one execution.
class ExecutionStateStore {
  val statsStore = new StateStore(ExecutionStatsStore())
  val metadataStore = new StateStore(ExecutionMetadataStore())
  val consoleStore = new StateStore(ExecutionConsoleStore())
  val breakpointStore = new StateStore(ExecutionBreakpointStore())
  val reconfigurationStore = new StateStore(ExecutionReconfigurationStore())

  def getAllStores: Iterable[StateStore[_]] = {
    Iterable(statsStore, consoleStore, breakpointStore, metadataStore, reconfigurationStore)
  }
}

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

package org.apache.texera.amber.core.workflow

/**
  * The codes persisted in `workflow_executions.status` for an execution that has ended. These are
  * the collapsed codes produced by amber's `Utils.maptoStatusCode`, NOT the ordinals of
  * `WorkflowAggregatedState`, and the services that read them cannot depend on the amber module to
  * reuse either. Keep this in sync with `Utils.maptoStatusCode`.
  *
  * Only the terminal codes are listed, and every other code means an execution still in flight.
  * That direction matters: `maptoStatusCode` collapses PAUSING, RESUMING, UNKNOWN and TERMINATED
  * alike to -1, so enumerating the non-terminal codes instead would make a paused-or-resuming
  * execution look ended.
  *
  * Known limitation: a non-terminal code is trusted without a time bound, so a row that never
  * reaches 3/4/5 never counts as ended. Two ways in:
  *   - a row left stuck at RUNNING -- a coordinator that died in place, an OOM inside the
  *     container -- because nothing ever rewrites it;
  *   - a row whose *final* status is -1, because `maptoStatusCode` gives an execution that
  *     ended the same code as one that is merely paused. Today that is UNKNOWN, the fallback
  *     `WorkflowExecution.getState` returns for a worker mix it cannot name. TERMINATED maps
  *     to -1 as well, but is a worker-level state: `ExecutionUtils.aggregateStates` reports a
  *     COMPLETED-or-TERMINATED worker set as COMPLETED, so it does not reach this column
  *     today. If amber ever persists it, it lands in this same bucket.
  * Neither is separable here -- a -1 carries nothing that distinguishes an ended execution
  * from a live one, so the fix belongs in what amber persists. Tracked as a follow-up in
  * apache/texera#8618.
  */
object TerminalWorkflowExecutionStatus extends Enumeration {
  val Completed: Value = Value(3)
  val Failed: Value = Value(4)
  val Killed: Value = Value(5)

  def dbStatuses: Seq[java.lang.Short] =
    values.toSeq.map(status => Short.box(status.id.toShort))
}

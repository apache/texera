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

package org.apache.texera.web.observability

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.virtualidentity.ExecutionIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED,
  PAUSED,
  PAUSING,
  RESUMING,
  RUNNING
}
import org.apache.texera.observability.WorkflowMetrics
import org.apache.texera.observability.WorkflowMetrics.WorkflowKind
import org.apache.texera.web.service.WorkflowService

import java.util.concurrent.ConcurrentHashMap

/**
  * Drives [[WorkflowMetrics]] from amber's execution lifecycle. The metric
  * instruments live in common/config; this object is the single place
  * that records them, so the lifecycle code only needs one-line calls.
  *
  *  - start / terminal counters and the duration histogram are recorded
  *    from [[onStart]] and [[onStateChange]];
  *  - the always-polled `texera.workflow.active` gauge is sourced from the
  *    live WorkflowService registry via the supplier registered in [[init]].
  */
object WorkflowMetricsRecorder extends LazyLogging {

  // Start time + kind per in-flight run, so a terminal transition can emit
  // a duration and attribute the outcome. Keyed by the execution identity.
  private val inFlight = new ConcurrentHashMap[ExecutionIdentity, (Long, WorkflowKind)]()

  private val ActiveStates: Set[WorkflowAggregatedState] = Set(RUNNING, PAUSING, PAUSED, RESUMING)
  private val TerminalStates: Set[WorkflowAggregatedState] = Set(COMPLETED, FAILED, KILLED)

  /** Register the active-executions gauge supplier and bind the instruments.
    *  Call once at startup, after OtelInit.init. The supplier is polled on
    *  every metric collection, so it must never throw.
    */
  def init(): Unit = {
    WorkflowMetrics.setActiveExecutionsSupplier(() =>
      try {
        WorkflowService.getAllWorkflowServices.iterator
          .flatMap(s => Option(s.executionService.getValue))
          .map(_.executionStateStore.metadataStore.getState.state)
          .count(ActiveStates.contains)
          .toLong
      } catch {
        case _: Throwable => 0L
      }
    )
    WorkflowMetrics.ensureBound()
  }

  /** Record that a run started. */
  def onStart(
      executionId: ExecutionIdentity,
      kind: WorkflowKind = WorkflowKind.Interactive
  ): Unit = {
    inFlight.put(executionId, (System.currentTimeMillis(), kind))
    WorkflowMetrics.recordStart(kind)
  }

  /** Record terminal counters + duration exactly once, on the first
    *  transition from a non-terminal into a terminal state. Safe to call on
    *  every state change; non-terminal and repeat-terminal calls are no-ops.
    */
  def onStateChange(
      executionId: ExecutionIdentity,
      oldState: WorkflowAggregatedState,
      newState: WorkflowAggregatedState
  ): Unit = {
    if (!TerminalStates.contains(newState) || TerminalStates.contains(oldState)) return
    val entry = Option(inFlight.remove(executionId))
    val kind = entry.map(_._2).getOrElse(WorkflowKind.Interactive)
    val durationSec = entry.map(e => (System.currentTimeMillis() - e._1) / 1000.0).getOrElse(0.0)
    newState match {
      case COMPLETED => WorkflowMetrics.recordCompletion(kind, durationSec)
      case FAILED    => WorkflowMetrics.recordFailure(kind, durationSec)
      case KILLED    => WorkflowMetrics.recordCancellation(kind)
      case _         => ()
    }
  }
}

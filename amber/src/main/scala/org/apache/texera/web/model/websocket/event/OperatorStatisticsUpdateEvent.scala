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

package org.apache.texera.web.model.websocket.event

import org.apache.texera.amber.engine.common.Utils.aggregatedStateToString
import org.apache.texera.amber.engine.common.executionruntimestate.OperatorMetrics

case class OperatorAggregatedMetrics(
    operatorState: String,
    aggregatedInputRowCount: Long,
    aggregatedInputSize: Long,
    inputPortMetrics: Map[String, Long],
    aggregatedOutputRowCount: Long,
    aggregatedOutputSize: Long,
    outputPortMetrics: Map[String, Long],
    numWorkers: Long,
    aggregatedDataProcessingTime: Long,
    aggregatedControlProcessingTime: Long,
    aggregatedIdleTime: Long,
    // Provenance: the operator completed by reusing cached results (no workers ran).
    // Deliberately no default: a new construction site must decide the flag
    // explicitly instead of silently sending false.
    reusedFromCache: Boolean,
    inputPortSizes: Map[String, Long] = Map.empty,
    outputPortSizes: Map[String, Long] = Map.empty
)

object OperatorAggregatedMetrics {
  def from(metrics: OperatorMetrics): OperatorAggregatedMetrics = {
    val stats = metrics.operatorStatistics
    val inputs = stats.inputMetrics.map(p => p.portId.id.toString -> p.tupleMetrics).toMap
    val outputs = stats.outputMetrics.map(p => p.portId.id.toString -> p.tupleMetrics).toMap
    OperatorAggregatedMetrics(
      aggregatedStateToString(metrics.operatorState),
      stats.inputMetrics.map(_.tupleMetrics.count).sum,
      stats.inputMetrics.map(_.tupleMetrics.size).sum,
      inputs.view.mapValues(_.count).toMap,
      stats.outputMetrics.map(_.tupleMetrics.count).sum,
      stats.outputMetrics.map(_.tupleMetrics.size).sum,
      outputs.view.mapValues(_.count).toMap,
      stats.numWorkers,
      stats.dataProcessingTime,
      stats.controlProcessingTime,
      stats.idleTime,
      metrics.reusedFromCache,
      inputs.view.mapValues(_.size).toMap,
      outputs.view.mapValues(_.size).toMap
    )
  }
}

case class OperatorStatisticsUpdateEvent(operatorStatistics: Map[String, OperatorAggregatedMetrics])
    extends TexeraWebSocketEvent

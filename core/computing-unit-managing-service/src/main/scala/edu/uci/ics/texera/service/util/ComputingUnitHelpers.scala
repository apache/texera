/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.texera.service.util

import edu.uci.ics.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import edu.uci.ics.texera.service.resource.ComputingUnitState.{ComputingUnitState, Pending, Running}

object ComputingUnitHelpers {
  def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState = {
    unit.getType match {
      // Local CUs are always “running”
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // Kubernetes CUs – only explicit “Running” counts as running
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val phaseOpt = KubernetesClient
          .getPodByName(KubernetesClient.generatePodName(unit.getCuid))
          .map(_.getStatus.getPhase)

        if (phaseOpt.contains("Running")) Running else Pending

      // Any other (unknown) type is treated as pending
      case _ =>
        Pending
    }
  }

  def getComputingUnitMetrics(unit: WorkflowComputingUnit): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = KubernetesClient.getPodMetrics(unit.getCuid)
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }
}

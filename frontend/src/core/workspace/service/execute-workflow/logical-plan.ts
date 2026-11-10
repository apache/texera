/**
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

import type { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";
import type { LogicalPlan, LogicalOperator, LogicalLink } from "../../types/execute-workflow.interface";
import { intersection } from "../../../common/util/set";

export function buildLogicalPlan(workflowGraph: WorkflowGraphReadonly, targetOperatorId?: string): LogicalPlan {
  const getInputPortOrdinal = (operatorID: string, inputPortID: string): number => {
    return workflowGraph.getOperator(operatorID).inputPorts.findIndex(port => port.portID === inputPortID);
  };

  const getOutputPortOrdinal = (operatorID: string, outputPortID: string): number => {
    return workflowGraph.getOperator(operatorID).outputPorts.findIndex(port => port.portID === outputPortID);
  };
  const subDAG = workflowGraph.getSubDAG(targetOperatorId);

  const operators: LogicalOperator[] = subDAG.operators.map(op => ({
    ...op.operatorProperties,
    operatorID: op.operatorID,
    operatorType: op.operatorType,
    inputPorts: op.inputPorts,
    outputPorts: op.outputPorts,
  }));

  const links: LogicalLink[] = subDAG.links.map(link => {
    const outputPortIdx = getOutputPortOrdinal(link.source.operatorID, link.source.portID);
    const inputPortIdx = getInputPortOrdinal(link.target.operatorID, link.target.portID);
    return {
      fromOpId: link.source.operatorID,
      fromPortId: { id: outputPortIdx, internal: false },
      toOpId: link.target.operatorID,
      toPortId: { id: inputPortIdx, internal: false },
    };
  });

  const operatorIds = new Set(subDAG.operators.map(op => op.operatorID));

  const opsToViewResult: string[] = Array.from(intersection(operatorIds, workflowGraph.getOperatorsToViewResult()));

  const opsToReuseResult: string[] = Array.from(
    intersection(operatorIds, workflowGraph.getOperatorsMarkedForReuseResult())
  );

  return { operators, links, opsToViewResult, opsToReuseResult };
}

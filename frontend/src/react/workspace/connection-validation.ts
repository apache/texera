/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { WorkflowGraphReadonly } from "../../core/workspace/service/workflow-graph/model/workflow-graph";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
/** Original editor wiring rules, shared by pointer validation and the model bridge. */
export function canConnect(
  graph: WorkflowGraphReadonly,
  metadata: OperatorMetadata,
  sourceId: string,
  sourcePort: string | null | undefined,
  targetId: string,
  targetPort: string | null | undefined,
  ignoredLinkId?: string
): boolean {
  if (
    sourceId === targetId ||
    !sourcePort ||
    !targetPort ||
    !graph.hasOperator(sourceId) ||
    !graph.hasOperator(targetId)
  )
    return false;
  const source = graph.getOperator(sourceId),
    target = graph.getOperator(targetId);
  if (!source.outputPorts.some(port => port.portID === sourcePort)) return false;
  const portIndex = target.inputPorts.findIndex(port => port.portID === targetPort);
  if (portIndex < 0) return false;
  const links = graph
    .getAllLinks()
    .filter(
      link => link.linkID !== ignoredLinkId && link.target.operatorID === targetId && link.target.portID === targetPort
    );
  if (links.some(link => link.source.operatorID === sourceId && link.source.portID === sourcePort)) return false;
  const schema =
    metadata.operators.find(
      schema => schema.operatorType === target.operatorType && schema.operatorVersion === target.operatorVersion
    ) ?? metadata.operators.find(schema => schema.operatorType === target.operatorType);
  const single =
    schema?.additionalMetadata.inputPorts[portIndex]?.disallowMultiLinks ??
    target.inputPorts[portIndex].disallowMultiInputs ??
    false;
  return !(single && links.length > 0);
}

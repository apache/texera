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

import dagre from "dagre";
import type { WorkflowState } from "../workflow-state";

// Values mirror frontend joint-graph-wrapper.ts so agent-generated and
// user-generated layouts visually match.
const LAYOUT_CONFIG: dagre.GraphLabel = {
  nodesep: 100,
  edgesep: 150,
  ranksep: 100,
  ranker: "tight-tree",
  rankdir: "LR",
};

const NODE_WIDTH = 200;
const NODE_HEIGHT = 80;

export function autoLayoutWorkflow(workflowState: WorkflowState): void {
  const operators = workflowState.getAllOperators();
  const links = workflowState.getAllLinks();

  if (operators.length === 0) {
    return;
  }

  const graph = new dagre.graphlib.Graph();
  graph.setGraph(LAYOUT_CONFIG);
  graph.setDefaultEdgeLabel(() => ({}));

  for (const operator of operators) {
    graph.setNode(operator.operatorID, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    });
  }

  for (const link of links) {
    graph.setEdge(link.source.operatorID, link.target.operatorID);
  }

  dagre.layout(graph);

  for (const operator of operators) {
    const node = graph.node(operator.operatorID);
    if (node) {
      workflowState.updateOperatorPosition(operator.operatorID, {
        x: node.x,
        y: node.y,
      });
    }
  }
}

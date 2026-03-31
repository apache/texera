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

/**
 * Auto Layout Utility for Texera Workflows
 *
 * Uses the dagre library to automatically arrange operators in a left-to-right
 * directed graph layout. Configuration matches the frontend JointJS layout
 * (see frontend/src/app/workspace/service/workflow-graph/model/joint-graph-wrapper.ts)
 */

import dagre from "dagre";
import type { WorkflowState } from "./workflow-state";

/**
 * Layout configuration matching frontend settings.
 * @see joint-graph-wrapper.ts:591-605
 */
const LAYOUT_CONFIG: dagre.GraphLabel = {
  nodesep: 100, // Vertical spacing between nodes at the same rank
  edgesep: 150, // Spacing for edge routing
  ranksep: 100, // Horizontal spacing between ranks
  ranker: "tight-tree", // Layout algorithm for tight, tree-like structures
  rankdir: "LR", // Left-to-right direction
};

/**
 * Default operator node dimensions for layout calculation.
 * These approximate the visual size of operator nodes in the frontend.
 */
const NODE_WIDTH = 200;
const NODE_HEIGHT = 80;

/**
 * Applies automatic layout to all operators in the workflow.
 *
 * Uses the dagre directed graph layout algorithm to calculate optimal
 * positions for operators based on their connections. The layout flows
 * left-to-right, with source operators on the left and sink operators
 * on the right.
 *
 * @param workflowState - The workflow state containing operators and links
 */
export function autoLayoutWorkflow(workflowState: WorkflowState): void {
  const operators = workflowState.getAllOperators();
  const links = workflowState.getAllLinks();

  // Skip layout for empty workflows
  if (operators.length === 0) {
    return;
  }

  // Create a new directed graph
  const graph = new dagre.graphlib.Graph();
  graph.setGraph(LAYOUT_CONFIG);
  graph.setDefaultEdgeLabel(() => ({}));

  // Add all operators as nodes
  for (const operator of operators) {
    graph.setNode(operator.operatorID, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    });
  }

  // Add all links as edges
  for (const link of links) {
    graph.setEdge(link.source.operatorID, link.target.operatorID);
  }

  // Run the dagre layout algorithm
  dagre.layout(graph);

  // Update operator positions in workflow state
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

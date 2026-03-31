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
 * Workflow State Manager - maintains the workflow graph state for the agent.
 * Uses RxJS for reactive state management, following the same patterns as the frontend.
 */

import { Subject, Observable, merge, Subscription } from "rxjs";
import type {
  OperatorPredicate,
  OperatorLink,
  WorkflowContent,
  LogicalPlan,
  LogicalOperator,
  LogicalLink,
  Point,
  CommentBox,
  WorkflowSettings,
  ValidationError,
} from "../types/workflow";

// Re-export validation types for backwards compatibility
export type { ValidationError, Validation } from "../types/workflow";

// ============================================================================
// Validation Output Type
// ============================================================================

export interface ValidationOutput {
  errors: Record<string, ValidationError>;
  workflowEmpty: boolean;
}


// ============================================================================
// Workflow State Class
// ============================================================================

/**
 * WorkflowState maintains the complete state of a workflow including:
 * - Operators and links (the graph structure)
 * - Validation state
 *
 * Uses RxJS Subjects for reactive event streams, following the frontend pattern.
 * Note: Compilation is done on-demand in tools, not cached here.
 */
// Default workflow settings
const DEFAULT_WORKFLOW_SETTINGS: WorkflowSettings = {
  dataTransferBatchSize: 400,
};

export class WorkflowState {
  // Graph state
  private operators: Map<string, OperatorPredicate> = new Map();
  private links: Map<string, OperatorLink> = new Map();
  private operatorPositions: Map<string, Point> = new Map();
  private commentBoxes: CommentBox[] = [];
  private settings: WorkflowSettings = { ...DEFAULT_WORKFLOW_SETTINGS };
  private operatorsToViewResult: Set<string> = new Set();

  // Per-agent ID counters (each WorkflowState instance has its own counters)
  private operatorIdCounter: number = 0;
  private linkIdCounter: number = 0;

  // ============================================================================
  // RxJS Subjects for workflow change events (similar to frontend WorkflowGraph)
  // ============================================================================

  /** Emits when an operator is added */
  private readonly operatorAddSubject = new Subject<OperatorPredicate>();

  /** Emits when an operator is deleted */
  private readonly operatorDeleteSubject = new Subject<{ deletedOperatorID: string }>();

  /** Emits when an operator's properties are changed */
  private readonly operatorPropertyChangeSubject = new Subject<{ operator: OperatorPredicate }>();

  /** Emits when a link is added */
  private readonly linkAddSubject = new Subject<OperatorLink>();

  /** Emits when a link is deleted */
  private readonly linkDeleteSubject = new Subject<{ deletedLink: OperatorLink }>();

  /** Emits when disabled operators change */
  private readonly disabledOperatorChangedSubject = new Subject<{
    newDisabled: string[];
    newEnabled: string[];
  }>();

  /** Emits when view result operators change */
  private readonly viewResultOperatorChangedSubject = new Subject<{
    newViewResultOps: string[];
    newUnviewResultOps: string[];
  }>();

  // ============================================================================
  // Validation state (similar to frontend ValidationWorkflowService)
  // ============================================================================

  /** Validation errors by operator ID */
  private validationErrors: Record<string, ValidationError> = {};
  private workflowEmpty: boolean = true;

  /** Emits when validation state changes */
  private readonly validationChangedSubject = new Subject<ValidationOutput>();

  // Track subscriptions for cleanup
  private subscriptions: Subscription[] = [];

  /**
   * Gets a merged stream of all workflow topology/property changes.
   * This is useful for triggering compilation or persistence.
   * Similar to the frontend's merge pattern in WorkflowCompilingService.
   */
  getWorkflowChangedStream(): Observable<unknown> {
    return merge(
      this.operatorAddSubject,
      this.operatorDeleteSubject,
      this.operatorPropertyChangeSubject,
      this.linkAddSubject,
      this.linkDeleteSubject,
      this.disabledOperatorChangedSubject
    );
  }

  // ============================================================================
  // ID Generation (per-agent counters)
  // ============================================================================

  /**
   * Generate a unique operator ID for this workflow/agent.
   * Format: {operatorType}-operator-{counter}
   */
  generateOperatorId(operatorType: string): string {
    return `${operatorType}-operator-${++this.operatorIdCounter}`;
  }

  /**
   * Generate a unique link ID for this workflow/agent.
   * Format: link-{counter}
   */
  generateLinkId(): string {
    return `link-${++this.linkIdCounter}`;
  }

  // ============================================================================
  // Operator Operations
  // ============================================================================

  addOperator(operator: OperatorPredicate, position?: Point): void {
    this.operators.set(operator.operatorID, operator);
    // Set default position if not provided - stack operators vertically
    const defaultPosition: Point = position || {
      x: 100 + (this.operators.size - 1) * 200,
      y: 100 + (this.operators.size - 1) * 100,
    };
    this.operatorPositions.set(operator.operatorID, defaultPosition);
    this.operatorAddSubject.next(operator);
  }

  getOperator(operatorId: string): OperatorPredicate | undefined {
    return this.operators.get(operatorId);
  }

  getAllOperators(): OperatorPredicate[] {
    return Array.from(this.operators.values());
  }

  getAllEnabledOperators(): OperatorPredicate[] {
    return this.getAllOperators();
  }

  deleteOperator(operatorId: string): boolean {
    const operator = this.operators.get(operatorId);
    if (!operator) return false;

    // Also delete any links connected to this operator
    const linksToDelete = this.getLinksConnectedToOperator(operatorId);
    for (const link of linksToDelete) {
      this.links.delete(link.linkID);
      this.linkDeleteSubject.next({ deletedLink: link });
    }

    this.operatorsToViewResult.delete(operatorId);
    this.operatorPositions.delete(operatorId);
    const deleted = this.operators.delete(operatorId);

    if (deleted) {
      this.operatorDeleteSubject.next({ deletedOperatorID: operatorId });
    }

    return deleted;
  }

  updateOperatorProperties(operatorId: string, properties: Record<string, any>): boolean {
    const operator = this.operators.get(operatorId);
    if (!operator) return false;

    const updatedOperator: OperatorPredicate = {
      ...operator,
      operatorProperties: { ...operator.operatorProperties, ...properties },
    };
    this.operators.set(operatorId, updatedOperator);
    this.operatorPropertyChangeSubject.next({ operator: updatedOperator });
    return true;
  }

  updateOperatorDisplayName(operatorId: string, displayName: string): boolean {
    const operator = this.operators.get(operatorId);
    if (!operator) return false;

    const updatedOperator: OperatorPredicate = {
      ...operator,
      customDisplayName: displayName,
    };
    this.operators.set(operatorId, updatedOperator);
    this.operatorPropertyChangeSubject.next({ operator: updatedOperator });
    return true;
  }

  /**
   * Update the input ports of an operator (for dynamic input port operators like PythonTableUDF).
   * This creates the specified number of input ports with generic names (Input 0, Input 1, etc.).
   * Port naming is handled in Python code via the INPUT_PORTS class variable.
   * @param operatorId The operator ID to update
   * @param numInputPorts The desired number of input ports
   * @returns true if successful, false if operator not found
   */
  updateOperatorInputPorts(operatorId: string, numInputPorts: number): boolean {
    const operator = this.operators.get(operatorId);
    if (!operator) return false;

    // Create the new input ports array with generic names
    const newInputPorts: import("../types/workflow").PortDescription[] = [];
    for (let i = 0; i < numInputPorts; i++) {
      newInputPorts.push({
        portID: `input-${i}`,
        displayName: `Input ${i}`,
        allowMultiInputs: false,
        isDynamicPort: i > 0, // First port is not dynamic, subsequent ports are
      });
    }

    const updatedOperator: OperatorPredicate = {
      ...operator,
      inputPorts: newInputPorts,
    };
    this.operators.set(operatorId, updatedOperator);
    this.operatorPropertyChangeSubject.next({ operator: updatedOperator });
    return true;
  }

  // ============================================================================
  // Position Operations
  // ============================================================================

  /**
   * Update the position of an operator on the canvas.
   * @param operatorId The operator ID to update
   * @param position The new position (x, y coordinates)
   * @returns true if successful, false if operator not found
   */
  updateOperatorPosition(operatorId: string, position: Point): boolean {
    if (!this.operators.has(operatorId)) {
      return false;
    }
    this.operatorPositions.set(operatorId, position);
    return true;
  }

  /**
   * Get the position of an operator.
   * @param operatorId The operator ID
   * @returns The position or undefined if not found
   */
  getOperatorPosition(operatorId: string): Point | undefined {
    return this.operatorPositions.get(operatorId);
  }

  // ============================================================================
  // Link Operations
  // ============================================================================

  addLink(link: OperatorLink): void {
    this.links.set(link.linkID, link);
    this.linkAddSubject.next(link);
  }

  getLink(linkId: string): OperatorLink | undefined {
    return this.links.get(linkId);
  }

  getAllLinks(): OperatorLink[] {
    return Array.from(this.links.values());
  }

  deleteLink(linkId: string): boolean {
    const link = this.links.get(linkId);
    if (!link) return false;

    const deleted = this.links.delete(linkId);
    if (deleted) {
      this.linkDeleteSubject.next({ deletedLink: link });
    }
    return deleted;
  }

  getLinksConnectedToOperator(operatorId: string): OperatorLink[] {
    return this.getAllLinks().filter(
      link => link.source.operatorID === operatorId || link.target.operatorID === operatorId
    );
  }

  /**
   * Retrieves a subgraph (subDAG) from the workflow graph.
   * This performs a depth-first search (DFS) starting from the specified target operator
   * and traverses backwards through incoming links to construct the subDAG.
   *
   * @param targetOperatorId - The unique identifier of the operator from which to start the DFS.
   * @returns An object containing two arrays: `operators` and `links`.
   */
  getSubDAG(targetOperatorId: string): { operators: OperatorPredicate[]; links: OperatorLink[] } {
    const visited = new Set<string>();
    const subDagOperators: OperatorPredicate[] = [];
    const subDagLinks: OperatorLink[] = [];

    const dfs = (currentOperatorId: string) => {
      if (visited.has(currentOperatorId)) {
        return;
      }

      visited.add(currentOperatorId);

      const currentOperator = this.getOperator(currentOperatorId);
      if (currentOperator && !currentOperator.isDisabled) {
        subDagOperators.push(currentOperator);

        // Find links connected to the current operator as target (incoming links)
        const connectedLinks = this.getAllLinks().filter(
          link => link.target.operatorID === currentOperatorId && !this.getOperator(link.source.operatorID)?.isDisabled
        );

        connectedLinks.forEach(link => {
          subDagLinks.push(link);
          dfs(link.source.operatorID);
        });
      }
    };

    dfs(targetOperatorId);

    return { operators: subDagOperators, links: subDagLinks };
  }

  /**
   * Get frontier operators by BFS backward from leaf/sink operators.
   * Leaf operators are those with no outgoing links.
   * Returns operator IDs sorted topologically (predecessors first, leaves last).
   *
   * @param depth - Number of BFS levels backward from leaves (1 = leaves only)
   * @returns Array of operator IDs in topological order
   */
  getFrontierOperators(depth: number): string[] {
    const allOperators = this.getAllOperators();
    if (allOperators.length === 0) return [];

    // Build set of operators that are sources of links (have outgoing edges)
    const sourceOperatorIds = new Set<string>();
    for (const link of this.getAllLinks()) {
      sourceOperatorIds.add(link.source.operatorID);
    }

    // Find leaf operators (no outgoing links)
    const leaves = allOperators
      .filter(op => !sourceOperatorIds.has(op.operatorID))
      .map(op => op.operatorID);

    if (leaves.length === 0) {
      // All operators have outgoing links (cycle or all connected) - use all as leaves
      return allOperators.map(op => op.operatorID);
    }

    // BFS backward through incoming links for `depth` levels
    const frontier = new Set<string>(leaves);
    let currentLevel = new Set<string>(leaves);

    for (let d = 1; d < depth; d++) {
      const nextLevel = new Set<string>();
      for (const opId of currentLevel) {
        // Find incoming links to this operator
        for (const link of this.getAllLinks()) {
          if (link.target.operatorID === opId && !frontier.has(link.source.operatorID)) {
            nextLevel.add(link.source.operatorID);
            frontier.add(link.source.operatorID);
          }
        }
      }
      if (nextLevel.size === 0) break;
      currentLevel = nextLevel;
    }

    // Topological sort: predecessors first, leaves last
    // Build adjacency for frontier-only subgraph
    const frontierArray = Array.from(frontier);
    const inDegree = new Map<string, number>();
    const children = new Map<string, string[]>();
    for (const opId of frontierArray) {
      inDegree.set(opId, 0);
      children.set(opId, []);
    }
    for (const link of this.getAllLinks()) {
      if (frontier.has(link.source.operatorID) && frontier.has(link.target.operatorID)) {
        children.get(link.source.operatorID)!.push(link.target.operatorID);
        inDegree.set(link.target.operatorID, (inDegree.get(link.target.operatorID) ?? 0) + 1);
      }
    }

    // Kahn's algorithm
    const queue: string[] = frontierArray.filter(opId => (inDegree.get(opId) ?? 0) === 0);
    const sorted: string[] = [];
    while (queue.length > 0) {
      const node = queue.shift()!;
      sorted.push(node);
      for (const child of children.get(node) ?? []) {
        const newDeg = (inDegree.get(child) ?? 1) - 1;
        inDegree.set(child, newDeg);
        if (newDeg === 0) queue.push(child);
      }
    }

    // If there are nodes not in sorted (cycle), append them
    if (sorted.length < frontierArray.length) {
      for (const opId of frontierArray) {
        if (!sorted.includes(opId)) sorted.push(opId);
      }
    }

    return sorted;
  }

  /**
   * Compute the average source-to-sink path length (in nodes) in the workflow DAG using DP.
   * A single isolated operator is a path of length 1. A chain A→B→C→D has length 4.
   * Returns Math.ceil(average), minimum 1.
   *
   * Algorithm: topological-order DP tracking (pathCount, edgeSum) per node.
   * Sources get (1, 0). For each edge u→v: v.count += u.count, v.edgeSum += u.edgeSum + u.count.
   * Path length in nodes = edges + 1, so average = (totalEdgeSum + totalPaths) / totalPaths.
   * All components (connected and disconnected) are included.
   *
   * O(V+E), no path enumeration.
   */
  computeAveragePathLength(): number {
    const allOperators = this.getAllOperators();
    if (allOperators.length === 0) return 1;

    const allLinks = this.getAllLinks();
    if (allLinks.length === 0) return 1;

    // Build adjacency list and track in-degree for topo sort
    const outgoing = new Map<string, string[]>();
    const incomingCount = new Map<string, number>();
    for (const op of allOperators) {
      outgoing.set(op.operatorID, []);
      incomingCount.set(op.operatorID, 0);
    }
    for (const link of allLinks) {
      const src = link.source.operatorID;
      const tgt = link.target.operatorID;
      if (outgoing.has(src) && incomingCount.has(tgt)) {
        outgoing.get(src)!.push(tgt);
        incomingCount.set(tgt, (incomingCount.get(tgt) ?? 0) + 1);
      }
    }

    // Find sources (no incoming links)
    const sources: string[] = [];
    for (const [opId, count] of incomingCount) {
      if (count === 0) sources.push(opId);
    }
    if (sources.length === 0) return 1; // cycle, no sources

    // Find sinks (no outgoing links)
    const sinks = new Set<string>();
    for (const [opId, children] of outgoing) {
      if (children.length === 0) sinks.add(opId);
    }
    if (sinks.size === 0) return 1; // cycle, no sinks

    // Kahn's topo sort with DP
    const pathCount = new Map<string, number>();
    const pathSum = new Map<string, number>();
    for (const op of allOperators) {
      pathCount.set(op.operatorID, 0);
      pathSum.set(op.operatorID, 0);
    }
    for (const s of sources) {
      pathCount.set(s, 1);
      pathSum.set(s, 0);
    }

    const queue = [...sources];
    const tempIncoming = new Map(incomingCount);

    while (queue.length > 0) {
      const u = queue.shift()!;
      const uCount = pathCount.get(u)!;
      const uSum = pathSum.get(u)!;
      for (const v of outgoing.get(u) ?? []) {
        pathCount.set(v, (pathCount.get(v) ?? 0) + uCount);
        pathSum.set(v, (pathSum.get(v) ?? 0) + uSum + uCount);
        const newIn = (tempIncoming.get(v) ?? 1) - 1;
        tempIncoming.set(v, newIn);
        if (newIn === 0) queue.push(v);
      }
    }

    // Aggregate across all sinks (including isolated nodes as valid 1-node paths).
    // Path length in nodes = edges + 1, so we convert via (totalEdgeSum + totalPaths) / totalPaths.
    let totalCount = 0;
    let totalSum = 0;
    for (const s of sinks) {
      totalCount += pathCount.get(s) ?? 0;
      totalSum += pathSum.get(s) ?? 0;
    }

    if (totalCount === 0) return 1;
    // Average path length in nodes: (totalEdgeSum + totalPaths) / totalPaths = avg_edges + 1
    return Math.max(1, Math.ceil((totalSum + totalCount) / totalCount));
  }

  // ============================================================================
  // Validation State
  // ============================================================================

  /**
   * Get validation changed stream for subscribing to validation state changes.
   */
  getValidationChangedStream(): Observable<ValidationOutput> {
    return this.validationChangedSubject.asObservable();
  }

  /**
   * Get current validation output (errors and empty state).
   */
  getValidationOutput(): ValidationOutput {
    return {
      errors: { ...this.validationErrors },
      workflowEmpty: this.workflowEmpty,
    };
  }

  /**
   * Set validation error for an operator.
   */
  setValidationError(operatorId: string, error: ValidationError): void {
    this.validationErrors[operatorId] = error;
    this.emitValidationChanged();
  }

  /**
   * Clear validation error for an operator.
   */
  clearValidationError(operatorId: string): void {
    delete this.validationErrors[operatorId];
    this.emitValidationChanged();
  }

  /**
   * Set all validation errors at once (e.g., after full validation run).
   */
  setAllValidationErrors(errors: Record<string, ValidationError>): void {
    this.validationErrors = { ...errors };
    this.updateWorkflowEmptyState();
    this.emitValidationChanged();
  }

  /**
   * Check and update if workflow is empty.
   */
  private updateWorkflowEmptyState(): void {
    const operators = this.getAllOperators();
    this.workflowEmpty = operators.length === 0;

    // If there are operators, check if they're all disabled
    if (!this.workflowEmpty) {
      this.workflowEmpty = operators.every(op => op.isDisabled);
    }
  }

  /**
   * Emit validation changed event.
   */
  private emitValidationChanged(): void {
    this.validationChangedSubject.next({
      errors: { ...this.validationErrors },
      workflowEmpty: this.workflowEmpty,
    });
  }

  // ============================================================================
  // Workflow Content (Serialization)
  // ============================================================================

  getWorkflowContent(): WorkflowContent {
    // Convert operatorPositions Map to object format expected by frontend
    const positionsObj: { [key: string]: Point } = {};
    for (const [id, pos] of this.operatorPositions) {
      positionsObj[id] = pos;
    }

    return {
      operators: this.getAllOperators(),
      operatorPositions: positionsObj,
      links: this.getAllLinks(),
      commentBoxes: [...this.commentBoxes],
      settings: { ...this.settings },
    };
  }

  setWorkflowContent(content: WorkflowContent): void {
    // Clear existing state without emitting events
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();

    // Add new content (no events emitted for bulk load)
    for (const op of content.operators) {
      this.operators.set(op.operatorID, op);
    }
    for (const link of content.links) {
      this.links.set(link.linkID, link);
    }

    // Load operator positions
    if (content.operatorPositions) {
      for (const [id, pos] of Object.entries(content.operatorPositions)) {
        this.operatorPositions.set(id, pos);
      }
    }

    // Load comment boxes
    this.commentBoxes = content.commentBoxes ? [...content.commentBoxes] : [];

    // Load settings
    this.settings = content.settings ? { ...content.settings } : { ...DEFAULT_WORKFLOW_SETTINGS };
  }

  /**
   * Convert to backend LogicalPlan format
   */
  toLogicalPlan(targetOperatorId?: string): LogicalPlan {
    const enabledOperators = this.getAllEnabledOperators();

    // If targetOperatorId specified, get subgraph up to that operator
    // For now, simplified: just use all enabled operators
    const operators: LogicalOperator[] = enabledOperators.map(op => ({
      operatorID: op.operatorID,
      operatorType: op.operatorType,
      ...op.operatorProperties,
      inputPorts: op.inputPorts,
      outputPorts: op.outputPorts,
    }));

    const operatorIds = new Set(operators.map(op => op.operatorID));

    const links: LogicalLink[] = this.getAllLinks()
      .filter(link => operatorIds.has(link.source.operatorID) && operatorIds.has(link.target.operatorID))
      .map(link => {
        const sourceOp = this.getOperator(link.source.operatorID)!;
        const targetOp = this.getOperator(link.target.operatorID)!;

        const fromPortIdx = sourceOp.outputPorts.findIndex(p => p.portID === link.source.portID);
        const toPortIdx = targetOp.inputPorts.findIndex(p => p.portID === link.target.portID);

        return {
          fromOpId: link.source.operatorID,
          fromPortId: { id: fromPortIdx >= 0 ? fromPortIdx : 0, internal: false },
          toOpId: link.target.operatorID,
          toPortId: { id: toPortIdx >= 0 ? toPortIdx : 0, internal: false },
        };
      });

    return {
      operators,
      links,
      opsToViewResult: Array.from(this.operatorsToViewResult).filter(id => operatorIds.has(id)),
      opsToReuseResult: [],
    };
  }

  // ============================================================================
  // Subscription Management
  // ============================================================================

  /**
   * Add a subscription to be tracked for cleanup.
   */
  addSubscription(subscription: Subscription): void {
    this.subscriptions.push(subscription);
  }

  // ============================================================================
  // Reset and Cleanup
  // ============================================================================

  reset(): void {
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();
    this.commentBoxes = [];
    this.settings = { ...DEFAULT_WORKFLOW_SETTINGS };
    this.operatorsToViewResult.clear();
    this.validationErrors = {};
    this.workflowEmpty = true;
  }

  /**
   * Cleanup all subscriptions and complete all subjects.
   * Call this when the WorkflowState is no longer needed.
   */
  destroy(): void {
    // Unsubscribe all tracked subscriptions
    for (const sub of this.subscriptions) {
      sub.unsubscribe();
    }
    this.subscriptions = [];

    // Complete all subjects
    this.operatorAddSubject.complete();
    this.operatorDeleteSubject.complete();
    this.operatorPropertyChangeSubject.complete();
    this.linkAddSubject.complete();
    this.linkDeleteSubject.complete();
    this.disabledOperatorChangedSubject.complete();
    this.viewResultOperatorChangedSubject.complete();
    this.validationChangedSubject.complete();

    // Clear all state
    this.reset();
  }
}

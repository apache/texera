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

import { assertWorkflowIDs, isMacro, toLogicalGraph } from "@texera/workflow-macros";
import { Subject, Observable, merge, Subscription } from "rxjs";
import type {
  OperatorPredicate,
  OperatorLink,
  WorkflowContent,
  LogicalPlan,
  Point,
  CommentBox,
  WorkflowSettings,
} from "../types/workflow";

const DEFAULT_WORKFLOW_SETTINGS: WorkflowSettings = {
  dataTransferBatchSize: 400,
};

/**
 * In-memory logical plan the agent edits across a conversation.
 *
 * Holds operators, links, positions, comment boxes, and workflow settings,
 * and emits change events via RxJS so subscribers (auto-persist, websocket
 * broadcast) can react. Converts to/from the backend `WorkflowContent` wire
 * format and to the `LogicalPlan` shape used for compilation and execution.
 */
export class WorkflowState {
  private operators: Map<string, OperatorPredicate> = new Map();
  private links: Map<string, OperatorLink> = new Map();
  private operatorPositions: Map<string, Point> = new Map();
  private commentBoxes: CommentBox[] = [];
  private settings: WorkflowSettings = { ...DEFAULT_WORKFLOW_SETTINGS };
  private contentMeta: Pick<WorkflowContent, "formBinding" | "macroInterface" | "macroDependencies"> = {};
  private operatorsToViewResult: Set<string> = new Set();

  private operatorIdCounter: number = 0;
  private linkIdCounter: number = 0;

  private readonly operatorAddSubject = new Subject<OperatorPredicate>();
  private readonly operatorDeleteSubject = new Subject<{ deletedOperatorID: string }>();
  private readonly operatorPropertyChangeSubject = new Subject<{ operator: OperatorPredicate }>();
  private readonly linkAddSubject = new Subject<OperatorLink>();
  private readonly linkDeleteSubject = new Subject<{ deletedLink: OperatorLink }>();
  private readonly disabledOperatorChangedSubject = new Subject<{
    newDisabled: string[];
    newEnabled: string[];
  }>();
  private readonly viewResultOperatorChangedSubject = new Subject<{
    newViewResultOps: string[];
    newUnviewResultOps: string[];
  }>();

  private subscriptions: Subscription[] = [];

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

  generateOperatorId(operatorType: string): string {
    let id: string;
    do {
      id = `${operatorType}-operator-${++this.operatorIdCounter}`;
    } while (this.hasGraphID(id));
    return id;
  }

  generateLinkId(): string {
    let id: string;
    do {
      id = `link-${++this.linkIdCounter}`;
    } while (this.hasGraphID(id));
    return id;
  }

  private hasGraphID(id: string): boolean {
    return this.operators.has(id) || this.links.has(id) || this.commentBoxes.some(box => box.commentBoxID === id);
  }

  addOperator(operator: OperatorPredicate, position?: Point): void {
    assertWorkflowIDs({ ...this.getWorkflowContent(), operators: [...this.getAllOperators(), operator] });
    this.operators.set(operator.operatorID, operator);
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

  updateOperatorPosition(operatorId: string, position: Point): boolean {
    if (!this.operators.has(operatorId)) {
      return false;
    }
    this.operatorPositions.set(operatorId, position);
    return true;
  }

  getOperatorPosition(operatorId: string): Point | undefined {
    return this.operatorPositions.get(operatorId);
  }

  addLink(link: OperatorLink): void {
    assertWorkflowIDs({ ...this.getWorkflowContent(), links: [...this.getAllLinks(), link] });
    this.links.set(link.linkID, link);
    this.linkAddSubject.next(link);
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

  getWorkflowContent(): WorkflowContent {
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
      ...this.contentMeta,
    };
  }

  setWorkflowContent(content: WorkflowContent): void {
    assertWorkflowIDs(content);
    const { operators, links, operatorPositions, commentBoxes, settings, ...contentMeta } = content;
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();

    for (const op of operators) {
      this.operators.set(op.operatorID, op);
    }
    for (const link of links) {
      this.links.set(link.linkID, link);
    }

    if (operatorPositions) {
      for (const [id, pos] of Object.entries(operatorPositions)) {
        this.operatorPositions.set(id, pos);
      }
    }

    this.commentBoxes = commentBoxes ? [...commentBoxes] : [];

    this.settings = settings ? { ...settings } : { ...DEFAULT_WORKFLOW_SETTINGS };

    this.contentMeta = contentMeta;
  }

  toLogicalPlan(targetOperatorId?: string): LogicalPlan {
    let graph = targetOperatorId
      ? this.getSubDAG(targetOperatorId)
      : { operators: this.getAllEnabledOperators(), links: this.getAllLinks() };
    const results = targetOperatorId !== undefined ? [targetOperatorId] : [...this.operatorsToViewResult];
    if (this.contentMeta.macroInterface || this.getAllOperators().some(isMacro))
      return {
        operators: [],
        links: [],
        macroContent: this.getWorkflowContent(),
        executeOperatorIds: graph.operators.map(op => op.operatorID),
        opsToViewResult: results,
        opsToReuseResult: [],
      };
    const ids = new Set(graph.operators.map(op => op.operatorID));
    const links = graph.links.filter(link => ids.has(link.source.operatorID) && ids.has(link.target.operatorID));
    return {
      ...toLogicalGraph({ ...graph, links }, 0),
      opsToViewResult: results.filter(id => ids.has(id)),
      opsToReuseResult: [],
    };
  }

  addSubscription(subscription: Subscription): void {
    this.subscriptions.push(subscription);
  }

  reset(): void {
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();
    this.commentBoxes = [];
    this.settings = { ...DEFAULT_WORKFLOW_SETTINGS };
    this.contentMeta = {};
    this.operatorsToViewResult.clear();
  }

  destroy(): void {
    for (const sub of this.subscriptions) {
      sub.unsubscribe();
    }
    this.subscriptions = [];

    this.operatorAddSubject.complete();
    this.operatorDeleteSubject.complete();
    this.operatorPropertyChangeSubject.complete();
    this.linkAddSubject.complete();
    this.linkDeleteSubject.complete();
    this.disabledOperatorChangedSubject.complete();
    this.viewResultOperatorChangedSubject.complete();

    this.reset();
  }
}

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
  // Opaque Form View definition carried through unchanged (the agent never touches it);
  // see WorkflowContent.formBinding.
  private formBinding: unknown = undefined;
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
    return `${operatorType}-operator-${++this.operatorIdCounter}`;
  }

  generateLinkId(): string {
    return `link-${++this.linkIdCounter}`;
  }

  addOperator(operator: OperatorPredicate, position?: Point): void {
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
      // Only re-emit the key when the loaded workflow carried one, so plain workflows
      // stay byte-identical (JSON.stringify would otherwise add "formBinding":null).
      ...(this.formBinding !== undefined ? { formBinding: this.formBinding } : {}),
    };
  }

  setWorkflowContent(content: WorkflowContent): void {
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();

    for (const op of content.operators) {
      this.operators.set(op.operatorID, op);
    }
    for (const link of content.links) {
      this.links.set(link.linkID, link);
    }

    if (content.operatorPositions) {
      for (const [id, pos] of Object.entries(content.operatorPositions)) {
        this.operatorPositions.set(id, pos);
      }
    }

    this.commentBoxes = content.commentBoxes ? [...content.commentBoxes] : [];

    this.settings = content.settings ? { ...content.settings } : { ...DEFAULT_WORKFLOW_SETTINGS };

    this.formBinding = content.formBinding;
  }

  toLogicalPlan(): LogicalPlan {
    const enabledOperators = this.getAllEnabledOperators();

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

  addSubscription(subscription: Subscription): void {
    this.subscriptions.push(subscription);
  }

  reset(): void {
    this.operators.clear();
    this.links.clear();
    this.operatorPositions.clear();
    this.commentBoxes = [];
    this.settings = { ...DEFAULT_WORKFLOW_SETTINGS };
    this.formBinding = undefined;
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

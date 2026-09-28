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

import { Injectable } from "@angular/core";
import { CommentBox, OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { InsertedWorkflowFragment, WorkflowFragment } from "../../types/workflow-fragment.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";

@Injectable({ providedIn: "root" })
export class WorkflowFragmentService {
  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  public captureSectionBox(sectionBoxID: string): WorkflowFragment {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasCommentBox(sectionBoxID) || !graph.getCommentBox(sectionBoxID).overbox) {
      throw new Error("Choose a section box to save.");
    }
    const sectionBox = graph.getCommentBox(sectionBoxID);
    const bounds = this.workflowActionService.getJointGraph().getCell(sectionBoxID).getBBox();
    const operatorIDs = graph
      .getAllOperators()
      .filter(operator => {
        const operatorBounds = this.workflowActionService.getJointGraph().getCell(operator.operatorID).getBBox();
        return (
          operatorBounds.x >= bounds.x &&
          operatorBounds.y >= bounds.y &&
          operatorBounds.x + operatorBounds.width <= bounds.x + bounds.width &&
          operatorBounds.y + operatorBounds.height <= bounds.y + bounds.height
        );
      })
      .map(operator => operator.operatorID);
    if (operatorIDs.length === 0) {
      throw new Error("The section box does not contain any operators.");
    }
    return {
      ...this.captureAtOrigin(operatorIDs, { x: bounds.x, y: bounds.y }),
      sectionBox: this.clone(sectionBox.overbox!),
    };
  }

  private captureAtOrigin(operatorIDs: readonly string[], origin: Point): WorkflowFragment {
    const graph = this.workflowActionService.getTexeraGraph();
    const selectedIDs = new Set(operatorIDs);
    const operators = operatorIDs.map(operatorID => this.clone(graph.getOperator(operatorID)));
    const operatorPositions: Record<string, Point> = {};

    operatorIDs.forEach(operatorID => {
      const position = this.workflowActionService.getJointGraphWrapper().getElementPosition(operatorID);
      operatorPositions[operatorID] = { x: position.x - origin.x, y: position.y - origin.y };
    });

    const links = graph
      .getAllLinks()
      .filter(link => selectedIDs.has(link.source.operatorID) && selectedIDs.has(link.target.operatorID))
      .map(link => this.clone(link));

    return { operators, operatorPositions, links };
  }

  public insert(fragment: WorkflowFragment, anchor: Point): InsertedWorkflowFragment {
    this.validate(fragment);
    const operatorIDMap = new Map<string, string>();
    const operatorsAndPositions = fragment.operators.map(operator => {
      const operatorID = `${operator.operatorType}-${this.workflowUtilService.getOperatorRandomUUID()}`;
      operatorIDMap.set(operator.operatorID, operatorID);
      const relativePosition = fragment.operatorPositions[operator.operatorID];
      const copiedOperator: OperatorPredicate = { ...this.clone(operator), operatorID };
      return {
        op: copiedOperator,
        pos: { x: anchor.x + relativePosition.x, y: anchor.y + relativePosition.y },
      };
    });

    const links: OperatorLink[] = fragment.links.map(link => ({
      ...this.clone(link),
      linkID: this.workflowUtilService.getLinkRandomUUID(),
      source: { ...link.source, operatorID: operatorIDMap.get(link.source.operatorID)! },
      target: { ...link.target, operatorID: operatorIDMap.get(link.target.operatorID)! },
    }));

    let sectionBox: CommentBox | undefined;
    if (fragment.sectionBox) {
      sectionBox = {
        commentBoxID: this.workflowUtilService.getCommentBoxRandomUUID(),
        comments: [],
        commentBoxPosition: { ...anchor },
        overbox: this.clone(fragment.sectionBox),
      };
    }
    this.workflowActionService.addOperatorsAndLinks(
      operatorsAndPositions,
      links,
      sectionBox ? [sectionBox] : undefined
    );
    const inserted = {
      operatorIDs: operatorsAndPositions.map(item => item.op.operatorID),
      linkIDs: links.map(link => link.linkID),
      sectionBoxID: sectionBox?.commentBoxID,
    };
    this.workflowActionService.highlightElements(
      true,
      ...inserted.operatorIDs,
      ...inserted.linkIDs,
      ...(inserted.sectionBoxID ? [inserted.sectionBoxID] : [])
    );
    return inserted;
  }

  private validate(fragment: WorkflowFragment): void {
    if (!fragment || !Array.isArray(fragment.operators) || fragment.operators.length === 0) {
      throw new Error("The reusable section does not contain any operators.");
    }
    if (!fragment.operatorPositions || !Array.isArray(fragment.links)) {
      throw new Error("The reusable section is malformed.");
    }

    const operatorIDs = new Set(fragment.operators.map(operator => operator.operatorID));
    if (operatorIDs.size !== fragment.operators.length) {
      throw new Error("The reusable section contains duplicate operator IDs.");
    }
    fragment.operators.forEach(operator => {
      if (!fragment.operatorPositions[operator.operatorID]) {
        throw new Error(`The reusable section is missing the position of ${operator.operatorID}.`);
      }
    });
    fragment.links.forEach(link => {
      if (!operatorIDs.has(link.source.operatorID) || !operatorIDs.has(link.target.operatorID)) {
        throw new Error("The reusable section contains a dangling link.");
      }
    });
    if (
      fragment.sectionBox &&
      (!fragment.sectionBox.name.trim() ||
        !Number.isFinite(fragment.sectionBox.width) ||
        fragment.sectionBox.width <= 0 ||
        !Number.isFinite(fragment.sectionBox.height) ||
        fragment.sectionBox.height <= 0 ||
        !/^#[0-9a-f]{6}$/i.test(fragment.sectionBox.color))
    ) {
      throw new Error("The reusable section contains an invalid section box.");
    }
  }

  private clone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
  }
}

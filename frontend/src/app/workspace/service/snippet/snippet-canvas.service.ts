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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import {
  SnippetLink,
  SnippetOperator,
  WorkflowSnippet,
} from "../../../dashboard/type/workflow-snippet.interface";

/**
 * SnippetCanvasService bridges canvas selection state with the snippet store.
 *
 * - `captureFromHighlightedSelection()` builds a snippet payload from the
 *   currently highlighted operators (positions normalized to the top-left).
 * - `placeSnippet()` instantiates a saved snippet on the canvas at a given
 *   drop point, regenerating fresh operator IDs and rewiring all internal
 *   links to those new IDs.
 */
@Injectable({
  providedIn: "root",
})
export class SnippetCanvasService {
  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private notificationService: NotificationService
  ) {}

  /**
   * Build a snippet payload from the currently highlighted operators.
   * Returns undefined when fewer than 2 operators are highlighted.
   */
  public captureFromHighlightedSelection():
    | {
        operators: SnippetOperator[];
        links: SnippetLink[];
      }
    | undefined {
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const highlightedIds = jointGraphWrapper.getCurrentHighlightedOperatorIDs();
    if (highlightedIds.length < 2) {
      return undefined;
    }
    const idSet = new Set(highlightedIds);

    const opsWithPositions = highlightedIds.map(id => ({
      op: texeraGraph.getOperator(id),
      pos: jointGraphWrapper.getElementPosition(id),
    }));

    const minX = Math.min(...opsWithPositions.map(o => o.pos.x));
    const minY = Math.min(...opsWithPositions.map(o => o.pos.y));

    const operators: SnippetOperator[] = opsWithPositions.map(({ op, pos }) => ({
      operatorId: op.operatorID,
      operatorType: op.operatorType,
      operatorVersion: op.operatorVersion,
      operatorProperties: { ...op.operatorProperties },
      customDisplayName: op.customDisplayName,
      showAdvanced: op.showAdvanced,
      position: { x: pos.x - minX, y: pos.y - minY },
    }));

    // Keep only links where both endpoints are inside the selection.
    const links: SnippetLink[] = texeraGraph
      .getAllLinks()
      .filter(
        link =>
          idSet.has(link.source.operatorID) && idSet.has(link.target.operatorID)
      )
      .map(link => ({
        fromOperatorId: link.source.operatorID,
        fromPortId: link.source.portID,
        toOperatorId: link.target.operatorID,
        toPortId: link.target.portID,
      }));

    return { operators, links };
  }

  /**
   * Place a snippet on the canvas at the given page-space drop point.
   * Returns true on success.
   */
  public placeSnippet(snippet: WorkflowSnippet, dropPoint: Point): boolean {
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    // eslint-disable-next-line no-console
    console.debug("[snippet] placeSnippet entry", {
      name: snippet.name,
      dropPoint,
      paperReady: !!paper,
      operatorCount: snippet.operators.length,
    });
    if (!paper) {
      this.notificationService.error("Canvas isn't ready yet — try again in a moment.");
      return false;
    }
    const origin = paper.pageToLocalPoint(dropPoint.x, dropPoint.y);
    // eslint-disable-next-line no-console
    console.debug("[snippet] computed local origin", origin);
    return this.placeSnippetAtLocal(snippet, origin);
  }

  /**
   * Place a snippet using local paper coordinates as the origin. Used by
   * programmatic placement (Quick Steps) where we don't have a real drop event.
   */
  public placeSnippetAtLocal(snippet: WorkflowSnippet, originLocal: Point): boolean {
    const origin = originLocal;

    // Generate fresh operator IDs for each snippet operator while preserving
    // their structure (type, properties, display name, ports).
    const idMap = new Map<string, string>();
    const operatorsAndPositions: { op: OperatorPredicate; pos: Point }[] = [];
    const missingTypes: string[] = [];

    for (const snippetOp of snippet.operators) {
      let predicate: OperatorPredicate;
      try {
        predicate = this.workflowUtilService.getNewOperatorPredicate(snippetOp.operatorType);
      } catch (e) {
        // eslint-disable-next-line no-console
        console.warn("[snippet] unknown operatorType", snippetOp.operatorType, e);
        missingTypes.push(snippetOp.operatorType);
        continue;
      }
      // Layer schema defaults under the snippet's saved properties so a
      // freshly-placed operator has valid configuration when fields weren't
      // captured (seed snippets have empty properties; user-saved snippets
      // carry the user's chosen values, which override defaults).
      const merged: OperatorPredicate = {
        ...predicate,
        operatorProperties: {
          ...predicate.operatorProperties,
          ...snippetOp.operatorProperties,
        },
        customDisplayName: snippetOp.customDisplayName ?? predicate.customDisplayName,
        showAdvanced: snippetOp.showAdvanced ?? predicate.showAdvanced,
      };
      idMap.set(snippetOp.operatorId, merged.operatorID);
      operatorsAndPositions.push({
        op: merged,
        pos: {
          x: origin.x + snippetOp.position.x,
          y: origin.y + snippetOp.position.y,
        },
      });
    }

    if (operatorsAndPositions.length === 0) {
      this.notificationService.error(
        "This snippet contains no operators known to this Texera deployment."
      );
      return false;
    }

    const links: OperatorLink[] = [];
    for (const snippetLink of snippet.links) {
      const fromOp = idMap.get(snippetLink.fromOperatorId);
      const toOp = idMap.get(snippetLink.toOperatorId);
      if (!fromOp || !toOp) continue;
      links.push({
        linkID: this.workflowUtilService.getLinkRandomUUID(),
        source: { operatorID: fromOp, portID: snippetLink.fromPortId },
        target: { operatorID: toOp, portID: snippetLink.toPortId },
      });
    }

    this.workflowActionService.getTexeraGraph().bundleActions(() => {
      this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, links);
    });

    if (missingTypes.length > 0) {
      this.notificationService.info(
        `Snippet placed with ${operatorsAndPositions.length}/${snippet.operators.length} operators. ` +
          `Missing operator types: ${[...new Set(missingTypes)].join(", ")}`
      );
    } else {
      this.notificationService.success(`Snippet "${snippet.name}" added to canvas.`);
    }
    return true;
  }
}

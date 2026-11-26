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
import { BehaviorSubject, Observable } from "rxjs";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../../workspace/service/undo-redo/undo-redo.service";
import { isEqual } from "lodash";
import { OperatorPredicate } from "../../../../workspace/types/workflow-common.interface";
import { WorkflowVersionEntry } from "../../../type/workflow-version-entry";
import { AppSettings } from "../../../../common/app-setting";
import { filter, map } from "rxjs/operators";
import { WorkflowUtilService } from "../../../../workspace/service/workflow-graph/util/workflow-util.service";

import { HttpClient } from "@angular/common/http";

const WORKFLOW_VERSIONS_API_BASE_URL = "version";

type DifferentOpIDsList = {
  [key in "modified" | "added" | "deleted"]: string[];
};

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private modificationEnabledBeforeTempWorkflow: boolean | undefined;
  public operatorPropertyDiff: { [key: string]: Map<String, String> } = {};
  private displayParticularWorkflowVersion = new BehaviorSubject<boolean>(false);
  private differentOpIDsList: DifferentOpIDsList = { modified: [], added: [], deleted: [] };
  public selectedVersionId = new BehaviorSubject<number | null>(null);
  public selectedDisplayedVersionId = new BehaviorSubject<number | null>(null);

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private undoRedoService: UndoRedoService,
    private http: HttpClient
  ) {}

  public setDisplayParticularVersion(flag: boolean, versionId?: number, displayedVersionId?: number): void {
    if (flag) {
      if (versionId != undefined) {
        this.selectedVersionId.next(versionId);
      }
      if (displayedVersionId !== undefined) {
        this.selectedDisplayedVersionId.next(displayedVersionId);
      }
    } else {
      this.selectedVersionId.next(null);
      this.selectedDisplayedVersionId.next(null);
    }

    this.displayParticularWorkflowVersion.next(flag);
  }

  public getDisplayParticularVersionStream(): Observable<boolean> {
    return this.displayParticularWorkflowVersion.asObservable();
  }

  public get canRestoreVersion(): boolean {
    return this.modificationEnabledBeforeTempWorkflow !== undefined && this.modificationEnabledBeforeTempWorkflow;
  }

  public displayReadonlyWorkflow(workflow: Workflow) {
    this.saveModificationState();
    // we need to display the version on the paper but keep the original workflow in the background
    this.workflowActionService.setTempWorkflow(this.workflowActionService.getWorkflow());
    // disable persist to DB because it is read only
    this.workflowPersistService.setWorkflowPersistFlag(false);
    // disable the undoredo service because reloading the workflow is considered an action
    this.undoRedoService.disableWorkFlowModification();
    // reload the read only workflow version on the paper
    this.workflowActionService.reloadWorkflow(workflow);
    // disable modifications because it is read only
    this.workflowActionService.disableWorkflowModification();
  }

  public displayParticularVersion(workflow: Workflow, vid: number, displayedVId: number) {
    // get the list of IDs of different elements when comparing displaying to the editing version
    this.differentOpIDsList = this.getWorkflowsDifference(
      this.workflowActionService.getWorkflowContent(),
      workflow.content
    );
    this.displayReadonlyWorkflow(workflow);
    this.setDisplayParticularVersion(true, vid, displayedVId);
    // highlight the different elements by changing the color of boundary of the operator
    // needs a list of ids of elements to be highlighted
    this.highlightOpVersionDiff(this.differentOpIDsList);
  }

  public highlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    differentOpIDsList.modified.map(id => this.highlightOpBoundary(id, "255,118,20,0.5"));
    differentOpIDsList.added.map(id => this.highlightOpBoundary(id, "0,255,0,0.5"));

    this.updateDeletedLinkBrackets(differentOpIDsList, "255,0,0,0.5");
  }

  private updateDeletedLinkBrackets(differentOpIDsList: DifferentOpIDsList, color: string) {
    if (differentOpIDsList.deleted.length > 0) {
      const tempWorkflow = this.workflowActionService.getTempWorkflow();
      if (tempWorkflow != undefined) {
        for (const link of tempWorkflow.content.links) {
          if (differentOpIDsList.deleted.includes(link.source.operatorID) && link.target.operatorID != undefined) {
            this.highlightOpBracket(link.target.operatorID, color, "left-");
          }
          if (differentOpIDsList.deleted.includes(link.target.operatorID) && link.source.operatorID != undefined) {
            this.highlightOpBracket(link.source.operatorID, color, "right-");
          }
        }
      }
    }
  }

  public highlightOpBoundary(id: string, color: string) {
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaper()
      ?.getModelById(id)
      .attr("rect.boundary/fill", "rgba(" + color + ")");
  }

  public highlightOpBracket(id: string, color: string, position: string) {
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaper()
      ?.getModelById(id)
      .attr("path." + position + "boundary/stroke", "rgba(" + color + ")");
  }

  public getWorkflowsDifference(workflowContent1: WorkflowContent, workflowContent2: WorkflowContent) {
    this.operatorPropertyDiff = {};
    const difference: DifferentOpIDsList = { added: [], modified: [], deleted: [] };
    if (isEqual(workflowContent1.operators, workflowContent2.operators)) {
      return difference;
    }

    const operatorsById = (workflow: WorkflowContent): Record<string, OperatorPredicate> => {
      const operators: Record<string, OperatorPredicate> = {};
      for (const operator of workflow.operators) {
        operators[operator.operatorID] = operator;
      }
      return operators;
    };

    const firstById = operatorsById(workflowContent1);
    const secondById = operatorsById(workflowContent2);
    for (const id of Object.keys(secondById)) {
      if (!Object.hasOwn(firstById, id)) {
        difference.added.push(id);
      } else if (!isEqual(firstById[id], secondById[id])) {
        difference.modified.push(id);
        this.operatorPropertyDiff[id] = this.getOperatorsDifference(firstById[id], secondById[id]);
      }
    }

    for (const id of Object.keys(firstById)) {
      if (!Object.hasOwn(secondById, id)) {
        difference.deleted.push(id);
      }
    }
    return difference;
  }

  public getOperatorsDifference(operator1: OperatorPredicate, operator2: OperatorPredicate) {
    const difference: Map<String, String> = new Map();
    const highlight = "outline: 3px solid rgb(255, 118, 20); transition: 0.3s ease-in-out outline;";
    const properties = new Set([
      ...Object.keys(operator1.operatorProperties),
      ...Object.keys(operator2.operatorProperties),
    ]);
    for (const property of properties) {
      if (!isEqual(operator1.operatorProperties[property], operator2.operatorProperties[property])) {
        difference.set(property, highlight);
      }
    }
    if (operator1.operatorVersion != operator2.operatorVersion) {
      difference.set("operatorVersion", highlight);
    }
    return difference;
  }

  public revertToVersion() {
    // set all elements to transparent boundary
    this.unhighlightOpVersionDiff(this.differentOpIDsList);
    // we need to clear the undo and redo stack because it is a new version from a previous workflow on paper
    this.undoRedoService.clearRedoStack();
    this.undoRedoService.clearUndoStack();
    // we need to enable workflow modifications which also automatically enables undoredo service
    this.workflowActionService.enableWorkflowModification();
    // clear the temp workflow
    this.workflowActionService.resetTempWorkflow();
    this.workflowPersistService.setWorkflowPersistFlag(true);
    this.setDisplayParticularVersion(false);
    this.restoreModificationState();
  }

  public closeReadonlyWorkflowDisplay() {
    // should enable modifications first to be able to make action of reloading old version on paper
    this.workflowActionService.enableWorkflowModification();
    // but still disable redo and undo service to not capture swapping the workflows, because enabling modifications
    // automatically enables undo and redo
    this.undoRedoService.disableWorkFlowModification();
    // reload the old workflow don't persist anything
    this.workflowActionService.reloadWorkflow(this.workflowActionService.getTempWorkflow());
    // clear the temp workflow
    this.workflowActionService.resetTempWorkflow();
    // after reloading the workflow, we can enable the undoredo service
    this.undoRedoService.enableWorkFlowModification();
    this.workflowPersistService.setWorkflowPersistFlag(true);
    this.restoreModificationState();
  }

  public closeParticularVersionDisplay() {
    // set all elements to transparent boundary
    this.unhighlightOpVersionDiff(this.differentOpIDsList);
    this.closeReadonlyWorkflowDisplay();
    this.setDisplayParticularVersion(false);
  }

  /**
   * Drops a version preview still open when the workspace closes, so the next workflow opened does
   * not inherit it. The graph is left alone: the workflow is closing, and reloading it here would
   * reach the co-editing room only half done.
   */
  public discardOpenVersionDisplay(): void {
    if (!this.displayParticularWorkflowVersion.getValue()) {
      return;
    }
    this.workflowActionService.resetTempWorkflow();
    this.workflowPersistService.setWorkflowPersistFlag(true);
    this.workflowActionService.enableWorkflowModification();
    this.modificationEnabledBeforeTempWorkflow = undefined;
    this.setDisplayParticularVersion(false);
  }

  public unhighlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    for (const id of differentOpIDsList.added.concat(differentOpIDsList.modified)) {
      this.highlightOpBoundary(id, "0,0,0,0");
    }

    this.updateDeletedLinkBrackets(differentOpIDsList, "0,0,0,0");
    this.operatorPropertyDiff = {};
  }

  /**
   * retrieves a list of versions for a particular workflow from backend database
   */
  retrieveVersionsOfWorkflow(wid: number): Observable<WorkflowVersionEntry[]> {
    return this.http.get<WorkflowVersionEntry[]>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSIONS_API_BASE_URL}/${wid}`
    );
  }

  /**
   * retrieves a version of the workflow from backend database
   */
  retrieveWorkflowByVersion(wid: number, vid: number): Observable<Workflow> {
    return this.http
      .get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSIONS_API_BASE_URL}/${wid}/${vid}`)
      .pipe(
        filter((updatedWorkflow: Workflow) => updatedWorkflow != null),
        map(WorkflowUtilService.parseWorkflowInfo)
      );
  }

  private saveModificationState(): void {
    if (this.modificationEnabledBeforeTempWorkflow === undefined) {
      this.modificationEnabledBeforeTempWorkflow = this.workflowActionService.checkWorkflowModificationEnabled();
    }
  }

  private restoreModificationState(): void {
    if (this.modificationEnabledBeforeTempWorkflow !== undefined) {
      if (!this.modificationEnabledBeforeTempWorkflow) {
        this.workflowActionService.disableWorkflowModification();
      }
      this.modificationEnabledBeforeTempWorkflow = undefined;
    }
  }

  public cloneWorkflowVersion(): Observable<number> {
    const vid = this.selectedVersionId.getValue();
    const displayedVersionId = this.selectedDisplayedVersionId.getValue();

    return this.http.post<number>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_VERSIONS_API_BASE_URL}/clone/${vid}`, {
      displayedVersionId,
    });
  }
}

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../../workspace/service/undo-redo/undo-redo.service";
import { isEqual } from "lodash";
import { OperatorLink, OperatorPredicate, Point } from "../../../../workspace/types/workflow-common.interface";
import { WorkflowVersionEntry } from "../../../type/workflow-version-entry";
import { AppSettings } from "../../../../common/app-setting";
import { filter, map } from "rxjs/operators";
import { WorkflowUtilService } from "../../../../workspace/service/workflow-graph/util/workflow-util.service";

import { HttpClient } from "@angular/common/http";

export const WORKFLOW_VERSIONS_API_BASE_URL = "version";

type WorkflowContentKeys = keyof WorkflowContent;
type Element = OperatorLink | OperatorPredicate | Point;
type DifferentOpIDsList = {
  [key in "modified" | "added" | "deleted"]: string[];
};

// only element types specified in this list are consider when calculating the difference between workflows
const ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC: Partial<WorkflowContentKeys>[] = ["operators"];
// it maps a name of the element type to its ID field name used in WorkflowContent
const ID_FILED_FOR_ELEMENTS_CONFIG: { [key: string]: string } = {
  operators: "operatorID",
  commentBoxes: "commentBoxID",
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

  public displayWorkflowVersions(): void {
    // unhighlight all the current highlighted operators and links
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);
  }
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

    if (differentOpIDsList.deleted.length > 0) {
      const tempWorkflow = this.workflowActionService.getTempWorkflow();
      if (tempWorkflow != undefined) {
        for (const link of tempWorkflow.content.links) {
          if (differentOpIDsList.deleted.includes(link.source.operatorID) && link.target.operatorID != undefined) {
            this.highlightOpBracket(link.target.operatorID, "255,0,0,0.5", "left-");
          }
          if (differentOpIDsList.deleted.includes(link.target.operatorID) && link.source.operatorID != undefined) {
            this.highlightOpBracket(link.source.operatorID, "255,0,0,0.5", "right-");
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

  // TODO: the logic of the function will be refined later
  public getWorkflowsDifference(workflowContent1: WorkflowContent, workflowContent2: WorkflowContent) {
    let eleType;
    this.operatorPropertyDiff = {};
    const difference: DifferentOpIDsList = { added: [], modified: [], deleted: [] };
    // get a list of element types that are changed between versions
    const eleTypeWithDiffList: WorkflowContentKeys[] = [];
    for (eleType of ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC) {
      if (!isEqual(workflowContent1[eleType], workflowContent2[eleType])) {
        eleTypeWithDiffList.push(eleType);
      }
    }

    for (eleType of eleTypeWithDiffList) {
      if (ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC.includes(eleType)) {
        let eleID;
        // return an object with key: ID of the element, value: detailed content of the element
        let getEleIDMap = function (workflowContent: WorkflowContent, eleType: WorkflowContentKeys) {
          const elements = workflowContent[eleType] as Element[];
          const eleIDtoContentMap: { [key: string]: Element } = {};
          for (const element of elements) {
            eleIDtoContentMap[element[ID_FILED_FOR_ELEMENTS_CONFIG[eleType] as keyof Element]] = element;
          }
          return eleIDtoContentMap;
        };

        const eleIDtoContentMap1 = getEleIDMap(workflowContent1, eleType);
        const eleIDtoContentMap2 = getEleIDMap(workflowContent2, eleType);

        for (eleID of Object.keys(eleIDtoContentMap2)) {
          if (!Object.keys(eleIDtoContentMap1).includes(eleID)) {
            // there is an addition if the element ID exist in historical but not current workflow version
            difference.added.push(eleID);
          } else {
            // there might be a modification if the element ID exist in both historical and current workflow versions
            if (!isEqual(eleIDtoContentMap1[eleID], eleIDtoContentMap2[eleID])) {
              // if the contents in two workflow versions are different for the same element ID
              difference.modified.push(eleID);
              if (eleType == "operators") {
                this.operatorPropertyDiff[eleID] = this.getOperatorsDifference(
                  eleIDtoContentMap1[eleID] as OperatorPredicate,
                  eleIDtoContentMap2[eleID] as OperatorPredicate
                );
              }
            }
          }
        }

        for (eleID of Object.keys(eleIDtoContentMap1)) {
          if (!Object.keys(eleIDtoContentMap2).includes(eleID)) {
            // there is a deletion if the element ID exist in current but not historical workflow version
            difference.deleted.push(eleID);
          }
        }
      }
    }
    return difference;
  }

  public getOperatorsDifference(operator1: OperatorPredicate, operator2: OperatorPredicate) {
    const difference: Map<String, String> = new Map();
    for (const property of Object.keys(operator1.operatorProperties)) {
      if (operator1.operatorProperties[property] != operator2.operatorProperties[property]) {
        difference.set(property, "outline: 3px solid rgb(255, 118, 20); transition: 0.3s ease-in-out outline;");
      }
    }
    if (operator1.operatorVersion != operator2.operatorVersion) {
      difference.set("operatorVersion", "outline: 3px solid rgb(255, 118, 20); transition: 0.3s ease-in-out outline;");
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

  public unhighlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    for (const id of differentOpIDsList.added.concat(differentOpIDsList.modified)) {
      this.highlightOpBoundary(id, "0,0,0,0");
    }
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

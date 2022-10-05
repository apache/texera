import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../workspace/service/undo-redo/undo-redo.service";
import { isEqual } from "lodash";
import { Breakpoint, OperatorLink, OperatorPredicate, Point } from "src/app/workspace/types/workflow-common.interface";

export const DISPLAY_WORKFLOW_VERIONS_EVENT = "display_workflow_versions_event";

type WorkflowContentKeys = keyof WorkflowContent;
type Element = Breakpoint | OperatorLink | OperatorPredicate | Point;
type DifferentOpIDsList = {
  [key in "modified" | "added" | "deleted"]: string[];
};

// only element types specified in this list are consider when calculating the difference between workflows
const ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC: Partial<WorkflowContentKeys>[] = ["operators"];
// it maps name of the element type to its ID field name used in WorkflowContent
const ID_FILED_FOR_ELEMENTS_CONFIG: { [key: string]: string } = {
  operators: "operatorID",
  commentBoxes: "commentBoxID",
};

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private workflowVersionsObservable = new Subject<readonly string[]>();
  private displayParticularWorkflowVersion = new BehaviorSubject<boolean>(false);
  private differentOpIDsList: DifferentOpIDsList = { modified: [], added: [], deleted: [] };
  public operatorPropertyDiff: { [key: string]: Map<String, String> } = {};
  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private undoRedoService: UndoRedoService
  ) {}

  public clickDisplayWorkflowVersions(): void {
    // unhighlight all the current highlighted operators/groups/links
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);

    // emit event for display workflow versions event
    this.workflowVersionsObservable.next([DISPLAY_WORKFLOW_VERIONS_EVENT]);
  }

  public workflowVersionsDisplayObservable(): Observable<readonly string[]> {
    return this.workflowVersionsObservable.asObservable();
  }

  public setDisplayParticularVersion(flag: boolean): void {
    this.displayParticularWorkflowVersion.next(flag);
  }

  public getDisplayParticularVersionStream(): Observable<boolean> {
    return this.displayParticularWorkflowVersion.asObservable();
  }

  public displayParticularVersion(workflow: Workflow) {
    // we need to display the version on the paper but keep the original workflow in the background
    this.workflowActionService.setTempWorkflow(this.workflowActionService.getWorkflow());
    // get the list of IDs of different elements when comparing displaying to the editing version
    this.differentOpIDsList = this.getWorkflowsDifference(
      this.workflowActionService.getWorkflowContent(),
      workflow.content
    );
    // disable persist to DB because it is read only
    this.workflowPersistService.setWorkflowPersistFlag(false);
    // disable the undoredo service because reloading the workflow is considered an action
    this.undoRedoService.disableWorkFlowModification();
    // reload the read only workflow version on the paper
    this.workflowActionService.reloadWorkflow(workflow);
    this.setDisplayParticularVersion(true);
    // disable modifications because it is read only
    this.workflowActionService.disableWorkflowModification();
    // highlight the different elements by changing the color of boudary of the operator
    // needs a list of ids of elements to be highlighted
    this.highlightOpVersionDiff(this.differentOpIDsList);
  }

  public highlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    for (var id of differentOpIDsList.modified) {
      this.highlighOpBoundary(id, "255,118,20,0.5");
    }
    for (var id of differentOpIDsList.added) {
      this.highlighOpBoundary(id, "0,255,0,0.5");
    }
    if (differentOpIDsList.deleted != []) {
      var tempWorkflow = this.workflowActionService.getTempWorkflow();
      if (tempWorkflow != undefined) {
        for (var link of tempWorkflow.content.links) {
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

  public highlighOpBoundary(id: string, color: string) {
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaper()
      ?.getModelById(id)
      .attr("rect.boundary/fill", "rgba(" + color + ")");
  }

  public highlightOpBracket(id: string, color: string, position: string) {
    console.log(id);
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaper()
      ?.getModelById(id)
      .attr("path." + position + "boundary/stroke", "rgba(" + color + ")");
  }

  // TODO: the logic of the function will be refined later
  public getWorkflowsDifference(workflowContent1: WorkflowContent, workflowContent2: WorkflowContent) {
    this.operatorPropertyDiff = {};
    var difference: DifferentOpIDsList = { added: [], modified: [], deleted: [] };
    // get a list of element types that are changed between versions
    var eleTypeWithDiffList: WorkflowContentKeys[] = [];
    for (var eleType of ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC) {
      if (!isEqual(workflowContent1[eleType], workflowContent2[eleType])) {
        eleTypeWithDiffList.push(eleType);
      }
    }

    for (var eleType of eleTypeWithDiffList) {
      if (ELEMENT_TYPES_IN_WORKFLOW_DIFF_CALC.includes(eleType)) {
        // return an object with key: ID of the element, value: detailed content of the element
        let geteleIDMap = function (workflowContent: WorkflowContent, eleType: WorkflowContentKeys) {
          var elements = workflowContent[eleType] as Element[];
          var eleIDtoContentMap: { [key: string]: Element } = {};
          for (var element of elements) {
            eleIDtoContentMap[element[ID_FILED_FOR_ELEMENTS_CONFIG[eleType] as keyof Element]] = element;
          }
          return eleIDtoContentMap;
        };

        var eleIDtoContentMap1 = geteleIDMap(workflowContent1, eleType);
        var eleIDtoContentMap2 = geteleIDMap(workflowContent2, eleType);

        for (var eleID of Object.keys(eleIDtoContentMap2)) {
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

        for (var eleID of Object.keys(eleIDtoContentMap1)) {
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
    var difference: Map<String, String> = new Map();
    for (var property of Object.keys(operator1.operatorProperties)) {
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
    // set all elements to tranparent boudary
    this.unhighlightOpVersionDiff(this.differentOpIDsList);
    // we need to clear the undo and redo stack because it is a new version from previous workflow on paper
    this.undoRedoService.clearRedoStack();
    this.undoRedoService.clearUndoStack();
    // we need to enable workflow modifications which also automatically enables undoredo service
    this.workflowActionService.enableWorkflowModification();
    // clear the temp workflow
    this.workflowActionService.resetTempWorkflow();
    this.workflowPersistService.setWorkflowPersistFlag(true);
    this.setDisplayParticularVersion(false);
  }

  public closeParticularVersionDisplay() {
    // set all elements to tranparent boudary
    this.unhighlightOpVersionDiff(this.differentOpIDsList);
    // should enable modifications first to be able to make action of reloading old version on paper
    this.workflowActionService.enableWorkflowModification();
    // but still disable redo and undo service to not capture swapping the workflows, because enabling modifictions automatically enables undo and redo
    this.undoRedoService.disableWorkFlowModification();
    // reload the old workflow don't persist anything
    this.workflowActionService.reloadWorkflow(this.workflowActionService.getTempWorkflow());
    // clear the temp workflow
    this.workflowActionService.resetTempWorkflow();
    // after reloading the workflow, we can enable the undoredo service
    this.undoRedoService.enableWorkFlowModification();
    this.workflowPersistService.setWorkflowPersistFlag(true);
    this.setDisplayParticularVersion(false);
  }

  public unhighlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    for (var id of differentOpIDsList.added.concat(differentOpIDsList.modified)) {
      this.highlighOpBoundary(id, "0,0,0,0");
    }
    this.operatorPropertyDiff = {};
  }
}

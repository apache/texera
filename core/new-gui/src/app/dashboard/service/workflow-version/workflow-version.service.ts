import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../workspace/service/undo-redo/undo-redo.service";
import { isEqual, reduce } from "lodash";
import { Breakpoint, OperatorLink, OperatorPredicate, Point } from "src/app/workspace/types/workflow-common.interface";

export const DISPLAY_WORKFLOW_VERIONS_EVENT = "display_workflow_versions_event";
const TYPES_FOR_WORKFLOW_DIFF_CALC = ["operators"];

type WorkflowContents = keyof WorkflowContent;
type Elements = Breakpoint | OperatorLink | OperatorPredicate | Point;
type DifferentOpIDsList = {
  [key in "modified" | "added"]: string[];
};

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private workflowVersionsObservable = new Subject<readonly string[]>();
  private displayParticularWorkflowVersion = new BehaviorSubject<boolean>(false);
  private differentOpIDsList: DifferentOpIDsList = { modified: [], added: [] };
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
    this.differentOpIDsList = this.getDifference(this.workflowActionService.getWorkflow(), workflow);
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

  public getPositionDifference(v1: Workflow, v2: Workflow) {
    var p1 = v1.content.operatorPositions;
    var p2 = v2.content.operatorPositions;
    var difference = reduce(
      p2,
      function (result: string[], value, key) {
        return p1[key as keyof typeof p1] == undefined || isEqual(value, p1[key as keyof typeof p1])
          ? result
          : result.concat(key);
      },
      []
    );
    return difference;
  }

  public highlightOpVersionDiff(differentOpIDsList: DifferentOpIDsList) {
    for (var id of differentOpIDsList.modified) {
      console.log(id)
      this.highlighOpBoundary(id, "76,46,255,0.5");
    }
    for (var id of differentOpIDsList.added) {
      console.log(id)
      this.highlighOpBoundary(id, "255,118,20,0.5");
    }
  }

  public highlighOpBoundary(id: string, color: string) {
    this.workflowActionService
    .getJointGraphWrapper()
    .getMainJointPaper()
    ?.getModelById(id)
    .attr("rect.boundary/fill", "rgba(" + color+ ")");
  }

  public getDifference(v1: Workflow, v2: Workflow) {
    var diff = { added: [], modified: [] };
    var c1 = v1.content;
    var c2 = v2.content;
    var differentTypes = reduce(
      c2,
      function (result: string[], value, key) {
        return isEqual(value, c1[key as WorkflowContents]) ? result : result.concat(key);
      },
      []
    );
    for (var type of differentTypes) {
      if (TYPES_FOR_WORKFLOW_DIFF_CALC.includes(type)) {
        let getIDList = function (element: Elements, index: number) {
          var id = "";
          if (type.substring(type.length - 2, type.length) === "es") {
            id = type.substring(0, type.length - 2) + "ID";
          } else {
            id = type.substring(0, type.length - 1) + "ID";
          }
          return { id: element[id as keyof Elements], index: index };
        };
        var l1 = c1[type as WorkflowContents] as Elements[];
        var IDList1 = l1.map(getIDList);
        var l2 = c2[type as WorkflowContents] as Elements[];
        var IDList2 = l2.map(getIDList);
        console.log(l1)
        diff.added = IDList2.filter(x => !IDList1.map(y => y.id).includes(x.id)).map(x => x.id);
        diff.modified = IDList2.filter(x => IDList1.map(y => y.id).includes(x.id))
          .filter(function (x) {
            var i1 = IDList1.filter(y => y.id == x.id)[0].index;
            return !isEqual(l1[i1], l2[x.index]);
          })
          .map(x => x.id);
      }
    }
    return diff;
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
    for (var id of Object.values(differentOpIDsList).reduce((accumulator, value) => accumulator.concat(value), [])) {
      this.highlighOpBoundary(id, "0,0,0,0");
    }
  }
}

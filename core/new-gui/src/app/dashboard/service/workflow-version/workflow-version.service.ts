import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../workspace/service/undo-redo/undo-redo.service";
import { isEqual, reduce } from "lodash";
import { JointHighlights } from "src/app/workspace/service/workflow-graph/model/joint-graph-wrapper";
import { Breakpoint, OperatorLink, OperatorPredicate, Point } from "src/app/workspace/types/workflow-common.interface";

export const DISPLAY_WORKFLOW_VERIONS_EVENT = "display_workflow_versions_event";

type WorkflowContents = keyof WorkflowContent;
type Elements = Breakpoint | OperatorLink | OperatorPredicate | Point;

@Injectable({
  providedIn: "root",
})
export class WorkflowVersionService {
  private workflowVersionsObservable = new Subject<readonly string[]>();
  private displayParticularWorkflowVersion = new BehaviorSubject<boolean>(false);
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
    var elementToHighlight = this.getDifference(this.workflowActionService.getWorkflow(), workflow);
    this.getPositionDifference(this.workflowActionService.getWorkflow(), workflow);
    // disable persist to DB because it is read only
    this.workflowPersistService.setWorkflowPersistFlag(false);
    // disable the undoredo service because reloading the workflow is considered an action
    this.undoRedoService.disableWorkFlowModification();
    // reload the read only workflow version on the paper
    // temporarily set JointJS asyncRendering to false to avoid errors,
    // TODO: fix the error and set asyncRendering to true to improve performance
    this.workflowActionService.reloadWorkflow(workflow, false);
    this.setDisplayParticularVersion(true);
    // disable modifications because it is read only
    this.workflowActionService.disableWorkflowModification();
    this.highlightDifference(elementToHighlight);
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
    console.log("position difference:", difference);
  }

  public highlightDifference(difference: JointHighlights) {
    this.workflowActionService.getJointGraphWrapper().highlightElements(difference);
  }

  public getDifference(v1: Workflow, v2: Workflow) {
    var typeToInclude = ["operators", "links", "groups", "breakpoints"];
    var difference = {
      operators: [],
      groups: [],
      links: [],
    };
    var c1 = v1.content;
    var c2 = v2.content;
    var differentTypes = reduce(
      c2,
      function (result: string[], value, key) {
        return isEqual(value, c1[key as WorkflowContents]) ? result : result.concat(key);
      },
      []
    );
    for (var i = 0; i < differentTypes.length; i++) {
      var type = differentTypes[i];
      if (typeToInclude.includes(type)) {
        console.log(type);
        let getIDList = function (element: Elements, index: number) {
          return { id: element[(type.substring(0, type.length - 1) + "ID") as keyof Elements], index: index };
        };
        var l1 = c1[type as keyof WorkflowContent] as Elements[];
        var IDList1 = l1.map(getIDList);
        var l2 = c2[type as keyof WorkflowContent] as Elements[];
        var IDList2 = l2.map(getIDList);

        var added = IDList2.filter(x => !IDList1.map(y => y.id).includes(x.id)).map(x => x.id);
        var modified = IDList2.filter(x => IDList1.map(y => y.id).includes(x.id))
          .filter(function (x) {
            var i1 = IDList1.filter(y => y.id == x.id)[0].index;
            return !isEqual(l1[i1], l2[x.index]);
          })
          .map(x => x.id);

        console.log("added:", added);
        console.log("modified:", modified);
        difference[type as keyof typeof difference] = added.concat(modified);
      }
    }
    console.log("difference:", difference);
    return difference;
    // console.log(d)
    // var allDifference: object[] = [];
    // var positionDifference: string[] = [];
    // for (var i = 0; i < d.length; i++) {
    //   var c = d[i];
    //   // console.log(c);
    //   // console.log(c1[c as keyof typeof c1])
    //   // console.log(c2[c as keyof typeof c2])
    //   var d1 = reduce(c2[c as keyof typeof c2], function(result: string[], value, key) {
    //     var x = c1[c as keyof typeof c1];
    //     // console.log(c.substring(0, c.length - 1) + "ID")
    //     // console.log(value[c.substring(0, c.length - 1) + "ID" as keyof typeof values])
    //     if (c === "operatorPositions" && !isEqual(value, x[key as keyof typeof x])) {
    //       positionDifference.push(key)
    //       return []
    //     }
    //     return isEqual(value, x[key as keyof typeof x]) ? result : result.concat(value[c.substring(0, c.length - 1) + "ID" as keyof typeof values]);
    //   }, []);
    //     allDifference = allDifference.concat(d1)
    // }
    // console.log("pos", positionDifference)
    // console.log(allDifference);

    // var diff = Object.entries(c2).reduce((diff, [category, valueList2]) => {
    //   if (c1.hasOwnProperty(category)) {
    //     const valueList1 = c1[category as keyof typeof c1];
    //     if (!isEqual(valueList1, valueList2)) {
    //       // var diff1 = Object.entries(valueList2).reduce((diff1, [key, v2]) => {
    //       //   const v1 = valueList1[key as keyof typeof valueList1];
    //       //   if (!isEqual())
    //       //   return {...diff1, [key]: v2};
    //       // })
    //       // for (var v2 in valueList2 as Array<object>) {
    //       //   if (!valueList1.includes(v2)) {

    //       //   }
    //       // }
    //       console.log("!", difference(valueList1 as Array<unknown>, valueList2 as Array<unknown>));
    //       var diff1 = difference(valueList1 as Array<unknown>, valueList2 as Array<unknown>);
    //       if (diff1 != []) {
    //         console.log(valueList1);
    //         console.log(valueList2);
    //       }
    //       return {
    //         ...diff,
    //         [category]: valueList2,
    //       };
    //     }
    //   }
    //   return diff;
    // }, {});
    // console.log(diff);

    // Object.entries(v2.content).forEach(([key, value])=>{
    //   if (typeToInclude.includes(key)) {
    //     console.log(value);
    //   }
    // });

    // for (var elementType in v2.content) {
    //   if (typeToInclude.includes(elementType)) {
    //     var elements = v2.content[elementType as keyof typeof v2.content];
    //     for (var i = 0; i < elements.length; i++) {
    //       console.log(elements[i]);
    //     }
    //   }
    // }
  }

  // public getDiff(origObj: Workflow, newObj: Workflow) {
  //   function changes(newObj: object, origObj: object) {
  //     let arrayIndexCounter = 0
  //     return transform(newObj, function (result: Array<unknown> | Object, value, key) {
  //       if (!isEqual(value, origObj[key])) {
  //         let resultKey = isArray(origObj) ? arrayIndexCounter++ : key
  //         result[resultKey] = (isObject(value) && isObject(origObj[key])) ? changes(value, origObj[key]) : value
  //       }
  //     })
  //   }
  //   return changes(newObj, origObj)
  // }

  public revertToVersion() {
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
}

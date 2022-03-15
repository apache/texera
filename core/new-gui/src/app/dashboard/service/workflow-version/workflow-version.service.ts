import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import { Workflow } from "../../../common/type/workflow";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { UndoRedoService } from "../../../workspace/service/undo-redo/undo-redo.service";

export const DISPLAY_WORKFLOW_VERIONS_EVENT = "display_workflow_versions_event";

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
    // disable persist to DB because it is read only
    this.workflowPersistService.setWorkflowPersistFlag(false);
    // disable the undoredo service because reloading the workflow is considered an action
    this.undoRedoService.disableWorkFlowModification();
    // reload the read only workflow version on the paper
    this.workflowActionService.reloadWorkflow(workflow);
    this.setDisplayParticularVersion(true);
    // disable modifications because it is read only
    this.workflowActionService.disableWorkflowModification();
  }

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

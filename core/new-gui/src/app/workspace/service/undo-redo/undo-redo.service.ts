import { Injectable } from "@angular/core";
import { Subject } from "rxjs";
import * as Y from "yjs";

/* TODO LIST FOR BUGS
1. Problem with repeatedly adding and deleting a link without letting go, unintended behavior
2. See if there's a way to only store a previous version of an operator's properties
after a certain period of time so we don't undo one character at a time */

@Injectable({
  providedIn: "root",
})
export class UndoRedoService {
  // lets us know whether to listen to the JointJS observables, most of the time we don't
  public listenJointCommand: boolean = true;
  // private testGraph: WorkflowGraphReadonly;

  private undoManager?: Y.UndoManager;

  private workFlowModificationEnabled = true;

  public setUndoManager(undoManager: Y.UndoManager) {
    this.undoManager = undoManager;
  }

  public enableWorkFlowModification() {
    this.workFlowModificationEnabled = true;
  }
  public disableWorkFlowModification() {
    this.workFlowModificationEnabled = false;
  }

  public undoAction(): void {
    if (!this.workFlowModificationEnabled) {
      console.error("attempted to undo a workflow-modifying command while workflow modification is disabled");
      return;
    }
    if (this.undoManager && this.undoManager.canUndo()) {
      this.setListenJointCommand(false);
      this.undoManager.undo();
      this.setListenJointCommand(true);
    }
  }

  public redoAction(): void {
    if (!this.workFlowModificationEnabled) {
      console.error("attempted to redo a workflow-modifying command while workflow modification is disabled");
      return;
    }
    if (this.undoManager && this.undoManager.canRedo()) {
      this.setListenJointCommand(false);
      this.undoManager.redo();
      this.setListenJointCommand(true);
    }
  }


  public setListenJointCommand(toggle: boolean): void {
    this.listenJointCommand = toggle;
  }


  public canUndo(): boolean {
    if (this.undoManager)
    return (
      (this.workFlowModificationEnabled && this.undoManager?.canUndo())
    );
    else return false;
  }

  public canRedo(): boolean {
    if (this.undoManager)
      return (
        (this.workFlowModificationEnabled && this.undoManager?.canRedo())
      );
    else return false;
  }

  public clearUndoStack(): void {
  }

  public clearRedoStack(): void {
  }
}

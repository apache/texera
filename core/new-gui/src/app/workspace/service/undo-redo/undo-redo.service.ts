import { Injectable } from "@angular/core";
import * as Y from "yjs";

/**
 * After the introduction of shared-editing, this service basically wraps the internal yjs undo-redo manager, except it
 * also adds some of our custom conditions for being able to undo/redo.
 */

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

  public getUndoLength(): number {
    return <number>this.undoManager?.undoStack.length;
  }

  public getRedoLength(): number {
    return <number>this.undoManager?.redoStack.length;
  }

  public canUndo(): boolean {
    if (this.undoManager) return this.workFlowModificationEnabled && this.undoManager?.canUndo();
    else return false;
  }

  public canRedo(): boolean {
    if (this.undoManager) return this.workFlowModificationEnabled && this.undoManager?.canRedo();
    else return false;
  }

  public clearUndoStack(): void {
    this.undoManager?.clear(true, false);
  }

  public clearRedoStack(): void {
    this.undoManager?.clear(false, true);
  }
}

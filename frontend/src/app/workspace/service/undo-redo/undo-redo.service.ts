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

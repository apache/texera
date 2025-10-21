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
import { CoeditorPresenceService } from "../workflow-graph/model/coeditor-presence.service";
import { Coeditor, CoeditorState } from "../../../common/type/user";

/**
 * CopilotCoeditorService manages the AI copilot as a virtual coeditor,
 * allowing it to appear in the collaborative editing UI with its own avatar and presence indicators.
 */
@Injectable({
  providedIn: "root",
})
export class CopilotCoeditorService {
  // Virtual copilot coeditor
  private readonly COPILOT_COEDITOR: Coeditor = {
    uid: -1,
    name: "AI",
    email: "copilot@texera.ai",
    role: "ADMIN" as any,
    color: "#9333ea", // Purple color for copilot
    clientId: "copilot-virtual",
    comment: "",
  };

  private isRegistered = false;
  private currentState: Partial<CoeditorState> = {};
  private currentEditingIntervalId?: NodeJS.Timer;

  constructor(
    private workflowActionService: WorkflowActionService,
    private coeditorPresenceService: CoeditorPresenceService
  ) {}

  /**
   * Register the copilot as a virtual coeditor
   */
  public register(): void {
    if (this.isRegistered) return;

    // Manually add copilot to coeditors list
    if (!this.coeditorPresenceService.coeditors.find(c => c.clientId === this.COPILOT_COEDITOR.clientId)) {
      this.coeditorPresenceService.coeditors.push(this.COPILOT_COEDITOR);
    }

    this.isRegistered = true;
    console.log("Copilot registered as virtual coeditor");
  }

  /**
   * Unregister the copilot and clean up all visual indicators
   */
  public unregister(): void {
    if (!this.isRegistered) return;

    // Clear all current indicators
    this.clearAll();

    // Remove from coeditors list
    const index = this.coeditorPresenceService.coeditors.findIndex(c => c.clientId === this.COPILOT_COEDITOR.clientId);
    if (index >= 0) {
      this.coeditorPresenceService.coeditors.splice(index, 1);
    }

    this.isRegistered = false;
    console.log("Copilot unregistered");
  }

  /**
   * Check if copilot is registered
   */
  public isActive(): boolean {
    return this.isRegistered;
  }

  /**
   * Get the copilot coeditor object
   */
  public getCopilot(): Coeditor {
    return this.COPILOT_COEDITOR;
  }

  /**
   * Show that the copilot is currently viewing/editing an operator
   */
  public showEditingOperator(operatorId: string): void {
    if (!this.isRegistered) return;

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();

    // Clear previous editing indicator
    if (this.currentEditingIntervalId && this.currentState.currentlyEditing) {
      jointWrapper.removeCurrentEditing(
        this.COPILOT_COEDITOR,
        this.currentState.currentlyEditing,
        this.currentEditingIntervalId
      );
    }

    // Set new editing indicator
    this.currentEditingIntervalId = jointWrapper.setCurrentEditing(this.COPILOT_COEDITOR, operatorId);
    this.currentState.currentlyEditing = operatorId;
  }

  /**
   * Clear the copilot's editing indicator
   */
  public clearEditingOperator(): void {
    if (!this.isRegistered || !this.currentState.currentlyEditing) return;

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
    if (this.currentEditingIntervalId) {
      jointWrapper.removeCurrentEditing(
        this.COPILOT_COEDITOR,
        this.currentState.currentlyEditing,
        this.currentEditingIntervalId
      );
      this.currentEditingIntervalId = undefined;
    }
    this.currentState.currentlyEditing = undefined;
  }

  /**
   * Show that the copilot changed a property on an operator
   */
  public showPropertyChanged(operatorId: string): void {
    if (!this.isRegistered) return;

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
    jointWrapper.setPropertyChanged(this.COPILOT_COEDITOR, operatorId);

    // Auto-clear after 2 seconds
    setTimeout(() => {
      jointWrapper.removePropertyChanged(this.COPILOT_COEDITOR, operatorId);
    }, 2000);

    this.currentState.changed = operatorId;
  }

  /**
   * Highlight operators to show copilot is inspecting them
   */
  public highlightOperators(operatorIds: string[]): void {
    if (!this.isRegistered) return;

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();

    // Clear previous highlights
    if (this.currentState.highlighted) {
      this.currentState.highlighted.forEach(opId => {
        jointWrapper.deleteCoeditorOperatorHighlight(this.COPILOT_COEDITOR, opId);
      });
    }

    // Add new highlights
    operatorIds.forEach(opId => {
      jointWrapper.addCoeditorOperatorHighlight(this.COPILOT_COEDITOR, opId);
    });

    this.currentState.highlighted = operatorIds;
  }

  /**
   * Clear operator highlights
   */
  public clearHighlights(): void {
    if (!this.isRegistered || !this.currentState.highlighted) return;

    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
    this.currentState.highlighted.forEach(opId => {
      jointWrapper.deleteCoeditorOperatorHighlight(this.COPILOT_COEDITOR, opId);
    });

    this.currentState.highlighted = undefined;
  }

  /**
   * Clear all copilot presence indicators
   */
  public clearAll(): void {
    this.clearEditingOperator();
    this.clearHighlights();
    this.currentState = {};
  }
}

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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ActionLineageService } from "../../service/copilot/action-lineage.service";
import { AgentResponse } from "../../service/copilot/texera-copilot";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { JointUIService } from "../../service/joint-ui/joint-ui.service";

@UntilDestroy()
@Component({
  selector: "texera-operator-response-panel",
  templateUrl: "./operator-response-panel.component.html",
  styleUrls: ["./operator-response-panel.component.scss"],
})
export class OperatorResponsePanelComponent implements OnInit {
  public isVisible = false;
  public selectedOperatorId: string | null = null;
  public relatedResponses: AgentResponse[] = [];
  public panelTop = 0;
  public panelLeft = 0;

  constructor(
    private actionLineageService: ActionLineageService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    // Listen to operator selection events
    this.workflowActionService
      .getJointGraphWrapper()
      .getJointOperatorHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe((highlightedOperatorIDs: readonly string[]) => {
        if (highlightedOperatorIDs.length === 1) {
          // Single operator selected
          this.selectedOperatorId = highlightedOperatorIDs[0];
          this.loadRelatedResponses(highlightedOperatorIDs[0]);
          this.updatePanelPosition(highlightedOperatorIDs[0]);
          this.isVisible = true;
        } else {
          // Multiple or no operators selected
          this.isVisible = false;
          this.selectedOperatorId = null;
          this.relatedResponses = [];
        }
      });

    // Listen to operator position changes to update panel position
    this.workflowActionService
      .getJointGraphWrapper()
      .getElementPositionChangeEvent()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (this.isVisible && this.selectedOperatorId === event.elementID) {
          this.updatePanelPosition(event.elementID);
        }
      });
  }

  /**
   * Load related responses for a given operator
   */
  private loadRelatedResponses(operatorId: string): void {
    this.relatedResponses = this.actionLineageService.getResponsesByOperator(operatorId);
  }

  /**
   * Update panel position to display below the operator
   */
  private updatePanelPosition(operatorId: string): void {
    try {
      const position = this.workflowActionService.getJointGraphWrapper().getElementPosition(operatorId);
      const operatorHeight = JointUIService.DEFAULT_OPERATOR_HEIGHT;

      // Position the panel below the operator with some spacing
      const spacing = 10;
      this.panelLeft = position.x;
      this.panelTop = position.y + operatorHeight + spacing;
    } catch (error) {
      console.error("Failed to get operator position:", error);
      // If we can't get the position, hide the panel
      this.isVisible = false;
    }
  }

  /**
   * Format timestamp for display
   */
  public formatTimestamp(timestamp: number): string {
    return new Date(timestamp).toLocaleString();
  }

  /**
   * Get a preview of the response content (first 100 characters)
   */
  public getContentPreview(content: string): string {
    if (content.length <= 100) {
      return content;
    }
    return content.substring(0, 100) + "...";
  }

  /**
   * Close the panel
   */
  public closePanel(): void {
    this.isVisible = false;
    this.selectedOperatorId = null;
    this.relatedResponses = [];
  }
}

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

import { ChangeDetectorRef, Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { combineLatest } from "rxjs";
import { TexeraCopilotManagerService } from "../../service/copilot/texera-copilot-manager.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ReActStep } from "../../service/copilot/texera-copilot";

/**
 * AgentInteractionComponent provides a compact interface for users to send feedback
 * or messages to agents regarding a specific operator. It can be embedded in various
 * panels like inline-code-panel, inline-property-panel, or operator-property-edit-frame.
 */
@UntilDestroy()
@Component({
  selector: "texera-agent-interaction",
  templateUrl: "./agent-interaction.component.html",
  styleUrls: ["./agent-interaction.component.scss"],
})
export class AgentInteractionComponent implements OnInit, OnChanges {
  @Input() operatorId!: string;
  @Input() operatorDisplayName?: string;
  @Input() compact: boolean = true;

  public availableAgents: Array<{ id: string; name: string }> = [];
  public selectedAgentId: string | null = null;
  public feedbackMessage: string = "";

  private modifyingReActSteps: ReActStep[] = [];

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadAvailableAgents();
    this.copilotManagerService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.loadAvailableAgents();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["operatorId"] && this.operatorId) {
      this.loadModifyingReActSteps(this.operatorId);
    }
  }

  private loadAvailableAgents(): void {
    this.copilotManagerService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => {
        this.availableAgents = agents.map(agent => ({
          id: agent.id,
          name: agent.name,
        }));

        if (this.availableAgents.length === 1) {
          this.selectedAgentId = this.availableAgents[0].id;
        }

        this.changeDetectorRef.detectChanges();
      });
  }

  private loadModifyingReActSteps(operatorId: string): void {
    this.modifyingReActSteps = [];

    this.copilotManagerService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => {
        if (agents.length === 0) {
          return;
        }

        const allStepObservables = agents.map(agent =>
          this.copilotManagerService.getReActStepsByOperatorAccess(agent.id, operatorId)
        );

        combineLatest(allStepObservables)
          .pipe(untilDestroyed(this))
          .subscribe(results => {
            const allModifyingSteps: ReActStep[] = [];
            results.forEach(result => {
              allModifyingSteps.push(...result.modifiedBy);
            });

            this.modifyingReActSteps = allModifyingSteps.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());
            this.changeDetectorRef.detectChanges();
          });
      });
  }

  public sendFeedbackToAgent(): void {
    if (!this.selectedAgentId || !this.feedbackMessage.trim() || !this.operatorId) {
      return;
    }

    const relevantSteps = this.modifyingReActSteps.map(step => ({
      agentId: this.selectedAgentId!,
      messageId: step.messageId,
      stepId: step.stepId,
    }));

    const operatorName = this.operatorDisplayName || this.getOperatorName() || "this operator";
    const contextMessage = `Regarding operator "${operatorName}" (ID: ${this.operatorId}): ${this.feedbackMessage.trim()}`;

    this.copilotManagerService.sendMessage(this.selectedAgentId, contextMessage, relevantSteps);

    this.notificationService.success("Message sent to agent successfully");
    this.feedbackMessage = "";
    this.changeDetectorRef.detectChanges();
  }

  private getOperatorName(): string | undefined {
    try {
      const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
      return operator?.customDisplayName || undefined;
    } catch {
      return undefined;
    }
  }

  public canSend(): boolean {
    return !!this.selectedAgentId && !!this.feedbackMessage.trim();
  }
}

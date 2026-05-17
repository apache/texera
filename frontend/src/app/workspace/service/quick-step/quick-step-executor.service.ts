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
import { BehaviorSubject, firstValueFrom, Observable, race, timer } from "rxjs";
import { filter, map, take } from "rxjs/operators";
import { NzModalService } from "ng-zorro-antd/modal";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowSnippetService } from "../../../dashboard/service/user/workflow-snippet/workflow-snippet.service";
import { SnippetCanvasService } from "../snippet/snippet-canvas.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { DataProfilingModalComponent } from "../../component/data-profiling-panel/data-profiling-modal.component";
import { DataProfilingService } from "../../component/data-profiling-panel/data-profiling.service";
import { ShareAccessComponent } from "../../../dashboard/component/user/share-access/share-access.component";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { AgentService } from "../agent/agent.service";
import {
  QuickStep,
  QuickStepAction,
} from "../../../dashboard/type/quick-step.interface";

const REPORT_AGENT_PROMPT =
  "Analyze the workflow execution results. Generate a comprehensive report with model comparison, key metrics, and findings.";
const AGENT_RESPONSE_TIMEOUT_MS = 90_000;

export type StepStatus = "pending" | "running" | "completed" | "failed" | "skipped" | "cancelled";

export interface QuickStepRunState {
  id: string;
  quickStep: QuickStep;
  startedAt: number;
  active: boolean;
  cancelled: boolean;
  currentIndex: number;
  steps: {
    action: QuickStepAction;
    status: StepStatus;
    detail?: string;
  }[];
}

/**
 * QuickStepExecutorService runs a saved quick step as a chain of real
 * Texera actions: actual workflow execution, the real Data Profiling modal,
 * placing real operator snippets, the real report generator, and the real
 * Share dialog.
 *
 * Each step transitions pending → running → completed (or cancelled / failed).
 * Cancellation stops the chain after the currently running step.
 */
@Injectable({
  providedIn: "root",
})
export class QuickStepExecutorService {
  private readonly stateSubject = new BehaviorSubject<QuickStepRunState | null>(null);
  private cancelToken: { cancelled: boolean } | null = null;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowSnippetService: WorkflowSnippetService,
    private snippetCanvasService: SnippetCanvasService,
    private executeWorkflowService: ExecuteWorkflowService,
    private dataProfilingService: DataProfilingService,
    private workflowPersistService: WorkflowPersistService,
    private agentService: AgentService,
    private modalService: NzModalService,
    private notificationService: NotificationService
  ) {}

  public state$(): Observable<QuickStepRunState | null> {
    return this.stateSubject.asObservable();
  }

  public currentState(): QuickStepRunState | null {
    return this.stateSubject.value;
  }

  public isRunning(): boolean {
    return !!this.stateSubject.value?.active;
  }

  public cancel(): void {
    const state = this.stateSubject.value;
    if (!state || !state.active) return;
    if (this.cancelToken) this.cancelToken.cancelled = true;
    const updated: QuickStepRunState = {
      ...state,
      active: false,
      cancelled: true,
      steps: state.steps.map(s =>
        s.status === "running" || s.status === "pending" ? { ...s, status: "cancelled" } : s
      ),
    };
    this.stateSubject.next(updated);
  }

  public dismiss(): void {
    if (this.isRunning()) return;
    this.stateSubject.next(null);
  }

  public async run(quickStep: QuickStep): Promise<void> {
    if (this.isRunning()) {
      this.notificationService.info("A quick step is already running.");
      return;
    }
    const cancelToken = { cancelled: false };
    this.cancelToken = cancelToken;

    const initial: QuickStepRunState = {
      id: `${quickStep.id}-${Date.now()}`,
      quickStep,
      startedAt: Date.now(),
      active: true,
      cancelled: false,
      currentIndex: -1,
      steps: quickStep.steps
        .slice()
        .sort((a, b) => a.order - b.order)
        .map(action => ({ action, status: "pending" as StepStatus })),
    };
    this.stateSubject.next(initial);

    try {
      for (let i = 0; i < initial.steps.length; i++) {
        if (cancelToken.cancelled) break;
        const current = this.stateSubject.value!;
        this.stateSubject.next({
          ...current,
          currentIndex: i,
          steps: current.steps.map((s, idx) =>
            idx === i ? { ...s, status: "running" as StepStatus } : s
          ),
        });

        let detail: string | undefined;
        let failed = false;
        try {
          detail = await this.executeAction(initial.steps[i].action, cancelToken);
        } catch (e) {
          failed = true;
          detail = e instanceof Error ? e.message : String(e);
        }
        if (cancelToken.cancelled) break;

        const afterRun = this.stateSubject.value!;
        this.stateSubject.next({
          ...afterRun,
          steps: afterRun.steps.map((s, idx) =>
            idx === i
              ? { ...s, status: failed ? ("failed" as StepStatus) : ("completed" as StepStatus), detail }
              : s
          ),
        });
        if (failed) break;
      }
    } finally {
      this.cancelToken = null;
      const last = this.stateSubject.value;
      if (last && last.active && !last.cancelled) {
        this.stateSubject.next({ ...last, active: false });
      }
    }
  }

  private async executeAction(
    action: QuickStepAction,
    cancelToken: { cancelled: boolean }
  ): Promise<string | undefined> {
    switch (action.action) {
      case "profile_data":
        return this.runProfileData(action);
      case "add_snippet":
        return this.runAddSnippet(action);
      case "run_workflow":
        return this.runRunWorkflow(cancelToken);
      case "generate_report":
        return this.runGenerateReport();
      case "publish_hub":
        return this.runPublishHub();
      case "notify": {
        const message = action.config?.message ?? action.label;
        this.notificationService.success(message);
        return message;
      }
    }
  }

  // === Real action implementations ===

  private async runProfileData(action: QuickStepAction): Promise<string> {
    const source = this.inferProfilingSource();
    // Kick off the profile computation in parallel — its result populates the
    // step detail with real findings whether or not the user keeps the modal open.
    const profilePromise = firstValueFrom(this.dataProfilingService.getProfile(source));

    const modalRef = this.modalService.create({
      nzContent: DataProfilingModalComponent,
      nzData: { source },
      nzWidth: 760,
      nzFooter: null,
      nzMaskClosable: true,
      nzBodyStyle: { padding: "0" },
    });
    await firstValueFrom(modalRef.afterClose);

    try {
      const profile = await profilePromise;
      const missingCols = profile.columns
        .filter(c => c.missingPercent > 0)
        .sort((a, b) => b.missingPercent - a.missingPercent);
      const topMissing = missingCols[0];
      const parts = [
        `${profile.rowCount.toLocaleString()} rows`,
        `${profile.columns.length} columns`,
        `${profile.duplicateRows} duplicates`,
      ];
      if (topMissing) {
        parts.push(`${topMissing.missingPercent.toFixed(1)}% missing in ${topMissing.name}`);
      }
      return `Profiled ${profile.source} — ${parts.join(", ")}.`;
    } catch {
      return `Profiled ${source}.`;
    }
  }

  private async runAddSnippet(action: QuickStepAction): Promise<string> {
    const targetName = action.config?.snippetName;
    const snippet = targetName
      ? this.workflowSnippetService.list().find(s => s.name === targetName)
      : undefined;
    if (!snippet) {
      throw new Error(`Snippet "${targetName ?? "?"}" not found.`);
    }
    const operatorCount = this.workflowActionService.getTexeraGraph().getAllOperators().length;
    const offsetX = 120 + operatorCount * 30;
    const offsetY = 160 + operatorCount * 25;
    const placed = this.snippetCanvasService.placeSnippetAtLocal(snippet, { x: offsetX, y: offsetY });
    if (!placed) {
      throw new Error(`Failed to place snippet "${snippet.name}" — canvas not ready.`);
    }
    return `Added ${snippet.operators.length} operators from "${snippet.name}".`;
  }

  private async runRunWorkflow(cancelToken: { cancelled: boolean }): Promise<string> {
    const initialState = this.executeWorkflowService.getExecutionState().state;
    if (initialState === ExecutionState.Running || initialState === ExecutionState.Initializing) {
      // Already running — just wait for it to finish.
    } else {
      this.executeWorkflowService.executeWorkflow("");
    }
    const terminalStates: ExecutionState[] = [
      ExecutionState.Completed,
      ExecutionState.Failed,
      ExecutionState.Killed,
      ExecutionState.Terminated,
    ];
    const terminal = await firstValueFrom(
      this.executeWorkflowService.getExecutionStateStream().pipe(
        filter(({ current }) => terminalStates.includes(current.state)),
        take(1)
      )
    );
    if (cancelToken.cancelled) return "Cancelled.";
    if (terminal.current.state === ExecutionState.Failed) {
      throw new Error("Workflow execution failed.");
    }
    if (terminal.current.state === ExecutionState.Killed) {
      throw new Error("Workflow execution was killed.");
    }
    return "Workflow execution completed.";
  }

  /**
   * Generate a report by messaging the active AI agent. We send the report
   * prompt over the agent's WebSocket and wait for the final ReActStep
   * (`isEnd: true`) that the agent emits after responding. If no agent is
   * currently connected we fall back to pre-filling the chat input so the
   * user can pick an agent and click send.
   */
  private async runGenerateReport(): Promise<string> {
    const activeAgentIds = this.agentService.getActivelyConnectedAgentIds();
    if (activeAgentIds.length === 0) {
      this.agentService.prefillChatInput(REPORT_AGENT_PROMPT);
      this.notificationService.info(
        "No active AI agent. Open the agent panel and click Send — the report prompt is pre-filled."
      );
      return "No active agent — chat input prefilled, awaiting user to send.";
    }
    const agentId = activeAgentIds[0];

    // Record current step count so we can detect new agent steps that arrive
    // after we send our prompt.
    const stepsBefore = await firstValueFrom(this.agentService.getReActStepsObservable(agentId));
    const baseline = stepsBefore.length;

    this.agentService.sendMessage(agentId, REPORT_AGENT_PROMPT, "chat");

    const responded$ = this.agentService.getReActStepsObservable(agentId).pipe(
      filter(steps => {
        if (steps.length <= baseline) return false;
        const latest = steps[steps.length - 1];
        return latest?.isEnd === true && latest?.role !== "user";
      }),
      take(1),
      map(steps => steps[steps.length - 1])
    );
    const timedOut$ = timer(AGENT_RESPONSE_TIMEOUT_MS).pipe(map(() => null));

    const winner = await firstValueFrom(race(responded$, timedOut$));
    if (winner === null) {
      throw new Error(
        `Agent did not finish within ${Math.round(AGENT_RESPONSE_TIMEOUT_MS / 1000)}s. Check the agent panel for progress.`
      );
    }
    const preview = (winner.content ?? "").trim();
    const condensed = preview.length > 140 ? preview.slice(0, 140) + "…" : preview;
    return condensed
      ? `Agent reported: "${condensed}"`
      : "Agent finished generating the report.";
  }

  private async runPublishHub(): Promise<string> {
    const metadata = this.workflowActionService.getWorkflowMetadata();
    const workflowId = metadata?.wid;
    if (workflowId == null) {
      throw new Error("Save the workflow before publishing — it has no ID yet.");
    }
    let allOwners: string[] = [];
    try {
      allOwners = await firstValueFrom(this.workflowPersistService.retrieveOwners());
    } catch {
      allOwners = [];
    }
    const modalRef = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: !metadata?.readonly,
        type: "workflow",
        id: workflowId,
        allOwners,
        inWorkspace: true,
      },
      nzFooter: null,
      nzTitle: "Share this workflow with others",
      nzCentered: true,
      nzWidth: "800px",
    });
    await firstValueFrom(modalRef.afterClose);
    return `Opened Share dialog for "${metadata?.name ?? "workflow"}".`;
  }

  // Walk the current workflow looking for a file-bearing operator and return
  // its path, falling back to the default profiler source.
  private inferProfilingSource(): string {
    const fileFields = ["fileName", "filePath", "file_path", "path"];
    for (const op of this.workflowActionService.getTexeraGraph().getAllOperators()) {
      const props = op.operatorProperties ?? {};
      for (const field of fileFields) {
        const value = (props as any)[field];
        if (typeof value === "string" && value.trim().length > 0) {
          return value;
        }
      }
    }
    return "diabetes.csv";
  }
}

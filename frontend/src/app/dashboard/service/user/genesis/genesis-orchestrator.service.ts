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

import { HttpClient, HttpErrorResponse, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Router } from "@angular/router";
import { firstValueFrom } from "rxjs";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { DASHBOARD_USER_WORKSPACE } from "../../../../app-routing.constant";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { AuthService } from "../../../../common/service/user/auth.service";
import { WorkflowContent } from "../../../../common/type/workflow";
import { ExecuteWorkflowService } from "../../../../workspace/service/execute-workflow/execute-workflow.service";
import { GenesisBuildModalComponent } from "../../../component/user/genesis/genesis-build-modal.component";
import { GenesisCardOverlayService } from "./genesis-card-overlay.service";
import { GenesisBuildProgressService } from "./genesis-build-progress.service";
import { GenesisService, InstantiateResponse } from "./genesis.service";
import { WorkflowGrowAnimator } from "./workflow-grow.service";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { DashboardWorkflowComputingUnit } from "../../../../common/type/workflow-computing-unit";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { OperatorPredicate } from "../../../../workspace/types/workflow-common.interface";

const AGENT_WS_IDLE_MS = 300_000;
const GENESIS_LEGACY_TEMPLATE_SUGGESTION_IDS = new Set<string>([
  "diabetes_prediction",
  "diabetes_risk_factors",
  "diabetes_clustering",
  "generic_classification",
  "generic_regression",
  "generic_clustering",
]);
const CU_READY_TIMEOUT_MS = 30_000;
const CU_POLL_INTERVAL_MS = 1_000;
const EXEC_WS_READY_TIMEOUT_MS = 8_000;

function isGenesisAgentMode(r: InstantiateResponse): boolean {
  return (
    r.mode === "agent" &&
    typeof r.agent_prompt === "string" &&
    r.agent_prompt.length > 0 &&
    typeof r.model === "string" &&
    r.model.length > 0
  );
}

function formatGenesisAgentStepLine(step: {
  stepId?: number;
  toolCalls?: { toolName?: string; input?: Record<string, unknown> }[];
  content?: string;
}): string {
  const n = step.stepId != null ? String(step.stepId) : "?";
  if (step.toolCalls?.length) {
    const parts = step.toolCalls.map(tc => {
      const name = tc.toolName ?? "tool";
      if (name === "addOperator") {
        const t = tc.input?.["operatorType"] ?? tc.input?.["operator_type"];
        if (typeof t === "string" && t.length > 0) {
          return `Added ${t}`;
        }
      }
      return name;
    });
    return `Step ${n}: ${parts.join(", ")}`;
  }
  const text = (step.content ?? "").trim().replace(/\s+/g, " ");
  if (text.length > 140) {
    return `Step ${n}: ${text.slice(0, 137)}…`;
  }
  return text.length > 0 ? `Step ${n}: ${text}` : `Step ${n}`;
}

@Injectable({
  providedIn: "root",
})
export class GenesisOrchestratorService {
  constructor(
    private genesis: GenesisService,
    private cardOverlay: GenesisCardOverlayService,
    private workflowPersist: WorkflowPersistService,
    private executeService: ExecuteWorkflowService,
    private growAnimator: WorkflowGrowAnimator,
    private router: Router,
    private message: NzMessageService,
    private modal: NzModalService,
    private http: HttpClient,
    private buildProgress: GenesisBuildProgressService,
    private config: GuiConfigService,
    private computingUnitStatus: ComputingUnitStatusService,
    private computingUnitManaging: WorkflowComputingUnitManagingService,
    private workflowWebsocket: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService
  ) {}

  public async run(file: File): Promise<void> {
    const jwt = localStorage.getItem("access_token") ?? "";
    this.message.loading("Analyzing data...", { nzDuration: 0 });
    try {
      const uploadResp = await firstValueFrom(this.genesis.upload(file, jwt));
      const analyzeResp = await firstValueFrom(this.genesis.analyze(uploadResp));
      this.message.remove();

      const choice = await this.cardOverlay.show(uploadResp, analyzeResp);

      if (choice.kind === "cancel") {
        return;
      }

      if (choice.kind === "skip") {
        await this.createWorkflowWithOnlyCsvScan(uploadResp);
        return;
      }

      let suggestionIdForAgent: string;
      let targetCol: string;
      let customGoal: string | undefined;

      if (choice.kind === "custom") {
        suggestionIdForAgent = "custom_goal";
        targetCol = analyzeResp.target_column ?? "";
        customGoal = choice.text;
      } else {
        suggestionIdForAgent = choice.suggestionId;
        const picked = analyzeResp.suggestions?.find(s => s.id === choice.suggestionId);
        targetCol =
          picked?.target_column != null && String(picked.target_column).trim() !== ""
            ? String(picked.target_column)
            : analyzeResp.target_column ?? "";
      }

      let inst: InstantiateResponse;
      try {
        inst = await firstValueFrom(
          this.genesis.instantiate(
            suggestionIdForAgent,
            uploadResp.dataset_id,
            uploadResp.file_path,
            targetCol,
            {
              mode: "agent",
              columns: uploadResp.columns,
              uploadId: uploadResp.upload_id,
              customGoal,
            }
          )
        );
      } catch (agentModeErr) {
        console.warn("[Genesis] /instantiate (mode=agent) failed; retrying with mode=template", agentModeErr);
        if (!GENESIS_LEGACY_TEMPLATE_SUGGESTION_IDS.has(suggestionIdForAgent)) {
          throw agentModeErr;
        }
        inst = await firstValueFrom(
          this.genesis.instantiate(
            suggestionIdForAgent,
            uploadResp.dataset_id,
            uploadResp.file_path,
            targetCol,
            {
              mode: "template",
              columns: uploadResp.columns,
            }
          )
        );
      }

      console.info("[Genesis] instantiate response", {
        mode: inst.mode,
        suggestion_id: inst.suggestion_id,
        has_agent_prompt: typeof inst.agent_prompt === "string" && inst.agent_prompt.length > 0,
        model: inst.model,
        workflow_name: inst.workflow_name,
      });

      if (isGenesisAgentMode(inst)) {
        console.info("[Genesis] taking agent path");
        await this.runAgentMode(inst, suggestionIdForAgent);
      } else {
        console.info("[Genesis] taking template path");
        await this.runTemplateMode(inst);
      }
    } catch (e: unknown) {
      this.message.remove();
      this.message.error(`Genesis failed: ${this.formatUserVisibleError(e)}`);
    } finally {
      this.message.remove();
    }
  }

  /**
   * Same as {@link run}, but targets an **existing** workflow already open in the workspace
   * (blank canvas). Does not create a new workflow record.
   */
  public async runIntoCurrentWorkflow(file: File, wid: number): Promise<void> {
    const jwt = localStorage.getItem("access_token") ?? "";
    this.message.loading("Analyzing data...", { nzDuration: 0 });
    try {
      const uploadResp = await firstValueFrom(this.genesis.upload(file, jwt));
      const analyzeResp = await firstValueFrom(this.genesis.analyze(uploadResp));
      this.message.remove();

      const choice = await this.cardOverlay.show(uploadResp, analyzeResp);

      if (choice.kind === "cancel") {
        return;
      }

      if (choice.kind === "skip") {
        await this.addCsvScanOnlyViaGrow(uploadResp);
        return;
      }

      let suggestionIdForAgent: string;
      let targetCol: string;
      let customGoal: string | undefined;

      if (choice.kind === "custom") {
        suggestionIdForAgent = "custom_goal";
        targetCol = analyzeResp.target_column ?? "";
        customGoal = choice.text;
      } else {
        suggestionIdForAgent = choice.suggestionId;
        const picked = analyzeResp.suggestions?.find(s => s.id === choice.suggestionId);
        targetCol =
          picked?.target_column != null && String(picked.target_column).trim() !== ""
            ? String(picked.target_column)
            : analyzeResp.target_column ?? "";
      }

      let inst: InstantiateResponse;
      try {
        inst = await firstValueFrom(
          this.genesis.instantiate(
            suggestionIdForAgent,
            uploadResp.dataset_id,
            uploadResp.file_path,
            targetCol,
            {
              mode: "agent",
              columns: uploadResp.columns,
              uploadId: uploadResp.upload_id,
              customGoal,
            }
          )
        );
      } catch (agentModeErr) {
        console.warn("[Genesis] /instantiate (mode=agent) failed; retrying with mode=template", agentModeErr);
        if (!GENESIS_LEGACY_TEMPLATE_SUGGESTION_IDS.has(suggestionIdForAgent)) {
          throw agentModeErr;
        }
        inst = await firstValueFrom(
          this.genesis.instantiate(
            suggestionIdForAgent,
            uploadResp.dataset_id,
            uploadResp.file_path,
            targetCol,
            {
              mode: "template",
              columns: uploadResp.columns,
            }
          )
        );
      }

      console.info("[Genesis] instantiate response (in-place)", {
        mode: inst.mode,
        suggestion_id: inst.suggestion_id,
        wid,
      });

      if (isGenesisAgentMode(inst)) {
        console.info("[Genesis] taking agent path (in-place)");
        await this.runAgentMode(inst, suggestionIdForAgent, wid);
      } else {
        console.info("[Genesis] taking template path (in-place)");
        await this.runTemplateMode(inst, wid);
      }
    } catch (e: unknown) {
      this.message.remove();
      this.message.error(`Genesis failed: ${this.formatUserVisibleError(e)}`);
    } finally {
      this.message.remove();
    }
  }

  /**
   * Agent path: empty workflow → POST /api/agents → WebSocket prompt → navigate → wait for completion → auto-run.
   * When `existingWid` is set, uses that workflow instead of creating one and reloads the canvas in place.
   */
  private async runAgentMode(
    inst: InstantiateResponse,
    suggestionIdFallback: string,
    existingWid?: number
  ): Promise<void> {
    this.buildProgress.clear();
    let modalRef: NzModalRef | undefined;
    try {
      modalRef = this.modal.create({
        nzTitle: "AI is building your workflow…",
        nzContent: GenesisBuildModalComponent,
        nzFooter: null,
        nzMaskClosable: false,
        nzClosable: false,
        nzWidth: 560,
      });

      let workflowObj: Partial<WorkflowContent> = {};
      if (inst.workflow_content?.length) {
        try {
          workflowObj = JSON.parse(inst.workflow_content) as Partial<WorkflowContent>;
        } catch {
          workflowObj = {};
        }
      }

      const emptyContent = this.emptyContentFromTemplate(workflowObj);

      let wid: number;
      if (existingWid != null) {
        wid = existingWid;
      } else {
        const created = await firstValueFrom(this.workflowPersist.createWorkflow(emptyContent, inst.workflow_name));
        wid = created.workflow.wid ?? 0;
        if (!wid) {
          throw new Error("Workflow creation did not return an id.");
        }
      }

      const userToken = AuthService.getAccessToken();
      if (!userToken) {
        throw new Error("You must be signed in to use AI workflow building.");
      }

      const suggestionKey = inst.suggestion_id ?? suggestionIdFallback;
      const agentParams = new HttpParams().set("source", "genesis").set("wid", String(wid));
      const genesisAgentHeaders = new HttpHeaders().set("x-genesis-source", "genesis");
      const agentResp = await firstValueFrom(
        this.http.post<{ id: string }>(
          "/api/agents",
          {
            modelType: inst.model,
            name: `Genesis-${suggestionKey}`,
            userToken,
            workflowId: wid,
            settings: {
              allowedOperatorTypes: inst.allowed_operator_types ?? [],
              maxSteps: 30,
            },
          },
          {
            params: agentParams,
            headers: genesisAgentHeaders,
          }
        )
      );
      const agentId = agentResp.id;
      if (!agentId) {
        throw new Error("Agent service did not return an agent id.");
      }

      const prompt = inst.agent_prompt!;

      // Kick off CU lookup/creation in parallel with the agent run — most of
      // the time there's already a running CU (cuid=1) so this resolves
      // instantly; if not we'll have spent the agent's 10–15 s spinning one
      // up rather than the user's wall clock.
      const cuPromise = this.ensureRunningComputingUnit().catch(err => {
        console.warn("[Genesis] ensureRunningComputingUnit failed", err);
        return null;
      });

      // IMPORTANT: wait for the agent to finish BEFORE navigating into the
      // workspace. Otherwise the workspace canvas mounts with an empty
      // workflow snapshot and its own auto-persist (workspace.component.ts:
      // registerAutoPersistWorkflow + ngOnDestroy) races with the agent's
      // persist and silently overwrites the agent's writes with the canvas's
      // empty in-memory state. Diagnosed against workflows 22+23 which were
      // both empty on the backend despite the agent completing 5–7 steps
      // with successful addOperator tool calls; persists from agent-service
      // worked in isolation but were clobbered as soon as a browser canvas
      // attached to the same wid.
      console.info("[Genesis] running agent, holding navigation until complete");
      await this.runAgentWebSocketSession(agentId, prompt);

      // Texera's Result panel only shows operators whose top-level `viewResult`
      // field is `true`. The agent's addOperator tool only writes
      // `operatorProperties` and never sets viewResult, so without this
      // post-process the workflow runs successfully but the Result tab is
      // empty/unclickable. Flip viewResult=true on every leaf operator (no
      // outgoing links — i.e. the operators whose output the user actually
      // wants to see). Must happen BEFORE navigate, same race-avoidance
      // reason as agent persists.
      await this.ensureViewResultOnLeafOperators(wid);

      this.buildProgress.addLine("Preparing computing unit…");
      const cu = await cuPromise;

      if (existingWid == null) {
        await this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]);
      } else {
        const refreshed = await firstValueFrom(this.workflowPersist.retrieveWorkflow(wid));
        this.workflowActionService.reloadWorkflow(refreshed);
      }

      modalRef.destroy();
      modalRef = undefined;

      await this.waitForWorkspaceCanvasReady();

      if (cu) {
        await this.attachComputingUnitAndExecute(wid, cu);
      } else {
        // No CU available — surface a hint and skip auto-run rather than
        // hitting the cuid=0 FK error.
        this.message.warning(
          "Workflow is ready, but no computing unit could be reached. Pick a unit and click Run."
        );
      }
    } catch (e: unknown) {
      const human =
        e instanceof Error
          ? e.message
          : "The AI agent could not finish the workflow. You can try again, or use template mode if the problem persists.";
      this.buildProgress.addLine(`Error: ${human}`);
      throw e;
    } finally {
      modalRef?.destroy();
    }
  }

  /**
   * Re-fetch the workflow and flip viewResult=true on any operator that is
   * a leaf (has no outgoing link). Persists back to the dashboard.
   *
   * This is a workaround for the fact that agent-service's `addOperator`
   * tool only writes `operatorProperties`, never top-level operator fields
   * like `viewResult`. Without this, the agent-built workflow runs but the
   * Result panel is empty because no operator opts in to result display.
   *
   * A **leaf** is any operator that never appears as a link *source* (no
   * outgoing edges). Multi-branch DAGs can have several leaves (e.g. Filter
   * table + BarChart); all of them are updated, not only a single topological
   * last node.
   */
  private async ensureViewResultOnLeafOperators(wid: number): Promise<void> {
    try {
      const workflow = await firstValueFrom(this.workflowPersist.retrieveWorkflow(wid));
      const content = workflow.content;
      const ops = (content?.operators ?? []) as Array<{ operatorID: string; operatorType: string; viewResult?: boolean | null }>;
      const links = (content?.links ?? []) as Array<{ source: { operatorID: string } }>;
      const opsWithOutgoing = new Set(links.map(l => l.source.operatorID));

      let changed = false;
      for (const op of ops) {
        const isLeaf = !opsWithOutgoing.has(op.operatorID);
        if (isLeaf && op.viewResult !== true) {
          op.viewResult = true;
          changed = true;
          console.info(
            "[Genesis] enabling viewResult on leaf operator",
            op.operatorID,
            op.operatorType
          );
        }
      }

      if (!changed) {
        return;
      }

      // Mutated in-place; persist back.
      await firstValueFrom(this.workflowPersist.persistWorkflow(workflow));
      console.info("[Genesis] persisted viewResult flips for wid=", wid);
    } catch (e) {
      console.warn("[Genesis] ensureViewResultOnLeafOperators failed", e);
    }
  }

  /**
   * Ensure there is a computing unit in the Running state we can attach to.
   * Prefers an existing one (no resource churn); falls back to creating a
   * local CU pointing at the dev-server's /wsapi proxy (the same shape the
   * default "My Computing Unit" uses). Polls up to CU_READY_TIMEOUT_MS.
   *
   * Returns the running CU, or null on timeout/failure (caller falls back to
   * "click Run yourself" UX rather than crashing the whole demo).
   */
  private async ensureRunningComputingUnit(): Promise<DashboardWorkflowComputingUnit | null> {
    const isRunning = (u: DashboardWorkflowComputingUnit): boolean => u.status === "Running";

    const fetchList = async (): Promise<DashboardWorkflowComputingUnit[]> => {
      try {
        return await firstValueFrom(this.computingUnitManaging.listComputingUnits());
      } catch (e) {
        console.warn("[Genesis] listComputingUnits failed", e);
        return [];
      }
    };

    const initial = await fetchList();
    const existing = initial.find(isRunning);
    if (existing) {
      console.info("[Genesis] reusing existing CU cuid=", existing.computingUnit.cuid);
      return existing;
    }

    // No running CU — create a local one with the same shape as the default
    // single-node deployment ("My Computing Unit" / type=local / uri=/wsapi).
    console.info("[Genesis] no running CU found, creating one");
    const wsapiUri = `${window.location.origin}/wsapi`;
    try {
      await firstValueFrom(this.computingUnitManaging.createLocalComputingUnit("Genesis Auto", wsapiUri));
    } catch (e) {
      console.warn("[Genesis] createLocalComputingUnit failed", e);
      // fall through — maybe a pending CU spawned by something else will appear
    }

    const deadline = Date.now() + CU_READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise(r => window.setTimeout(r, CU_POLL_INTERVAL_MS));
      const list = await fetchList();
      const running = list.find(isRunning);
      if (running) {
        console.info("[Genesis] new CU is running, cuid=", running.computingUnit.cuid);
        return running;
      }
    }
    console.warn("[Genesis] gave up waiting for any CU to become Running");
    return null;
  }

  /**
   * Wire up the websocket the execute call needs by selecting the CU through
   * ComputingUnitStatusService (which opens the workflow websocket as a side
   * effect), then wait briefly for that websocket to connect, then kick off
   * the execution. Matches the manual UX where the user picks a CU from the
   * power-button selector and then hits Run.
   */
  private async attachComputingUnitAndExecute(wid: number, cu: DashboardWorkflowComputingUnit): Promise<void> {
    const cuid = cu.computingUnit.cuid;
    console.info("[Genesis] attaching CU cuid=", cuid, "to wid=", wid);
    this.computingUnitStatus.selectComputingUnit(wid, cuid);

    const deadline = Date.now() + EXEC_WS_READY_TIMEOUT_MS;
    while (Date.now() < deadline) {
      if (this.workflowWebsocket.isConnected) {
        break;
      }
      await new Promise(r => window.setTimeout(r, 100));
    }
    if (!this.workflowWebsocket.isConnected) {
      console.warn("[Genesis] workflow websocket did not connect within timeout; trying execute anyway");
    } else {
      console.info("[Genesis] workflow websocket connected, running");
    }
    this.executeService.executeWorkflow("Genesis run");
  }

  private runAgentWebSocketSession(agentId: string, prompt: string): Promise<void> {
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${wsProtocol}//${window.location.host}/api/agents/${agentId}/react`;
    return new Promise((resolve, reject) => {
      let settled = false;
      const ws = new WebSocket(url);

      const finish = (fn: () => void): void => {
        if (settled) {
          return;
        }
        settled = true;
        window.clearTimeout(timer);
        try {
          ws.close(1000);
        } catch {
          /* ignore */
        }
        fn();
      };

      const timer = window.setTimeout(() => {
        finish(() => reject(new Error("Timed out waiting for the agent. Is agent-service running on port 3001?")));
      }, AGENT_WS_IDLE_MS);

      ws.onopen = () => {
        console.info("[Genesis] agent WS open", { agentId, promptLen: prompt.length });
        ws.send(JSON.stringify({ type: "message", content: prompt }));
      };

      ws.onerror = ev => {
        console.error("[Genesis] agent WS error", ev);
        finish(() =>
          reject(
            new Error(
              "Could not open a live connection to the agent. Confirm agent-service is running and the dev proxy maps /api/agents to port 3001."
            )
          )
        );
      };

      ws.onmessage = (ev: MessageEvent<string>) => {
        let msg: { type?: string; step?: Parameters<typeof formatGenesisAgentStepLine>[0]; error?: string };
        try {
          msg = JSON.parse(ev.data);
        } catch {
          return;
        }
        if (msg.type === "step" && msg.step) {
          const line = formatGenesisAgentStepLine(msg.step);
          const rawId = msg.step.stepId;
          const idx = rawId != null && Number.isFinite(Number(rawId)) ? Number(rawId) : null;
          this.buildProgress.setAgentStep(idx, line);
          this.buildProgress.addLine(line);
          return;
        }
        if (msg.type === "complete") {
          this.buildProgress.addLine("Complete — workflow updated.");
          finish(() => resolve());
          return;
        }
        if (msg.type === "error") {
          const errText = typeof msg.error === "string" ? msg.error : "Agent reported an error.";
          this.buildProgress.addLine(`Error: ${errText}`);
          finish(() => reject(new Error(errText)));
        }
      };

      ws.onclose = ev => {
        if (settled) {
          return;
        }
        finish(() =>
          reject(
            new Error(
              ev.reason?.length ? ev.reason : `WebSocket closed before completion (code ${ev.code}). Try template mode.`
            )
          )
        );
      };
    });
  }

  private async runTemplateMode(inst: InstantiateResponse, existingWid?: number): Promise<void> {
    let workflowObj: Partial<WorkflowContent> = {};
    try {
      workflowObj = JSON.parse(inst.workflow_content || "{}") as Partial<WorkflowContent>;
    } catch {
      workflowObj = {};
    }
    const emptyContent = this.emptyContentFromTemplate(workflowObj);

    let wid: number;
    if (existingWid != null) {
      wid = existingWid;
    } else {
      const created = await firstValueFrom(this.workflowPersist.createWorkflow(emptyContent, inst.workflow_name));
      wid = created.workflow.wid ?? 0;
      if (!wid) {
        throw new Error("Workflow creation did not return an id.");
      }
      await this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]);
    }

    await this.waitForWorkspaceCanvasReady();
    await this.growAnimator.grow(workflowObj, 500);
    this.executeService.executeWorkflow("Genesis run");
  }

  private formatUserVisibleError(e: unknown): string {
    if (e instanceof HttpErrorResponse) {
      const s = e.status;
      if (s === 502 || s === 504) {
        return "Backend not responding. Check bioflow-genesis-service on port 9099.";
      }
      if (s === 401 || s === 403) {
        return "Authentication failed. Try re-logging in.";
      }
      const body = e.error as { error?: string; message?: string } | string | null;
      if (body && typeof body === "object" && typeof body.error === "string") {
        return body.error;
      }
      if (body && typeof body === "object" && typeof body.message === "string") {
        return body.message;
      }
      if (typeof body === "string" && body.length > 0) {
        return body;
      }
      if (s > 0) {
        return `Request failed (HTTP ${s}).`;
      }
      return "Request failed (network or unknown error).";
    }
    if (e instanceof Error) {
      return e.message;
    }
    return String(e);
  }

  private emptyContentFromTemplate(template: Partial<WorkflowContent>): WorkflowContent {
    const settings = template.settings ?? {
      dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
      executionMode: this.config.env.defaultExecutionMode,
    };
    return {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings,
    };
  }

  private async addCsvScanOnlyViaGrow(upload: { file_path: string }): Promise<void> {
    const opId = `CSVFileScan-operator-genesis-${crypto.randomUUID()}`;
    const op: OperatorPredicate = {
      operatorID: opId,
      operatorType: "CSVFileScan",
      operatorVersion: "N/A",
      operatorProperties: {
        fileEncoding: "UTF_8",
        customDelimiter: ",",
        hasHeader: true,
        fileName: upload.file_path,
      },
      inputPorts: [],
      outputPorts: [
        {
          portID: "output-0",
          displayName: "",
          disallowMultiInputs: true,
          isDynamicPort: false,
        },
      ],
      showAdvanced: false,
      isDisabled: false,
      viewResult: true,
      dynamicInputPorts: false,
      dynamicOutputPorts: false,
    };
    await this.growAnimator.grow(
      {
        operators: [op],
        operatorPositions: { [opId]: { x: 200, y: 200 } },
        links: [],
        settings: {
          dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
          executionMode: this.config.env.defaultExecutionMode,
        },
      },
      0
    );
  }

  private async createWorkflowWithOnlyCsvScan(upload: {
    file_path: string;
  }): Promise<void> {
    const opId = `CSVFileScan-operator-genesis-${crypto.randomUUID()}`;
    const content: WorkflowContent = {
      operators: [
        {
          operatorID: opId,
          operatorType: "CSVFileScan",
          operatorVersion: "N/A",
          operatorProperties: {
            fileEncoding: "UTF_8",
            customDelimiter: ",",
            hasHeader: true,
            fileName: upload.file_path,
          },
          inputPorts: [],
          outputPorts: [
            {
              portID: "output-0",
              displayName: "",
              disallowMultiInputs: true,
              isDynamicPort: false,
            },
          ],
          showAdvanced: false,
          isDisabled: false,
          viewResult: true,
          dynamicInputPorts: false,
          dynamicOutputPorts: false,
        },
      ],
      operatorPositions: {
        [opId]: { x: 200, y: 200 },
      },
      links: [],
      commentBoxes: [],
      settings: {
        dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
        executionMode: this.config.env.defaultExecutionMode,
      },
    };

    const created = await firstValueFrom(this.workflowPersist.createWorkflow(content, "[Genesis] CSV import"));
    const wid = created.workflow.wid;
    if (!wid) {
      throw new Error("Workflow creation did not return an id.");
    }
    await this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]);
  }

  private waitForWorkspaceCanvasReady(timeoutMs = 45000): Promise<void> {
    const start = Date.now();
    return new Promise((resolve, reject) => {
      const tick = (): void => {
        const spinning = document.querySelector("texera-workspace .ant-spin-spinning");
        const editor = document.querySelector("texera-workflow-editor");
        if (editor && !spinning) {
          resolve();
          return;
        }
        if (Date.now() - start > timeoutMs) {
          reject(new Error("Canvas load timed out."));
          return;
        }
        window.requestAnimationFrame(tick);
      };
      tick();
    });
  }
}

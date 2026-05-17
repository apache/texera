/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { HttpErrorResponse } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Router } from "@angular/router";
import { firstValueFrom } from "rxjs";
import { NzMessageService } from "ng-zorro-antd/message";
import { DASHBOARD_USER_WORKSPACE } from "../../../../app-routing.constant";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { WorkflowContent } from "../../../../common/type/workflow";
import { GenesisCardChoice } from "../../../component/user/genesis/genesis-card.component";
import { GenesisCardOverlayService } from "./genesis-card-overlay.service";
import { AnalyzeResponse, GenesisService } from "./genesis.service";
import { WorkflowGrowAnimator } from "./workflow-grow.service";
import { WorkflowActionService } from "../../../../workspace/service/workflow-graph/model/workflow-action.service";
import { OperatorPredicate } from "../../../../workspace/types/workflow-common.interface";

@Injectable({
  providedIn: "root",
})
export class GenesisOrchestratorService {
  constructor(
    private genesis: GenesisService,
    private cardOverlay: GenesisCardOverlayService,
    private workflowPersist: WorkflowPersistService,
    private growAnimator: WorkflowGrowAnimator,
    private router: Router,
    private message: NzMessageService,
    private config: GuiConfigService,
    private workflowActionService: WorkflowActionService
  ) {}

  public async run(file: File): Promise<void> {
    const jwt = localStorage.getItem("access_token") ?? "";
    try {
      const { choice, upload: uploadResp, analyze: analyzeResp } =
        await this.cardOverlay.runUploadAnalyzeWithProgress(file, jwt, this.genesis);

      if (choice.kind === "cancel") {
        return;
      }

      if (choice.kind === "skip") {
        if (!uploadResp) {
          throw new Error("Upload incomplete — cannot create workflow.");
        }
        await this.createWorkflowWithOnlyCsvScan(uploadResp);
        return;
      }

      if (!uploadResp || !analyzeResp) {
        throw new Error("Analyze incomplete — try again.");
      }

      if (!uploadResp.upload_id) {
        throw new Error("Upload did not return upload_id — cannot build workflow.");
      }

      let cardIndex = 0;
      let freeText: string | undefined;
      if (choice.kind === "custom") {
        freeText = choice.text;
      } else {
        const idx = analyzeResp.suggestions?.findIndex(s => s.id === choice.suggestionId) ?? -1;
        if (idx < 0) {
          throw new Error("Selected suggestion not found.");
        }
        cardIndex = idx;
      }

      const progressTitle = this.buildProgressLabel(choice, analyzeResp);
      const operatorCount = this.operatorCountForChoice(choice, analyzeResp);
      const buildReq = {
        uploadId: uploadResp.upload_id,
        cardIndex,
        freeText,
        jwt,
      };
      const [built] = await Promise.all([
        firstValueFrom(this.genesis.build(buildReq)),
        this.cardOverlay.showBuildProgress({ title: progressTitle, operatorCount }),
      ]);

      await this.router.navigate([DASHBOARD_USER_WORKSPACE, built.wid]);
    } catch (e: unknown) {
      this.message.remove();
      this.message.error(`Genesis failed: ${this.formatUserVisibleError(e)}`);
    } finally {
      this.message.remove();
    }
  }

  /**
   * Same as {@link run}, but targets an **existing** workflow already open in the workspace
   * (blank canvas). Updates that workflow's content in place.
   */
  public async runIntoCurrentWorkflow(file: File, wid: number): Promise<void> {
    const jwt = localStorage.getItem("access_token") ?? "";
    try {
      const { choice, upload: uploadResp, analyze: analyzeResp } =
        await this.cardOverlay.runUploadAnalyzeWithProgress(file, jwt, this.genesis);

      if (choice.kind === "cancel") {
        return;
      }

      if (choice.kind === "skip") {
        if (!uploadResp) {
          throw new Error("Upload incomplete — cannot add operators.");
        }
        await this.addCsvScanOnlyViaGrow(uploadResp);
        return;
      }

      if (!uploadResp || !analyzeResp) {
        throw new Error("Analyze incomplete — try again.");
      }

      if (!uploadResp.upload_id) {
        throw new Error("Upload did not return upload_id — cannot build workflow.");
      }

      let cardIndex = 0;
      let freeText: string | undefined;
      if (choice.kind === "custom") {
        freeText = choice.text;
      } else {
        const idx = analyzeResp.suggestions?.findIndex(s => s.id === choice.suggestionId) ?? -1;
        if (idx < 0) {
          throw new Error("Selected suggestion not found.");
        }
        cardIndex = idx;
      }

      const progressTitle = this.buildProgressLabel(choice, analyzeResp);
      const operatorCount = this.operatorCountForChoice(choice, analyzeResp);
      const buildReq = {
        uploadId: uploadResp.upload_id,
        cardIndex,
        freeText,
        wid,
        jwt,
      };
      const [built] = await Promise.all([
        firstValueFrom(this.genesis.build(buildReq)),
        this.cardOverlay.showBuildProgress({ title: progressTitle, operatorCount }),
      ]);

      const refreshed = await firstValueFrom(this.workflowPersist.retrieveWorkflow(built.wid));
      this.workflowActionService.reloadWorkflow(refreshed);
    } catch (e: unknown) {
      this.message.remove();
      this.message.error(`Genesis failed: ${this.formatUserVisibleError(e)}`);
    } finally {
      this.message.remove();
    }
  }

  private buildProgressLabel(choice: GenesisCardChoice, analyze: AnalyzeResponse): string {
    if (choice.kind === "custom") {
      const t = choice.text.trim();
      return t.length > 0 ? t : "Custom analysis";
    }
    if (choice.kind === "suggestion") {
      const s = analyze.suggestions?.find(x => x.id === choice.suggestionId);
      const title = s?.title?.trim();
      return title && title.length > 0 ? title : "Workflow";
    }
    return "Workflow";
  }

  private operatorCountForChoice(choice: GenesisCardChoice, analyze: AnalyzeResponse): number {
    let task: string | undefined;
    if (choice.kind === "suggestion") {
      task = analyze.suggestions?.find(s => s.id === choice.suggestionId)?.task_type;
    }
    return this.operatorCountForTaskType(task);
  }

  private operatorCountForTaskType(taskType: string | undefined): number {
    const t = (taskType || "").toLowerCase();
    if (t === "exploration") {
      return 4;
    }
    if (t === "automl") {
      return 10;
    }
    return 6;
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

  private async createWorkflowWithOnlyCsvScan(upload: { file_path: string }): Promise<void> {
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
}

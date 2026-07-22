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
import { AppSettings } from "../../../common/app-setting";
import { Notebook, NotebookMigrationLLM } from "./migration-llm";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { catchError, firstValueFrom, map, Observable, of } from "rxjs";

interface LiteLLMModel {
  id: string;
  object: string;
  created: number;
  owned_by: string;
}

interface LiteLLMModelsResponse {
  data: LiteLLMModel[];
  object: string;
}

interface MappingContent {
  cell_to_operator: Record<string, string[]>;
  operator_to_cell: Record<string, string[]>;
}

interface StoreNotebookResponse {
  success: boolean;
  message: string;
}

@Injectable({
  providedIn: "root",
})
export class NotebookMigrationService {
  private mapping: { [key: string]: MappingContent } = {};

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService,
    private config: GuiConfigService,
    private workflowUtilService: WorkflowUtilService
  ) {}

  private get enabled(): boolean {
    return this.config.env.pythonNotebookMigrationEnabled;
  }

  public getAvailableModels(): Observable<{ name: string }[]> {
    if (!this.enabled) return of([]);
    return this.http.get<LiteLLMModelsResponse>(`${AppSettings.getApiEndpoint()}/models`).pipe(
      map(response =>
        response.data.map(model => ({
          name: model.id,
        }))
      ),
      catchError((err: unknown) => {
        console.error("Failed to fetch models", err);
        return of([]);
      })
    );
  }

  public async sendToAIGenerateWorkflow(notebookContent: Notebook, modelType: string) {
    if (!this.enabled) throw new Error("Notebook migration feature is disabled");
    const migrationLLM = this.createMigrationLLM();
    // initialize() defaults to the user's Texera JWT via AuthService.getAccessToken().
    // The outer try/finally guarantees close() runs for the whole lifecycle,
    // including a verifyConnection failure.
    try {
      migrationLLM.initialize(modelType);

      const isValid = await migrationLLM.verifyConnection();
      if (!isValid) {
        throw new Error("Unable to authenticate with or reach the LLM backend");
      }

      try {
        const result = await migrationLLM.convertNotebookToWorkflow(notebookContent);
        const parsedResult = JSON.parse(result);
        const workflowContent = parsedResult.workflowJSON;
        const mappingContent = parsedResult.workflowNotebookMapping;
        return { workflowContent, mappingContent };
      } catch (error) {
        console.error("Error converting notebook:", error);
        throw error;
      }
    } finally {
      migrationLLM.close();
    }
  }

  // Factory seam for the LLM client. Extracted so specs can override it to supply
  // a fake, keeping the real NotebookMigrationLLM (and its `ai` transport) out of
  // the test module graph. A new instance is created per conversion.
  protected createMigrationLLM(): NotebookMigrationLLM {
    return new NotebookMigrationLLM(this.config, this.workflowUtilService);
  }

  public async sendNotebookToJupyter(notebookData: Notebook) {
    if (!this.enabled) return 0;
    const jupyterAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/set-notebook`;

    const requestBody = {
      // Fixed filename is intentional for the v1 per-user-pod design: each user runs
      // their own notebook-migration-service and Jupyter, so a single notebook.ipynb
      // never collides. A shared multi-user (global) service would need per-user or
      // per-workflow keying here and for the backend's process-global jupyterIframeURL.
      notebookName: "notebook.ipynb",
      notebookData: notebookData,
    };

    const headers = new HttpHeaders({
      "Content-Type": "application/json",
    });

    try {
      await firstValueFrom(this.http.post(jupyterAPIUrl, requestBody, { headers }));
      this.notificationService.success("Notebook successfully sent to Jupyter");
      return 1;
    } catch (error) {
      console.error("Error sending notebook to pod: ", error);
      const message = error instanceof Error ? error.message : String(error);
      this.notificationService.error("Error sending notebook to Jupyter: " + message);
      return 0;
    }
  }

  public async getJupyterURL(): Promise<string | null> {
    if (!this.enabled) return null;
    try {
      const data = await firstValueFrom(
        this.http.get<{ success: boolean; url?: string }>(
          `${AppSettings.getApiEndpoint()}/notebook-migration/get-jupyter-url`
        )
      );

      if (!data.success || !data.url) {
        console.error("Jupyter server unavailable");
        return null;
      }

      return data.url;
    } catch (err) {
      console.error("Error fetching Jupyter URL:", err);
      return null;
    }
  }

  public async getJupyterIframeURL(): Promise<string | null> {
    if (!this.enabled) return null;
    try {
      const data = await firstValueFrom(
        this.http.get<{ success: boolean; url?: string }>(
          `${AppSettings.getApiEndpoint()}/notebook-migration/get-jupyter-iframe-url`
        )
      );

      if (!data.success || !data.url) {
        console.error("Jupyter server unavailable");
        return null;
      }

      return data.url;
    } catch (err) {
      console.error("Error fetching Jupyter iframe URL:", err);
      return null;
    }
  }

  public storeNotebookAndMapping(
    wid: number | undefined,
    vid: number = 1,
    mappingContent: any,
    notebookContent: any
  ): Observable<StoreNotebookResponse> {
    if (!this.enabled) {
      return of({ success: false, message: "Notebook migration feature is disabled" });
    }
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/store-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });

    const payload = {
      wid,
      vid,
      mapping: mappingContent,
      notebook: notebookContent,
    };

    return this.http.post<StoreNotebookResponse>(dbAPIUrl, payload, { headers });
  }

  public hasMapping(id: string): boolean {
    return id in this.mapping;
  }

  public getMapping(id: string): MappingContent | undefined {
    return this.mapping[id];
  }

  public setMapping(id: string, value: MappingContent): void {
    this.mapping[id] = value;
  }

  public deleteMapping(id: string): void {
    delete this.mapping[id];
  }
}

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
import { MigrationLanguage, Notebook, NotebookMigrationLLM, SourceConversion } from "./migration-llm";
import {
  buildFolderDocument,
  checkFolderByteSize,
  checkFolderDocument,
  checkFolderFileCount,
  FolderDocument,
  folderRelativePath,
  folderRootName,
  isExcludedPath,
  isMigratablePythonPath,
} from "./folder-assembly";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowContent } from "../../../common/type/workflow";
import { catchError, firstValueFrom, map, Observable, of } from "rxjs";
import { v4 as uuidv4 } from "uuid";

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

export interface MappingContent {
  cell_to_operator: Record<string, string[]>;
  operator_to_cell: Record<string, string[]>;
}

/**
 * What a conversion yields, whichever input produced it. `notebook` is the uploaded one for an
 * .ipynb and the LLM-derived one for a .py or a folder; either way it is what gets stored and
 * shown in the Jupyter panel.
 */
export interface GeneratedWorkflowContent {
  workflowContent: WorkflowContent;
  mappingContent: MappingContent;
  notebook: Notebook;
}

interface StoreNotebookResponse {
  success: boolean;
  message: string;
}

interface DeleteNotebookResponse {
  success: boolean;
  deleted?: number;
  message?: string;
}

// Single source of truth for the mapping cache key, shared with JupyterPanelService so it can't drift.
export function notebookMappingKey(wid: number | undefined): string {
  return "mapping_wid_" + wid;
}

// Per-workflow notebook filename so workflows don't overwrite each other's notebook.
// Falls back to the default when there's no wid.
export function notebookFileName(wid: number | undefined): string {
  return wid ? `notebook_${wid}.ipynb` : "notebook.ipynb";
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

  public async sendToAIGenerateWorkflow(
    notebookContent: Notebook,
    modelType: string,
    language: MigrationLanguage = "python"
  ): Promise<{ workflowContent: WorkflowContent; mappingContent: MappingContent }> {
    return this.withMigrationLLM(modelType, async migrationLLM => {
      try {
        const result = await migrationLLM.convertNotebookToWorkflow(notebookContent, language);
        const parsedResult = JSON.parse(result);
        const workflowContent = parsedResult.workflowJSON;
        const mappingContent = parsedResult.workflowNotebookMapping;
        return { workflowContent, mappingContent };
      } catch (error) {
        console.error("Error converting notebook:", error);
        throw error;
      }
    });
  }

  /**
   * Convert a script into a workflow.
   *
   * Returns a notebook alongside the workflow and mapping, which the notebook path does not:
   * a script has no cells, so the LLM reports which line ranges became which operator and the
   * notebook is derived from that. Callers store and display it as they would a user's own
   * .ipynb, so the Jupyter panel and cell highlighting work the same way for both inputs.
   */
  public async sendScriptToAIGenerateWorkflow(
    scriptSource: string,
    modelType: string,
    language: MigrationLanguage = "python"
  ): Promise<GeneratedWorkflowContent> {
    return this.generateFromSource(modelType, "script", migrationLLM =>
      migrationLLM.convertScriptToWorkflow(scriptSource, language)
    );
  }

  /**
   * Convert a folder of Python files, already assembled by parseFolder, into a workflow.
   *
   * Identical to the script path from the caller's side: the folder is one document by the time
   * it gets here, so the derived notebook and the mapping have the same shape and are stored the
   * same way.
   */
  public async sendFolderToAIGenerateWorkflow(
    folder: FolderDocument,
    modelType: string
  ): Promise<GeneratedWorkflowContent> {
    return this.generateFromSource(modelType, "folder", migrationLLM => migrationLLM.convertFolderToWorkflow(folder));
  }

  /**
   * Run one conversion of an input that arrives without cells, then unpack it into the shape
   * callers store. The script and folder paths differ only in which conversion they ask for.
   */
  private async generateFromSource(
    modelType: string,
    input: "script" | "folder",
    convert: (migrationLLM: NotebookMigrationLLM) => Promise<SourceConversion>
  ): Promise<GeneratedWorkflowContent> {
    return this.withMigrationLLM(modelType, async migrationLLM => {
      try {
        const conversion = await convert(migrationLLM);
        return {
          workflowContent: conversion.workflowJSON,
          mappingContent: conversion.workflowNotebookMapping,
          notebook: conversion.notebook,
        };
      } catch (error) {
        console.error(`Error converting ${input}:`, error);
        throw error;
      }
    });
  }

  /**
   * Run one conversion against a fresh LLM session: initialize, check the backend is reachable,
   * then convert. Shared by both input paths so the lifecycle cannot drift between them, and so
   * the finally guarantees close() for every exit, including a failed verifyConnection.
   */
  private async withMigrationLLM<T>(
    modelType: string,
    convert: (migrationLLM: NotebookMigrationLLM) => Promise<T>
  ): Promise<T> {
    if (!this.enabled) throw new Error("Notebook migration feature is disabled");
    const migrationLLM = this.createMigrationLLM();
    // initialize() defaults to the user's Texera JWT via AuthService.getAccessToken().
    try {
      migrationLLM.initialize(modelType);

      const isValid = await migrationLLM.verifyConnection();
      if (!isValid) {
        throw new Error("Unable to authenticate with or reach the LLM backend");
      }

      return await convert(migrationLLM);
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

  public async sendNotebookToJupyter(notebookData: Notebook, notebookName: string) {
    if (!this.enabled) return 0;
    const jupyterAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/set-notebook`;

    const requestBody = {
      notebookName: notebookName,
      notebookData: notebookData,
    };

    const headers = new HttpHeaders({
      "Content-Type": "application/json",
    });

    try {
      await firstValueFrom(this.http.post(jupyterAPIUrl, requestBody, { headers }));
      this.notificationService.success("Source code successfully sent to Jupyter");
      return 1;
    } catch (error) {
      console.error("Error sending notebook to pod: ", error);
      const message = error instanceof Error ? error.message : String(error);
      this.notificationService.error("Error sending source code to Jupyter: " + message);
      return 0;
    }
  }

  // Remove a workflow's notebook file from the Jupyter pod. Takes a concrete wid so it can
  // never fall back to the shared default filename and delete the wrong file; callers guard
  // out unsaved workflows before calling. Best effort by design: the database rows are the
  // source of truth for whether a workflow has a notebook, so a failure here is logged, not
  // surfaced, and nothing acts on the outcome.
  public async deleteNotebookForWorkflow(wid: number): Promise<void> {
    if (!this.enabled) return;
    if (!Number.isInteger(wid) || wid <= 0) return;
    const jupyterAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/delete-notebook`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });

    try {
      await firstValueFrom(this.http.post(jupyterAPIUrl, { notebookName: notebookFileName(wid) }, { headers }));
    } catch (error) {
      console.error("Error deleting notebook from pod: ", error);
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

  public async getJupyterIframeURL(notebookName?: string): Promise<string | null> {
    if (!this.enabled) return null;
    try {
      const url = `${AppSettings.getApiEndpoint()}/notebook-migration/get-jupyter-iframe-url`;
      // Send notebookName when given; otherwise the backend uses its default.
      const params: Record<string, string> = {};
      if (notebookName) {
        params["notebookName"] = notebookName;
      }
      const data = await firstValueFrom(this.http.get<{ success: boolean; url?: string }>(url, { params }));

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
    mappingContent: any,
    notebookContent: any
  ): Observable<StoreNotebookResponse> {
    if (!this.enabled) {
      return of({ success: false, message: "Notebook migration feature is disabled" });
    }
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/store-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });

    // The mapping's version id (vid) is resolved server-side from the workflow's
    // latest version to anchor its FK, so no vid is sent from here.
    const payload = {
      wid,
      mapping: mappingContent,
      notebook: notebookContent,
    };

    return this.http.post<StoreNotebookResponse>(dbAPIUrl, payload, { headers });
  }

  // Delete the stored notebook and its mapping for a workflow. The backend's
  // notebook -> workflow_notebook_mapping FK is ON DELETE CASCADE, and wid alone
  // identifies the notebook, so only wid is sent. `deleted` is 1 when a notebook
  // was removed, 0 when nothing was stored.
  public deleteNotebookAndMapping(wid: number | undefined): Observable<DeleteNotebookResponse> {
    if (!this.enabled) {
      return of({ success: false, message: "Notebook migration feature is disabled" });
    }
    const dbAPIUrl = `${AppSettings.getApiEndpoint()}/notebook-migration/delete-notebook-and-mapping`;
    const headers = new HttpHeaders({ "Content-Type": "application/json" });

    const payload = { wid };

    return this.http.post<DeleteNotebookResponse>(dbAPIUrl, payload, { headers });
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

  // Reads a file as text. Uses FileReader for the same reason parseAndTagNotebook does: jsdom
  // (the test environment) does not implement Blob/File.text(). `description` names the input in
  // the read-failure message, since the caller knows what the user picked and this does not.
  private readFileAsText(file: File, description: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error(`Failed to read the ${description}.`));
      reader.onload = () => {
        if (typeof reader.result !== "string") {
          reject(new Error("File content is not a valid string."));
          return;
        }
        resolve(reader.result);
      };
      reader.readAsText(file);
    });
  }

  // Reads a .py file as text. Rejects on a file with nothing in it: an empty script would
  // otherwise cost a full LLM round trip to produce an empty workflow.
  public async parseScriptFile(file: File): Promise<string> {
    const source = await this.readFileAsText(file, "Python file");
    if (source.trim() === "") {
      throw new Error("The Python file is empty.");
    }
    return source;
  }

  /**
   * Read a directory selection into the one document a folder conversion is run against.
   *
   * Keeps only project Python source, reads it, and assembles it in a fixed order. The file-count
   * and byte checks run before anything is read, so an over-broad or oversized selection fails at
   * once rather than after thousands of reads or one enormous one.
   */
  public async parseFolder(files: readonly File[]): Promise<FolderDocument> {
    const picked = files.map(file => ({ file, path: folderRelativePath(file) }));
    const selected = picked.filter(entry => isMigratablePythonPath(entry.path));
    this.refuse(checkFolderFileCount(selected.length));
    this.refuse(checkFolderByteSize(selected.reduce((sum, entry) => sum + entry.file.size, 0)));

    const sources = await Promise.all(
      selected.map(async entry => ({
        // Named so a read failure says which file failed; the caller shows this message as-is.
        path: entry.path,
        source: await this.readFileAsText(entry.file, `file ${entry.path}`),
      }))
    );
    const folder = buildFolderDocument(sources, {
      rootName: folderRootName(files[0]?.webkitRelativePath ?? "") ?? "project",
      // Named in the layout but never read. Caches and hidden directories stay out entirely.
      otherPaths: picked
        .filter(entry => !isMigratablePythonPath(entry.path) && !isExcludedPath(entry.path))
        .map(entry => entry.path),
    });

    // Run against what the document actually holds: blank files are dropped during assembly,
    // and the character cap needs the assembled size.
    this.refuse(checkFolderDocument(folder));
    return folder;
  }

  // Turns a cap check's message into the rejection the caller shows the user as-is.
  private refuse(message: string | null): void {
    if (message !== null) {
      throw new Error(message);
    }
  }

  // Reads and parses an .ipynb file, then tags each cell with a uuid (the mapping keys off these).
  // Rejects on a read error, invalid JSON, or a missing cells array. Uses FileReader rather than
  // file.text() because jsdom (the test environment) does not implement Blob/File.text().
  public parseAndTagNotebook(file: File): Promise<Notebook> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error("Failed to read the notebook file."));
      reader.onload = () => {
        try {
          if (typeof reader.result !== "string") {
            throw new Error("File content is not a valid string.");
          }
          const notebook = JSON.parse(reader.result) as Notebook;
          if (!notebook || !Array.isArray(notebook.cells)) {
            throw new Error("Invalid notebook structure.");
          }
          for (const cell of notebook.cells) {
            if (!cell.metadata) {
              cell.metadata = {};
            }
            cell.metadata.uuid = uuidv4();
          }
          resolve(notebook);
        } catch (error) {
          reject(error);
        }
      };
      reader.readAsText(file);
    });
  }
}

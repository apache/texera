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
import { forkJoin, from, Observable, of, throwError } from "rxjs";
import { catchError, map, switchMap, tap } from "rxjs/operators";
import { FileSaverService } from "../file/file-saver.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetService } from "../dataset/dataset.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import * as JSZip from "jszip";
import { Workflow } from "../../../../common/type/workflow";
import { HttpClient, HttpResponse } from "@angular/common/http";
import { WORKFLOW_EXECUTIONS_API_BASE_URL } from "../workflow-executions/workflow-executions.service";
import { DashboardWorkflowComputingUnit } from "../../../../workspace/types/workflow-computing-unit";
import { TOKEN_KEY } from "../../../../common/service/user/auth.service";

var contentDisposition = require("content-disposition");

export const EXPORT_BASE_URL = "result/export";
const IFRAME_TIMEOUT_MS = 10000;
export const DOWNLOADABILITY_BASE_URL = "result/downloadability";

interface DownloadableItem {
  blob: Blob;
  fileName: string;
}

export interface ExportWorkflowJsonResponse {
  status: string;
  message: string;
}

export interface WorkflowResultDownloadabilityResponse {
  [operatorId: string]: string[]; // operatorId -> array of dataset labels blocking export
}

@Injectable({
  providedIn: "root",
})
export class DownloadService {
  constructor(
    private fileSaverService: FileSaverService,
    private notificationService: NotificationService,
    private datasetService: DatasetService,
    private workflowPersistService: WorkflowPersistService,
    private http: HttpClient
  ) {}

  downloadWorkflow(id: number, name: string): Observable<DownloadableItem> {
    return this.workflowPersistService.retrieveWorkflow(id).pipe(
      map(({ content }) => {
        const workflowJson = JSON.stringify(content, null, 2);
        const fileName = `${name}.json`;
        const blob = new Blob([workflowJson], { type: "text/plain;charset=utf-8" });
        return { blob, fileName };
      }),
      tap(this.saveFile.bind(this))
    );
  }

  downloadDataset(id: number, name: string): Observable<Blob> {
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetVersionZip(id),
      `${name}.zip`,
      "Starting to download the latest version of the dataset as ZIP",
      "The latest version of the dataset has been downloaded as ZIP",
      "Error downloading the latest version of the dataset as ZIP"
    );
  }

  downloadDatasetVersion(
    datasetId: number,
    datasetVersionId: number,
    datasetName: string,
    versionName: string
  ): Observable<Blob> {
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetVersionZip(datasetId, datasetVersionId),
      `${datasetName}-${versionName}.zip`,
      `Starting to download version ${versionName} as ZIP`,
      `Version ${versionName} has been downloaded as ZIP`,
      `Error downloading version '${versionName}' as ZIP`
    );
  }

  downloadSingleFile(filePath: string, isLogin: boolean = true): Observable<Blob> {
    const DEFAULT_FILE_NAME = "download";
    const fileName = filePath.split("/").pop() || DEFAULT_FILE_NAME;
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetVersionSingleFile(filePath, isLogin),
      fileName,
      `Starting to download file ${filePath}`,
      `File ${filePath} has been downloaded`,
      `Error downloading file '${filePath}'`
    );
  }

  downloadWorkflowsAsZip(workflowEntries: Array<{ id: number; name: string }>): Observable<Blob> {
    return this.downloadWithNotification(
      () => this.createWorkflowsZip(workflowEntries),
      `workflowExports-${new Date().toISOString()}.zip`,
      "Starting to download workflows as ZIP",
      "Workflows have been downloaded as ZIP",
      "Error downloading workflows as ZIP"
    );
  }

  /**
   * Retrieves workflow result downloadability information from the backend.
   * Returns a map of operator IDs to arrays of dataset labels that block their export.
   *
   * @param workflowId The workflow ID to check
   * @returns Observable of downloadability information
   */
  public getWorkflowResultDownloadability(workflowId: number): Observable<WorkflowResultDownloadabilityResponse> {
    const urlPath = `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${workflowId}/${DOWNLOADABILITY_BASE_URL}`;
    return this.http.get<WorkflowResultDownloadabilityResponse>(urlPath);
  }

  /**
   * Export the workflow result. If destination = "local", the server returns a BLOB (file).
   * Otherwise, it returns JSON with a status message.
   */
  public exportWorkflowResult(
    exportType: string,
    workflowId: number,
    workflowName: string,
    operators: {
      id: string;
      outputType: string;
    }[],
    datasetIds: number[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    unit: DashboardWorkflowComputingUnit // computing unit for cluster setting
  ): Observable<HttpResponse<Blob> | HttpResponse<ExportWorkflowJsonResponse>> {
    const computingUnitId = unit.computingUnit.cuid;
    const requestBody = {
      exportType,
      workflowId,
      workflowName,
      operators,
      datasetIds,
      rowIndex,
      columnIndex,
      filename,
      computingUnitId,
    };

    const urlPath =
      unit && unit.computingUnit.type == "kubernetes" && unit.computingUnit?.cuid
        ? `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/dataset?cuid=${unit.computingUnit.cuid}`
        : `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/dataset`;

    return this.http.post<ExportWorkflowJsonResponse>(urlPath, requestBody, {
      responseType: "json",
      observe: "response",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
    });
  }

  /**
   * Export the workflow result to local filesystem. The export is handled by the browser.
   */
  public exportWorkflowResultToLocal(
    exportType: string,
    workflowId: number,
    workflowName: string,
    operators: {
      id: string;
      outputType: string;
    }[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    unit: DashboardWorkflowComputingUnit // computing unit for cluster setting
  ): void {
    const computingUnitId = unit.computingUnit.cuid;
    const datasetIds: number[] = [];
    const requestBody = {
      exportType,
      workflowId,
      workflowName,
      operators,
      datasetIds,
      rowIndex,
      columnIndex,
      filename,
      computingUnitId,
    };
    const token = localStorage.getItem(TOKEN_KEY) ?? "";

    const urlPath =
      unit && unit.computingUnit.type == "kubernetes" && unit.computingUnit?.cuid
        ? `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/local?cuid=${unit.computingUnit.cuid}`
        : `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}/local`;

    const iframe = document.createElement("iframe");
    iframe.name = "download-iframe";
    iframe.style.display = "none";
    document.body.appendChild(iframe);

    const form = document.createElement("form");
    form.method = "POST";
    form.action = urlPath;
    form.target = "download-iframe";
    form.enctype = "application/x-www-form-urlencoded";
    form.style.display = "none";

    const requestInput = document.createElement("input");
    requestInput.type = "hidden";
    requestInput.name = "request";
    requestInput.value = JSON.stringify(requestBody);
    form.appendChild(requestInput);

    const tokenInput = document.createElement("input");
    tokenInput.type = "hidden";
    tokenInput.name = "token";
    tokenInput.value = token;
    form.appendChild(tokenInput);

    document.body.appendChild(form);
    form.submit();

    setTimeout(() => {
      document.body.removeChild(form);
      document.body.removeChild(iframe);
    }, IFRAME_TIMEOUT_MS);
  }

  downloadOperatorsResult(
    resultObservables: Observable<{ filename: string; blob: Blob }[]>[],
    workflow: Workflow
  ): Observable<Blob> {
    return forkJoin(resultObservables).pipe(
      map(filesArray => filesArray.flat()),
      switchMap(files => {
        if (files.length === 0) {
          return throwError(() => new Error("No files to download"));
        } else if (files.length === 1) {
          // Single file, download directly
          return this.downloadWithNotification(
            () => of(files[0].blob),
            files[0].filename,
            "Starting to download operator result",
            "Operator result has been downloaded",
            "Error downloading operator result"
          );
        } else {
          // Multiple files, create a zip
          return this.downloadWithNotification(
            () => this.createZip(files),
            `results_${workflow.wid}_${workflow.name}.zip`,
            "Starting to download operator results as ZIP",
            "Operator results have been downloaded as ZIP",
            "Error downloading operator results as ZIP"
          );
        }
      })
    );
  }

  private createWorkflowsZip(workflowEntries: Array<{ id: number; name: string }>): Observable<Blob> {
    const zip = new JSZip();
    const downloadObservables = workflowEntries.map(entry =>
      this.downloadWorkflow(entry.id, entry.name).pipe(
        tap(({ blob, fileName }) => {
          zip.file(this.nameWorkflow(fileName, zip), blob);
        })
      )
    );

    return forkJoin(downloadObservables).pipe(switchMap(() => zip.generateAsync({ type: "blob" })));
  }

  private nameWorkflow(name: string, zip: JSZip): string {
    let count = 0;
    let copyName = name;
    while (zip.file(copyName)) {
      copyName = `${name.replace(".json", "")}-${++count}.json`;
    }
    return copyName;
  }

  private downloadWithNotification(
    retrieveFunction: () => Observable<Blob>,
    fileName: string,
    startMessage: string,
    successMessage: string,
    errorMessage: string
  ): Observable<Blob> {
    this.notificationService.info(startMessage);
    return retrieveFunction().pipe(
      tap(blob => {
        this.saveFile({ blob, fileName });
        this.notificationService.success(successMessage);
      }),
      catchError((error: unknown) => {
        this.notificationService.error(errorMessage);
        return throwError(() => error);
      })
    );
  }

  private saveFile({ blob, fileName }: DownloadableItem): void {
    this.fileSaverService.saveAs(blob, fileName);
  }

  private createZip(files: { filename: string; blob: Blob }[]): Observable<Blob> {
    const zip = new JSZip();
    files.forEach(file => {
      zip.file(file.filename, file.blob);
    });
    return from(zip.generateAsync({ type: "blob" }));
  }
}

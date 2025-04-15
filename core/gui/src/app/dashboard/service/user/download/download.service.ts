import { Injectable } from "@angular/core";
import { Observable, throwError, of, forkJoin, from } from "rxjs";
import { map, tap, catchError, switchMap } from "rxjs/operators";
import { FileSaverService } from "../file/file-saver.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DATASET_BASE_URL, DatasetService } from "../dataset/dataset.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import * as JSZip from "jszip";
import { Workflow } from "../../../../common/type/workflow";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient, HttpResponse } from "@angular/common/http";
import { WORKFLOW_EXECUTIONS_API_BASE_URL } from "../workflow-executions/workflow-executions.service";
import { DashboardWorkflowComputingUnit } from "../../../../workspace/types/workflow-computing-unit";
var contentDisposition = require("content-disposition");

export const EXPORT_BASE_URL = "result/export";

interface DownloadableItem {
  blob: Blob;
  fileName: string;
}

export interface ExportWorkflowJsonResponse {
  status: string;
  message: string;
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
      map(({ wid, creationTime, lastModifiedTime, ...workflowCopy }) => {
        const workflowJson = JSON.stringify({ ...workflowCopy, readonly: false });
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

  downloadSingleFile(filePath: string): Observable<Blob> {
    const DEFAULT_FILE_NAME = "download";
    const fileName = filePath.split("/").pop() || DEFAULT_FILE_NAME;
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetVersionSingleFile(filePath),
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
   * Export the workflow result. If destination = "local", the server returns a BLOB (file).
   * Otherwise, it returns JSON with a status message.
   */
  public exportWorkflowResult(
    exportType: string,
    workflowId: number,
    workflowName: string,
    operatorIds: string[],
    datasetIds: number[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    destination: "local" | "dataset" = "dataset", // "local" or "dataset" => default to "dataset"
    unit: DashboardWorkflowComputingUnit | null = null // computing unit for cluster setting
  ): Observable<HttpResponse<Blob> | HttpResponse<ExportWorkflowJsonResponse>> {
    const requestBody = {
      exportType,
      workflowId,
      workflowName,
      operatorIds,
      datasetIds,
      rowIndex,
      columnIndex,
      filename,
      destination,
    };
    console.log("received cui from exportWorkflowResult", unit);
    const urlPath =
      unit && unit.computingUnit?.cuid
        ? `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}?cuid=${unit.computingUnit.cuid}`
        : `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${EXPORT_BASE_URL}`;
    if (destination === "local") {
      return this.http.post(urlPath, requestBody, {
        responseType: "blob",
        observe: "response",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/octet-stream",
        },
      });
    } else {
      // dataset => return JSON
      return this.http.post<ExportWorkflowJsonResponse>(urlPath, requestBody, {
        responseType: "json",
        observe: "response",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
      });
    }
  }

  /**
   * Utility function to download a file from the server from blob object.
   */
  public saveBlobFile(response: any, defaultFileName: string): void {
    // If the server sets "Content-Disposition: attachment; filename="someName.csv"" header,
    // we can parse that out. Otherwise just use defaultFileName.
    const dispositionHeader = response.headers.get("Content-Disposition");
    let fileName = defaultFileName;
    if (dispositionHeader) {
      const parsed = contentDisposition.parse(dispositionHeader);
      fileName = parsed.parameters.filename || defaultFileName;
    }

    const blob = response.body; // the actual file data
    this.fileSaverService.saveAs(blob, fileName);
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

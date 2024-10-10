import { Injectable } from "@angular/core";
import { Observable, throwError } from "rxjs";
import { map, tap, catchError } from "rxjs/operators";
import { FileSaverService } from "../file/file-saver.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetService } from "../dataset/dataset.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";

@Injectable({
  providedIn: "root",
})
export class DownloadService {
  constructor(
    private fileSaverService: FileSaverService,
    private notificationService: NotificationService,
    private datasetService: DatasetService,
    private workflowPersistService: WorkflowPersistService
  ) {}

  downloadWorkflow(id: number, name: string): Observable<{ blob: Blob; fileName: string }> {
    return this.workflowPersistService.retrieveWorkflow(id).pipe(
      map(({ wid, creationTime, lastModifiedTime, ...workflowCopy }) => {
        const workflowJson = JSON.stringify({ ...workflowCopy, readonly: false });
        const fileName = `${name}.json`;
        const blob = new Blob([workflowJson], { type: "text/plain;charset=utf-8" });
        return { blob, fileName };
      }),
      tap(({ blob, fileName }) => this.fileSaverService.saveAs(blob, fileName))
    );
  }

  downloadDataset(id: number, name: string): Observable<Blob> {
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetZip({ did: id }),
      `${name}.zip`,
      "Starting to download the latest version of the dataset as ZIP",
      "The latest version of the dataset has been downloaded as ZIP",
      "Error downloading the latest version of the dataset as ZIP"
    );
  }

  downloadDatasetVersion(versionPath: string, datasetName: string, versionName: string): Observable<Blob> {
    return this.downloadWithNotification(
      () => this.datasetService.retrieveDatasetZip({ path: versionPath }),
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
        this.fileSaverService.saveAs(blob, fileName);
        this.notificationService.info(successMessage);
      }),
      catchError((error: unknown) => {
        this.notificationService.error(errorMessage);
        return throwError(() => error);
      })
    );
  }
}

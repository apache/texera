import { Injectable } from "@angular/core";
import { HttpClient, HttpParams } from "@angular/common/http";
import {catchError, map, switchMap, tap} from "rxjs/operators";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { AppSettings } from "../../../../common/app-setting";
import {forkJoin, from, Observable, throwError} from "rxjs";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { FileUploadItem } from "../../../type/dashboard-file.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import {S3Client} from "@aws-sdk/client-s3";

export const DATASET_BASE_URL = "dataset";
export const DATASET_CREATE_URL = DATASET_BASE_URL + "/create";
export const DATASET_UPDATE_BASE_URL = DATASET_BASE_URL + "/update";
export const DATASET_UPDATE_NAME_URL = DATASET_UPDATE_BASE_URL + "/name";
export const DATASET_UPDATE_DESCRIPTION_URL = DATASET_UPDATE_BASE_URL + "/description";
export const DATASET_UPDATE_PUBLICITY_URL = "update/publicity";
export const DATASET_LIST_URL = DATASET_BASE_URL + "/list";
export const DATASET_SEARCH_URL = DATASET_BASE_URL + "/search";
export const DATASET_DELETE_URL = DATASET_BASE_URL + "/delete";

export const DATASET_VERSION_BASE_URL = "version";
export const DATASET_VERSION_RETRIEVE_LIST_URL = DATASET_VERSION_BASE_URL + "/list";
export const DATASET_VERSION_LATEST_URL = DATASET_VERSION_BASE_URL + "/latest";
export const DEFAULT_DATASET_NAME = "Untitled dataset";
export const DATASET_PUBLIC_VERSION_BASE_URL = "publicVersion";
export const DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL = DATASET_PUBLIC_VERSION_BASE_URL + "/list";
export const DATASET_GET_OWNERS_URL = DATASET_BASE_URL + "/datasetUserAccess";

const MULTIPART_UPLOAD_PART_SIZE_MB = 50 * 1024 * 1024; // 50MB per part

@Injectable({
  providedIn: "root",
})
export class DatasetService {
  constructor(private http: HttpClient) {
  }

  public createDataset(dataset: Dataset): Observable<DashboardDataset> {
    const formData = new FormData();
    formData.append("datasetName", dataset.name);
    formData.append("datasetDescription", dataset.description);
    formData.append("isDatasetPublic", dataset.isPublic ? "true" : "false");

    return this.http.post<DashboardDataset>(`${AppSettings.getApiEndpoint()}/${DATASET_CREATE_URL}`, formData);
  }

  public getDataset(did: number, isLogin: boolean = true): Observable<DashboardDataset> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/public/${did}`;
    return this.http.get<DashboardDataset>(apiUrl);
  }

  /**
   * Retrieves a single file from a dataset version using a pre-signed URL.
   * @param filePath Relative file path within the dataset.
   * @returns Observable<Blob>
   */
  public retrieveDatasetVersionSingleFile(filePath: string): Observable<Blob> {
    return this.http
      .get<{
        presignedUrl: string;
      }>(`${AppSettings.getApiEndpoint()}/dataset/presign?key=${encodeURIComponent(filePath)}`)
      .pipe(
        switchMap(({ presignedUrl }) => {
          return this.http.get(presignedUrl, { responseType: "blob" });
        })
      );
  }

  /**
   * Retrieves a zip file of a dataset or a specific path within a dataset.
   * @param options An object containing optional parameters:
   *   - path: A string representing a specific file or directory path within the dataset
   *   - did: A number representing the dataset ID
   * @returns An Observable that emits a Blob containing the zip file
   */
  public retrieveDatasetZip(options: { did: number; dvid?: number }): Observable<Blob> {
    let params = new HttpParams();
    params = params.set("did", options.did.toString());
    if (options.dvid) {
      params = params.set("dvid", options.dvid.toString());
    }

    return this.http.get(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/version-zip`, {
      params,
      responseType: "blob",
    });
  }

  public retrieveAccessibleDatasets(): Observable<DashboardDataset[]> {
    return this.http.get<DashboardDataset[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}`);
  }
  public createDatasetVersion(did: number, newVersion: string): Observable<DatasetVersion> {
    return this.http
      .post<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/version/create`, newVersion, {
        headers: { "Content-Type": "text/plain" },
      })
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * Handles multipart upload for large files using RxJS.
   * @param did Dataset ID
   * @param filePath Path of the file within the dataset
   * @param file File object to be uploaded
   */
  public multipartUpload(did: number, filePath: string, file: File): Observable<Response> {
    const partCount = Math.ceil(file.size / MULTIPART_UPLOAD_PART_SIZE_MB);

    return this.initiateMultipartUpload(did, filePath, partCount).pipe(
      switchMap(initiateResponse => {
        const uploadId = initiateResponse.uploadId;
        if (!uploadId) {
          return throwError(() => new Error("Failed to initiate multipart upload"));
        }

        console.log(`Started multipart upload for ${filePath} with UploadId: ${uploadId}`);

        // Array to store part numbers and ETags
        const uploadedParts: { PartNumber: number; ETag: string }[] = [];

        const uploadObservables = initiateResponse.presignedUrls.map((url, index) => {
          const start = index * MULTIPART_UPLOAD_PART_SIZE_MB;
          const end = Math.min(start + MULTIPART_UPLOAD_PART_SIZE_MB, file.size);
          const chunk = file.slice(start, end);

          return from(fetch(url, { method: "PUT", body: chunk })).pipe(
            switchMap(response => {
              if (!response.ok) {
                return throwError(() => new Error(`Failed to upload part ${index + 1}`));
              }
              const etag = response.headers.get("ETag")?.replace(/"/g, ""); // Extract and clean ETag
              if (!etag) {
                return throwError(() => new Error(`Missing ETag for part ${index + 1}`));
              }

              uploadedParts.push({ PartNumber: index + 1, ETag: etag });
              console.log(`Uploaded part ${index + 1} of ${partCount}, ETag: ${etag}`);
              return from(Promise.resolve());
            })
          );
        });

        return forkJoin(uploadObservables).pipe(
          switchMap(() => this.finalizeMultipartUpload(did, filePath, uploadId, uploadedParts, initiateResponse.physicalAddress, false)),
          tap(() => console.log(`Multipart upload for ${filePath} completed successfully!`)),
          catchError(error => {
            console.error(`Multipart upload failed for ${filePath}`, error);
            return this.finalizeMultipartUpload(did, filePath, uploadId, uploadedParts, initiateResponse.physicalAddress, true).pipe(
              tap(() => console.error(`Upload aborted for ${filePath}`)),
              switchMap(() => throwError(() => error))
            );
          })
        );
      })
    );
  }

  /**
   * Initiates a multipart upload and retrieves presigned URLs for each part.
   * @param did Dataset ID
   * @param filePath File path within the dataset
   * @param numParts Number of parts for the multipart upload
   */
  private initiateMultipartUpload(did: number, filePath: string, numParts: number): Observable<{ uploadId: string; presignedUrls: string[]; physicalAddress: string }> {
    const params = new HttpParams()
      .set("type", "init")
      .set("key", encodeURIComponent(filePath))
      .set("numParts", numParts.toString());

    return this.http.post<{ uploadId: string; presignedUrls: string[]; physicalAddress: string }>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/multipart-upload`,
      {},
      { params }
    );
  }

  /**
   * Completes or aborts a multipart upload, sending part numbers and ETags to the backend.
   */
  private finalizeMultipartUpload(
    did: number,
    filePath: string,
    uploadId: string,
    parts: { PartNumber: number; ETag: string }[],
    physicalAddress: string,
    isAbort: boolean
  ): Observable<Response> {
    const params = new HttpParams()
      .set("type", isAbort ? "abort" : "finish")
      .set("key", encodeURIComponent(filePath))
      .set("uploadId", uploadId);

    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/dataset/${did}/multipart-upload`,
      { parts, physicalAddress },
      { params }
    );
  }

  /**
   * Resets a dataset file difference in LakeFS.
   * @param did Dataset ID
   * @param filePath File path to reset
   */
  public resetDatasetFileDiff(did: number, filePath: string): Observable<Response> {
    const apiUrl = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`;
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.put<Response>(apiUrl, {}, { params }).pipe(
      tap(() => console.log(`Reset file diff for dataset ${did}, file: ${filePath}`)),
      catchError(error => {
        console.error(`Failed to reset file diff for ${filePath}:`, error);
        return throwError(() => error);
      })
    );
  }

  /**
   * Deletes a dataset file from LakeFS.
   * @param did Dataset ID
   * @param filePath File path to delete
   */
  public deleteDatasetFile(did: number, filePath: string): Observable<Response> {
    const apiUrl = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/file`;
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.delete<Response>(apiUrl, { params }).pipe(
      tap(() => console.log(`Deleted file from dataset ${did}, file: ${filePath}`)),
      catchError(error => {
        console.error(`Failed to delete file ${filePath}:`, error);
        return throwError(() => error);
      })
    );
  }

  /**
   * Retrieves the list of uncommitted dataset changes (diffs).
   * @param did Dataset ID
   */
  public getDatasetDiff(did: number): Observable<DatasetStagedObject[]> {
    return this.http.get<DatasetStagedObject[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`);
  }

  /**
   * retrieve a list of versions of a dataset. The list is sorted so that the latest versions are at front.
   * @param did
   * @param isLogin
   */
  public retrieveDatasetVersionList(did: number, isLogin: boolean = true): Observable<DatasetVersion[]> {
    const apiEndPont = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_RETRIEVE_LIST_URL}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL}`;
    return this.http.get<DatasetVersion[]>(apiEndPont);
  }

  /**
   * retrieve the latest version of a dataset.
   * @param did
   */
  public retrieveDatasetLatestVersion(did: number): Observable<DatasetVersion> {
    return this.http
      .get<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_LATEST_URL}`)
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * retrieve a list of nodes that represent the files in the version
   * @param did
   * @param dvid
   * @param isLogin
   */
  public retrieveDatasetVersionFileTree(
    did: number,
    dvid: number,
    isLogin: boolean = true
  ): Observable<{ fileNodes: DatasetFileNode[]; size: number }> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_BASE_URL}/${dvid}/rootFileNodes`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_BASE_URL}/${dvid}/rootFileNodes`;
    return this.http.get<{ fileNodes: DatasetFileNode[]; size: number }>(apiUrl);
  }

  public deleteDatasets(dids: number[]): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_DELETE_URL}`, {
      dids: dids,
    });
  }

  public updateDatasetName(did: number, name: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_NAME_URL}`, {
      did: did,
      name: name,
    });
  }

  public updateDatasetDescription(did: number, description: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_DESCRIPTION_URL}`, {
      did: did,
      description: description,
    });
  }

  public updateDatasetPublicity(did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_UPDATE_PUBLICITY_URL}`,
      {}
    );
  }

  public getDatasetOwners(did: number): Observable<number[]> {
    return this.http.get<number[]>(`${AppSettings.getApiEndpoint()}/${DATASET_GET_OWNERS_URL}?did=${did}`);
  }
}

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
import { HttpClient, HttpParams, HttpResponse } from "@angular/common/http";
import { catchError, map, mergeMap, switchMap, tap, toArray } from "rxjs/operators";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { AppSettings } from "../../../../common/app-setting";
import { EMPTY, forkJoin, from, Observable, of, throwError } from "rxjs";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { GuiConfigService } from "../../../../common/service/gui-config.service";

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
export const DATASET_GET_OWNERS_URL = DATASET_BASE_URL + "/user-dataset-owners";

export interface MultipartUploadProgress {
  filePath: string;
  percentage: number;
  status: "initializing" | "uploading" | "finished" | "aborted";
  uploadId: string;
  physicalAddress: string;
}

@Injectable({
  providedIn: "root",
})
export class DatasetService {
  constructor(
    private http: HttpClient,
    private config: GuiConfigService
  ) {}

  public createDataset(dataset: Dataset): Observable<DashboardDataset> {
    return this.http.post<DashboardDataset>(`${AppSettings.getApiEndpoint()}/${DATASET_CREATE_URL}`, {
      datasetName: dataset.name,
      datasetDescription: dataset.description,
      isDatasetPublic: dataset.isPublic,
    });
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
   * @param isLogin Determine whether a user is currently logged in
   * @returns Observable<Blob>
   */
  public retrieveDatasetVersionSingleFile(filePath: string, isLogin: boolean = true): Observable<Blob> {
    const endpointSegment = isLogin ? "presign-download" : "public-presign-download";
    const endpoint = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${endpointSegment}?filePath=${encodeURIComponent(filePath)}`;

    return this.http
      .get<{ presignedUrl: string }>(endpoint)
      .pipe(switchMap(({ presignedUrl }) => this.http.get(presignedUrl, { responseType: "blob" })));
  }

  /**
   * Retrieves a single file from a dataset version using a pre-signed URL.
   * @param filePath Relative file path within the dataset.
   * @param isLogin Determine whether a user is currently logged in
   * @returns void File is downloaded natively by the browser.
   */
  public retrieveDatasetVersionSingleFileViaBrowser(filePath: string, isLogin: boolean = true): void {
    const endpointSegment = isLogin ? "presign-download-s3" : "public-presign-download-s3";
    const endpoint = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${endpointSegment}?filePath=${encodeURIComponent(filePath)}`;

    this.http.get<{ presignedUrl: string }>(endpoint).subscribe({
      next: response => {
        const presignedUrl = response.presignedUrl;
        const downloadUrl = document.createElement("a");

        downloadUrl.href = presignedUrl;
        document.body.appendChild(downloadUrl);
        downloadUrl.click();
        downloadUrl.remove();
      },
    });
  }

  /**
   * Retrieves a zip file of a dataset version.
   * @param did Dataset ID
   * @param dvid (Optional) Dataset version ID. If omitted, the latest version is downloaded.
   * @returns An Observable that emits a Blob containing the zip file.
   */
  public retrieveDatasetVersionZip(did: number, dvid?: number): Observable<Blob> {
    let params = new HttpParams();

    if (dvid !== undefined && dvid !== null) {
      params = params.set("dvid", dvid.toString());
    } else {
      params = params.set("latest", "true");
    }

    return this.http.get(`${AppSettings.getApiEndpoint()}/dataset/${did}/versionZip`, {
      params,
      responseType: "blob",
    });
  }

  public retrieveAccessibleDatasets(): Observable<DashboardDataset[]> {
    return this.http.get<DashboardDataset[]>(`${AppSettings.getApiEndpoint()}/${DATASET_LIST_URL}`);
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
   * Handles multipart upload for large files using RxJS,
   * with a concurrency limit on how many parts we process in parallel.
   */
  public multipartUpload(
    datasetName: string,
    filePath: string,
    file: File,
    partSize: number,
    concurrencyLimit: number
  ): Observable<MultipartUploadProgress> {
    const partCount = Math.ceil(file.size / partSize);

    return new Observable(observer => {
      // Track upload progress for each part independently
      const partProgress = new Map<number, number>();

      const subscription = this.initiateMultipartUpload(datasetName, filePath, partCount)
        .pipe(
          switchMap(initiateResponse => {
            const { uploadId, presignedUrls, physicalAddress } = initiateResponse;
            if (!uploadId) {
              observer.error(new Error("Failed to initiate multipart upload"));
              return EMPTY;
            }
            observer.next({
              filePath: filePath,
              percentage: 0,
              status: "initializing",
              uploadId: uploadId,
              physicalAddress: physicalAddress,
            });

            // Keep track of all uploaded parts
            const uploadedParts: { PartNumber: number; ETag: string }[] = [];

            // 1) Convert presignedUrls into a stream of URLs
            return from(presignedUrls).pipe(
              // 2) Use mergeMap with concurrency limit to upload chunk by chunk
              mergeMap((url, index) => {
                const partNumber = index + 1;
                const start = index * partSize;
                const end = Math.min(start + partSize, file.size);
                const chunk = file.slice(start, end);

                // Upload the chunk
                return new Observable(partObserver => {
                  const xhr = new XMLHttpRequest();

                  xhr.upload.addEventListener("progress", event => {
                    if (event.lengthComputable) {
                      // Update this specific part's progress
                      partProgress.set(partNumber, event.loaded);

                      // Calculate total progress across all parts
                      let totalUploaded = 0;
                      partProgress.forEach(bytes => (totalUploaded += bytes));
                      const percentage = Math.round((totalUploaded / file.size) * 100);

                      observer.next({
                        filePath,
                        percentage: Math.min(percentage, 99), // Cap at 99% until finalized
                        status: "uploading",
                        uploadId,
                        physicalAddress,
                      });
                    }
                  });

                  xhr.addEventListener("load", () => {
                    if (xhr.status === 200 || xhr.status === 201) {
                      const etag = xhr.getResponseHeader("ETag")?.replace(/"/g, "");
                      if (!etag) {
                        partObserver.error(new Error(`Missing ETag for part ${partNumber}`));
                        return;
                      }

                      // Mark this part as fully uploaded
                      partProgress.set(partNumber, chunk.size);
                      uploadedParts.push({ PartNumber: partNumber, ETag: etag });

                      // Recalculate progress
                      let totalUploaded = 0;
                      partProgress.forEach(bytes => (totalUploaded += bytes));
                      const percentage = Math.round((totalUploaded / file.size) * 100);

                      observer.next({
                        filePath,
                        percentage: Math.min(percentage, 99),
                        status: "uploading",
                        uploadId,
                        physicalAddress,
                      });
                      partObserver.complete();
                    } else {
                      partObserver.error(new Error(`Failed to upload part ${partNumber}`));
                    }
                  });

                  xhr.addEventListener("error", () => {
                    // Remove failed part from progress
                    partProgress.delete(partNumber);
                    partObserver.error(new Error(`Failed to upload part ${partNumber}`));
                  });

                  xhr.open("PUT", url);
                  xhr.send(chunk);
                });
              }, concurrencyLimit),

              // 3) Collect results from all uploads (like forkJoin, but respects concurrency)
              toArray(),
              // 4) Finalize if all parts succeeded
              switchMap(() =>
                this.finalizeMultipartUpload(datasetName, filePath, uploadId, uploadedParts, physicalAddress, false)
              ),
              tap(() => {
                observer.next({
                  filePath,
                  percentage: 100,
                  status: "finished",
                  uploadId: uploadId,
                  physicalAddress: physicalAddress,
                });
                observer.complete();
              }),
              catchError((error: unknown) => {
                // If an error occurred, abort the upload
                observer.next({
                  filePath,
                  percentage: Math.round((uploadedParts.length / partCount) * 100),
                  status: "aborted",
                  uploadId: uploadId,
                  physicalAddress: physicalAddress,
                });

                return this.finalizeMultipartUpload(
                  datasetName,
                  filePath,
                  uploadId,
                  uploadedParts,
                  physicalAddress,
                  true
                ).pipe(switchMap(() => throwError(() => error)));
              })
            );
          })
        )
        .subscribe({
          error: (err: unknown) => observer.error(err),
        });
      return () => subscription.unsubscribe();
    });
  }

  /**
   * Initiates a multipart upload and retrieves presigned URLs for each part.
   * @param datasetName Dataset Name
   * @param filePath File path within the dataset
   * @param numParts Number of parts for the multipart upload
   */
  private initiateMultipartUpload(
    datasetName: string,
    filePath: string,
    numParts: number
  ): Observable<{ uploadId: string; presignedUrls: string[]; physicalAddress: string }> {
    const params = new HttpParams()
      .set("type", "init")
      .set("datasetName", datasetName)
      .set("filePath", encodeURIComponent(filePath))
      .set("numParts", numParts.toString());

    return this.http.post<{ uploadId: string; presignedUrls: string[]; physicalAddress: string }>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`,
      {},
      { params }
    );
  }

  /**
   * Completes or aborts a multipart upload, sending part numbers and ETags to the backend.
   */
  public finalizeMultipartUpload(
    datasetName: string,
    filePath: string,
    uploadId: string,
    parts: { PartNumber: number; ETag: string }[],
    physicalAddress: string,
    isAbort: boolean
  ): Observable<Response> {
    const params = new HttpParams()
      .set("type", isAbort ? "abort" : "finish")
      .set("datasetName", datasetName)
      .set("filePath", encodeURIComponent(filePath))
      .set("uploadId", uploadId);

    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`,
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
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.put<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`, {}, { params });
  }

  /**
   * Deletes a dataset file from LakeFS.
   * @param did Dataset ID
   * @param filePath File path to delete
   */
  public deleteDatasetFile(did: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/file`, { params });
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

  public deleteDatasets(did: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`);
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

  public retrieveOwners(): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${DATASET_GET_OWNERS_URL}`);
  }
}

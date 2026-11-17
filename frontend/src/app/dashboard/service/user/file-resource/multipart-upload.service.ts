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
import { HttpClient, HttpParams } from "@angular/common/http";
import { catchError, map, mergeMap, switchMap, tap, toArray } from "rxjs/operators";
import { from, Observable, throwError } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { AuthService } from "src/app/common/service/user/auth.service";
import { FileResourceEndpoint } from "./file-resource-endpoint";

export interface MultipartUploadProgress {
  filePath: string;
  percentage: number;
  status: "initializing" | "uploading" | "finished" | "aborted" | "failed";
  uploadSpeed?: number; // bytes per second
  estimatedTimeRemaining?: number; // seconds
  totalTime?: number; // total seconds taken
}

/**
 * The multipart upload engine, shared by every versioned resource kind. Addressing comes from the
 * FileResourceEndpoint the caller supplies; everything else here is resource-agnostic.
 */
@Injectable({
  providedIn: "root",
})
export class MultipartUploadService {
  constructor(private http: HttpClient) {}

  /**
   * Handles multipart upload for large files using RxJS,
   * with a concurrency limit on how many parts we process in parallel.
   *
   * Backend flow, where {base} and {nameKey} come from the endpoint:
   *   POST /{base}/multipart-upload?type=init&ownerEmail=...&{nameKey}=...&filePath=...&numParts=N
   *   POST /{base}/multipart-upload/part?ownerEmail=...&{nameKey}=...&filePath=...&partNumber=<n>  (body: raw chunk)
   *   POST /{base}/multipart-upload?type=finish&ownerEmail=...&{nameKey}=...&filePath=...
   *   POST /{base}/multipart-upload?type=abort&ownerEmail=...&{nameKey}=...&filePath=...
   */
  public multipartUpload(
    endpoint: FileResourceEndpoint,
    ownerEmail: string,
    resourceName: string,
    filePath: string,
    file: File,
    partSize: number,
    concurrencyLimit: number,
    restart: boolean
  ): Observable<MultipartUploadProgress> {
    const partCount = Math.ceil(file.size / partSize);

    return new Observable<MultipartUploadProgress>(observer => {
      // Track upload progress (bytes) for each part independently
      const partProgress = new Map<number, number>();

      let baselineUploaded = 0;
      // Progress tracking state
      let startTime: number | null = null;
      const speedSamples: number[] = [];
      let lastETA = 0;
      let lastUpdateTime = 0;

      const lastStats = {
        uploadSpeed: 0,
        estimatedTimeRemaining: 0,
        totalTime: 0,
      };

      const getTotalTime = () => (startTime ? (Date.now() - startTime) / 1000 : 0);

      // Calculate stats with smoothing and simple throttling (~1s)
      const calculateStats = (totalUploaded: number) => {
        if (startTime === null) {
          startTime = Date.now();
        }

        const now = Date.now();
        const elapsed = getTotalTime();

        const shouldUpdate = now - lastUpdateTime >= 1000;
        if (!shouldUpdate) {
          // keep totalTime fresh even when throttled
          lastStats.totalTime = elapsed;
          return lastStats;
        }
        lastUpdateTime = now;

        const sessionUploaded = Math.max(0, totalUploaded - baselineUploaded);
        const currentSpeed = elapsed > 0 ? sessionUploaded / elapsed : 0;
        speedSamples.push(currentSpeed);
        if (speedSamples.length > 5) {
          speedSamples.shift();
        }
        const avgSpeed = speedSamples.length > 0 ? speedSamples.reduce((a, b) => a + b, 0) / speedSamples.length : 0;

        const remaining = file.size - totalUploaded;
        let eta = avgSpeed > 0 ? remaining / avgSpeed : 0;
        eta = Math.min(eta, 24 * 60 * 60); // cap ETA at 24h

        if (lastETA > 0 && eta > 0) {
          const maxChange = lastETA * 0.3;
          const diff = Math.abs(eta - lastETA);
          if (diff > maxChange) {
            eta = lastETA + (eta > lastETA ? maxChange : -maxChange);
          }
        }
        lastETA = eta;

        const percentComplete = (totalUploaded / file.size) * 100;
        if (percentComplete > 95) {
          eta = Math.min(eta, 10);
        }

        lastStats.uploadSpeed = avgSpeed;
        lastStats.estimatedTimeRemaining = Math.max(0, Math.round(eta));
        lastStats.totalTime = elapsed;

        return lastStats;
      };

      // 1. INIT: ask backend to create a LakeFS multipart upload session
      const initParams = new HttpParams()
        .set("type", "init")
        .set("ownerEmail", ownerEmail)
        .set(endpoint.nameParamKey, resourceName)
        .set("filePath", encodeURIComponent(filePath))
        .set("fileSizeBytes", file.size.toString())
        .set("partSizeBytes", partSize.toString())
        .set("restart", restart);

      const init$ = this.http.post<{ missingParts: number[]; completedPartsCount: number }>(
        `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/multipart-upload`,
        {},
        { params: initParams }
      );

      const subscription = init$
        .pipe(
          switchMap(initResp => {
            const missingParts = (initResp?.missingParts ?? []).slice();
            const completedPartsCount = initResp?.completedPartsCount ?? 0;

            const missingBytes = missingParts.reduce((sum, partNumber) => {
              const start = (partNumber - 1) * partSize;
              const end = Math.min(start + partSize, file.size);
              return sum + (end - start);
            }, 0);

            baselineUploaded = file.size - missingBytes;
            const baselinePct = partCount > 0 ? Math.round((completedPartsCount / partCount) * 100) : 0;

            observer.next({
              filePath,
              percentage: baselinePct,
              status: "initializing",
              uploadSpeed: 0,
              estimatedTimeRemaining: 0,
              totalTime: 0,
            });
            // 2. Upload each part to /multipart-upload/part using XMLHttpRequest
            return from(missingParts).pipe(
              mergeMap(partNumber => {
                const start = (partNumber - 1) * partSize;
                const end = Math.min(start + partSize, file.size);
                const chunk = file.slice(start, end);

                return new Observable<void>(partObserver => {
                  const xhr = new XMLHttpRequest();

                  xhr.upload.addEventListener("progress", event => {
                    if (event.lengthComputable) {
                      partProgress.set(partNumber, event.loaded);

                      let totalUploaded = baselineUploaded; // CHANGED
                      partProgress.forEach(bytes => {
                        totalUploaded += bytes;
                      });

                      const percentage = Math.round((totalUploaded / file.size) * 100);
                      const stats = calculateStats(totalUploaded);

                      observer.next({
                        filePath,
                        percentage: Math.min(percentage, 99),
                        status: "uploading",
                        ...stats,
                      });
                    }
                  });

                  xhr.addEventListener("load", () => {
                    if (xhr.status === 200 || xhr.status === 204) {
                      // Mark part as fully uploaded
                      partProgress.set(partNumber, chunk.size);

                      let totalUploaded = baselineUploaded;
                      partProgress.forEach(bytes => {
                        totalUploaded += bytes;
                      });

                      // Force stats recompute on completion
                      lastUpdateTime = 0;
                      const percentage = Math.round((totalUploaded / file.size) * 100);
                      const stats = calculateStats(totalUploaded);

                      observer.next({
                        filePath,
                        percentage: Math.min(percentage, 99),
                        status: "uploading",
                        ...stats,
                      });

                      partObserver.complete();
                    } else {
                      partObserver.error(new Error(`Failed to upload part ${partNumber} (HTTP ${xhr.status})`));
                    }
                  });

                  xhr.addEventListener("error", () => {
                    // Remove failed part from progress
                    partProgress.delete(partNumber);
                    partObserver.error(new Error(`Failed to upload part ${partNumber}`));
                  });

                  const partUrl =
                    `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/multipart-upload/part` +
                    `?ownerEmail=${encodeURIComponent(ownerEmail)}` +
                    `&${endpoint.nameParamKey}=${encodeURIComponent(resourceName)}` +
                    `&filePath=${encodeURIComponent(filePath)}` +
                    `&partNumber=${partNumber}`;

                  xhr.open("POST", partUrl);
                  xhr.setRequestHeader("Content-Type", "application/octet-stream");
                  const token = AuthService.getAccessToken();
                  if (token) {
                    xhr.setRequestHeader("Authorization", `Bearer ${token}`);
                  }
                  xhr.send(chunk);
                  return () => {
                    try {
                      xhr.abort();
                    } catch {}
                  };
                });
              }, concurrencyLimit),
              toArray(), // wait for all parts
              // 3. FINISH: notify backend that all parts are done
              switchMap(() => {
                const finishParams = new HttpParams()
                  .set("type", "finish")
                  .set("ownerEmail", ownerEmail)
                  .set(endpoint.nameParamKey, resourceName)
                  .set("filePath", encodeURIComponent(filePath));

                return this.http.post(
                  `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/multipart-upload`,
                  {},
                  { params: finishParams }
                );
              }),
              tap(() => {
                const totalTime = getTotalTime();
                observer.next({
                  filePath,
                  percentage: 100,
                  status: "finished",
                  uploadSpeed: 0,
                  estimatedTimeRemaining: 0,
                  totalTime,
                });
                observer.complete();
              }),
              catchError((error: unknown) => {
                // On error, compute best-effort percentage from bytes we've seen
                let totalUploaded = baselineUploaded;
                partProgress.forEach(bytes => {
                  totalUploaded += bytes;
                });
                const percentage = file.size > 0 ? Math.round((totalUploaded / file.size) * 100) : 0;

                observer.next({
                  filePath,
                  percentage,
                  status: "failed",
                  uploadSpeed: 0,
                  estimatedTimeRemaining: 0,
                  totalTime: getTotalTime(),
                });

                return throwError(() => error);
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

  public listMultipartUploads(
    endpoint: FileResourceEndpoint,
    ownerEmail: string,
    resourceName: string
  ): Observable<string[]> {
    const params = new HttpParams()
      .set("type", "list")
      .set("ownerEmail", ownerEmail)
      .set(endpoint.nameParamKey, resourceName);

    return this.http
      .post<{
        filePaths: string[];
      }>(`${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/multipart-upload`, {}, { params })
      .pipe(map(res => res?.filePaths ?? []));
  }

  public findExistingUploadFiles(
    endpoint: FileResourceEndpoint,
    resourceId: number,
    files: { path: string; sizeBytes: number }[]
  ): Observable<string[]> {
    return this.http
      .post<{ filePaths: string[] }>(
        `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/${resourceId}/existing-upload-files`,
        {
          files,
        }
      )
      .pipe(map(res => res?.filePaths ?? []));
  }

  public finalizeMultipartUpload(
    endpoint: FileResourceEndpoint,
    ownerEmail: string,
    resourceName: string,
    filePath: string,
    isAbort: boolean
  ): Observable<Response> {
    const params = new HttpParams()
      .set("type", isAbort ? "abort" : "finish")
      .set("ownerEmail", ownerEmail)
      .set(endpoint.nameParamKey, resourceName)
      .set("filePath", encodeURIComponent(filePath));

    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/multipart-upload`,
      {},
      { params }
    );
  }
}

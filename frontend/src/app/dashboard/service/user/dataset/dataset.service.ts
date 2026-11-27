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
import { map, switchMap } from "rxjs/operators";
import { Contributor, Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { AppSettings } from "../../../../common/app-setting";
import { Observable } from "rxjs";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { StagedFileService } from "../file-resource/staged-file.service";
import { DATASET_FILE_RESOURCE_ENDPOINT } from "../file-resource/file-resource-endpoint";

const DATASET_BASE_URL = "dataset";

const DATASET_VERSION_BASE_URL = "version";
export const DEFAULT_DATASET_NAME = "Untitled-dataset";

const DATASET_NAME_MAX_LENGTH = 128;
const DATASET_NAME_PATTERN = /^[A-Za-z0-9_-]+$/;

export function validateDatasetName(name: string): string | null {
  if (!DATASET_NAME_PATTERN.test(name) || name.length > DATASET_NAME_MAX_LENGTH) {
    return "Invalid dataset name: only letters, numbers, underscores, and hyphens are allowed (max 128 characters)";
  }
  return null;
}

// Blank optional fields are omitted from requests so they are stored as NULL
// instead of empty strings.
function normalizeContributor(contributor: Contributor): Contributor {
  return {
    ...contributor,
    email: contributor.email?.trim() || undefined,
    affiliation: contributor.affiliation?.trim() || undefined,
    comments: contributor.comments?.trim() || undefined,
  };
}

const DATASET_PUBLIC_VERSION_BASE_URL = "publicVersion";

@Injectable({
  providedIn: "root",
})
export class DatasetService {
  constructor(
    private http: HttpClient,
    private stagedFileService: StagedFileService
  ) {}

  public createDataset(dataset: Dataset, contributors: Contributor[] = []): Observable<DashboardDataset> {
    return this.http.post<DashboardDataset>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/create`, {
      datasetName: dataset.name,
      datasetDescription: dataset.description,
      isDatasetPublic: dataset.isPublic,
      isDatasetDownloadable: dataset.isDownloadable,
      contributors: contributors.map(normalizeContributor),
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
    return this.http.get<DashboardDataset[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/list`);
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
   * Deletes a dataset file from LakeFS.
   * @param did Dataset ID
   * @param filePath File path to delete
   */
  public deleteDatasetFile(did: number, filePath: string): Observable<Response> {
    return this.stagedFileService.deleteFile(DATASET_FILE_RESOURCE_ENDPOINT, did, filePath);
  }

  /**
   * retrieve a list of versions of a dataset. The list is sorted so that the latest versions are at front.
   * @param did
   * @param isLogin
   */
  public retrieveDatasetVersionList(did: number, isLogin: boolean = true): Observable<DatasetVersion[]> {
    const apiEndPont = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_BASE_URL}/list`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_BASE_URL}/list`;
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
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_BASE_URL}/latest`)
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
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/update/name`, {
      did: did,
      name: name,
    });
  }

  public updateDatasetDescription(did: number, description: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/update/description`, {
      did: did,
      description: description,
    });
  }

  public updateDatasetPublicity(did: number): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/update/publicity`, {});
  }

  public updateDatasetDownloadable(did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/update/downloadable`,
      {}
    );
  }

  public retrieveOwners(): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/user-dataset-owners`);
  }

  public updateDatasetCoverImage(did: number, coverImage: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/dataset/${did}/update/cover`, {
      coverImage: coverImage,
    });
  }

  public getDatasetCoverUrl(did: number): Observable<{ url: string | null }> {
    return this.http.get<{ url: string | null }>(`${AppSettings.getApiEndpoint()}/dataset/${did}/cover-url`);
  }

  public updateDatasetContributors(did: number, contributors: ReadonlyArray<Contributor>): Observable<void> {
    return this.http.post<void>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/update/contributors`, {
      did,
      contributors: contributors.map(normalizeContributor),
    });
  }
}

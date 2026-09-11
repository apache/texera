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
import { HttpClient } from "@angular/common/http";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";

const CU_IMAGE_BASE_URL = `${AppSettings.getApiEndpoint()}/cu-image`;

/** PENDING and VALIDATING mean the check is running; only READY can back a unit. */
export type CuImageStatus = "PENDING" | "VALIDATING" | "READY" | "FAILED";

export interface CuImage {
  iid: number;
  /** What users see in the computing-unit dropdown. */
  name: string;
  /** The reference the administrator gave, normalised. */
  sourceRef: string;
  /** The digest sourceRef resolved to. Null until a check succeeds. */
  sourceDigest: string | null;
  status: CuImageStatus;
  /** sourceRef pinned to its digest: what a unit actually runs. */
  imageTag: string | null;
  /** Numbers this image's checks, so a retry is told apart from the one before. */
  attempt: number;
  creationTime: number;
  updateTime: number;
}

export interface CuImageValidationLog {
  iid: number;
  status: CuImageStatus;
  attempt: number;
  log: string;
}

/** Whether the list is worth polling. */
export function isInProgress(image: CuImage): boolean {
  return image.status === "PENDING" || image.status === "VALIDATING";
}

@Injectable({ providedIn: "root" })
export class CuImageService {
  constructor(private http: HttpClient) {}

  /** Readable by any signed-in user: the unit dropdown is built from it. Rest is admin-only. */
  list(): Observable<CuImage[]> {
    return this.http.get<CuImage[]>(CU_IMAGE_BASE_URL);
  }

  /** Registering starts the first check. */
  create(name: string, sourceRef: string): Observable<CuImage> {
    return this.http.post<CuImage>(CU_IMAGE_BASE_URL, { name, sourceRef });
  }

  /** Checks the same reference again: picks up a moved tag, retries a failed check. */
  refresh(iid: number): Observable<CuImage> {
    return this.http.post<CuImage>(`${CU_IMAGE_BASE_URL}/${iid}/refresh`, {});
  }

  /** The check's output, where a rejection explains itself. */
  log(iid: number): Observable<CuImageValidationLog> {
    return this.http.get<CuImageValidationLog>(`${CU_IMAGE_BASE_URL}/${iid}/log`);
  }

  delete(iid: number): Observable<void> {
    return this.http.delete<void>(`${CU_IMAGE_BASE_URL}/${iid}`);
  }
}

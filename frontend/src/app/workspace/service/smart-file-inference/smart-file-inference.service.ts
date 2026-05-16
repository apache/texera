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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";

export interface SmartFileInferenceColumn {
  name: string;
  type: string;
}

export interface SmartFileInferenceResponse {
  detectedFormat: string;
  schema: SmartFileInferenceColumn[];
  customDelimiter: string | null;
  hasHeader: boolean | null;
  sheetName: string | null;
  availableSheetNames: string[];
  flatten: boolean | null;
  isFolder: boolean;
  fileCount: number;
}

export interface SmartFileInferenceRequest {
  fileName: string;
  fileEncoding?: string;
  formatOverride?: string;
  customDelimiter?: string;
  hasHeader?: boolean;
  sheetName?: string;
  flatten?: boolean;
}

/** Operator type string registered in LogicalOp.scala. */
export const SMART_FILE_SCAN_TYPE = "SmartFileScan";

/**
 * Talks to the backend `POST /api/file-inference/preview` endpoint that backs the
 * SmartFileScan operator. The endpoint runs the same inference path the operator
 * uses at workflow compile time, so what the user sees in the property panel is
 * exactly what the workflow will produce for either one file or one folder.
 */
@Injectable({
  providedIn: "root",
})
export class SmartFileInferenceService {
  constructor(private http: HttpClient) {}

  preview(request: SmartFileInferenceRequest): Observable<SmartFileInferenceResponse> {
    return this.http.post<SmartFileInferenceResponse>(
      `${AppSettings.getApiEndpoint()}/file-inference/preview`,
      request
    );
  }
}

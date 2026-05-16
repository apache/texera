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

export interface LLMSourceColumn {
  name: string;
  type: string;
}

export interface LLMSourceTable {
  name: string;
  description: string;
  columns: LLMSourceColumn[];
}

export interface LLMSourceGenerateRequest {
  fileName: string;
  userHint?: string;
  llmModel?: string;
  previousCode?: string;
  previousError?: string;
}

export interface LLMSourceGenerateResponse {
  generatedCode: string;
  tables: LLMSourceTable[];
  unionColumns: LLMSourceColumn[];
  llmModel: string;
  sampleHash: string;
  generatedAt: string;
  warnings: string[];
}

/** Operator type string registered in LogicalOp.scala. */
export const LLM_FILE_SCAN_TYPE = "LLMFileScan";

/**
 * Talks to the backend `POST /api/llm-source/generate` endpoint that powers the
 * LLM-generated source operator. The endpoint reads a sample of the user's file,
 * asks the LLM to write a Python parser and declare per-table schemas, validates
 * the result, and returns it. Generation is design-time only — the workflow itself
 * never calls the LLM at execution time.
 */
@Injectable({
  providedIn: "root",
})
export class LLMSourceService {
  constructor(private http: HttpClient) {}

  generate(request: LLMSourceGenerateRequest): Observable<LLMSourceGenerateResponse> {
    return this.http.post<LLMSourceGenerateResponse>(
      `${AppSettings.getApiEndpoint()}/llm-source/generate`,
      request
    );
  }
}

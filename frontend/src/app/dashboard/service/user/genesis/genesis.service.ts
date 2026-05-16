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
import { delay, Observable, of } from "rxjs";

// USE_MOCK = true means use hardcoded data for offline frontend dev. Set to false to call real backend at /api/genesis/*.
export const USE_MOCK = false;

export interface UploadResponse {
  /** Server-issued id for analyze/instantiate cache (preferred round-trip). */
  upload_id?: string;
  dataset_id: number;
  dataset_name: string;
  file_path: string;
  columns: string[];
  sample_rows: any[][];
  /** Total data rows in the file (excluding header), when provided by the server. */
  row_count?: number;
}

export interface Suggestion {
  id: string;
  title: string;
  description: string;
  estimated_runtime_seconds: number;
  analysis_type?: string;
  /** Per-card prediction target; may be absent for clustering flows. */
  target_column?: string | null;
}

export interface AnalyzeResponse {
  detected_scenario: string;
  confidence: number;
  target_column: string;
  /** LLM / heuristic one-line dataset description for the card header. */
  dataset_summary?: string;
  scenario_label?: string;
  suggestions: Suggestion[];
  upload_id?: string;
  row_count?: number;
  file_path?: string;
  dataset_id?: number;
  dataset_name?: string;
}

export type GenesisInstantiateMode = "template" | "agent";

export interface InstantiateResponse {
  workflow_name: string;
  /** JSON-encoded string of full Texera workflow for template mode (may be empty `{}` for agent mode). */
  workflow_content: string;
  mode?: GenesisInstantiateMode;
  suggestion_id?: string;
  agent_prompt?: string;
  allowed_operator_types?: string[];
  model?: string;
}

const MOCK_UPLOAD_RESPONSE: UploadResponse = {
  upload_id: "mock-upload-id",
  dataset_id: 5,
  dataset_name: "diabetes",
  file_path: "/texera/diabetes/v1/diabetes.csv",
  columns: [
    "Pregnancies",
    "Glucose",
    "BloodPressure",
    "SkinThickness",
    "Insulin",
    "BMI",
    "DiabetesPedigreeFunction",
    "Age",
    "Outcome",
  ],
  sample_rows: [
    [6, 148, 72, 35, 0, 33.6, 0.627, 50, 1],
    [1, 85, 66, 29, 0, 26.6, 0.351, 31, 0],
    [8, 183, 64, 0, 0, 23.3, 0.672, 32, 1],
    [1, 89, 66, 23, 94, 28.1, 0.167, 21, 0],
    [0, 137, 40, 35, 168, 43.1, 2.288, 33, 1],
  ],
  row_count: 768,
};

const MOCK_ANALYZE_RESPONSE: AnalyzeResponse = {
  detected_scenario: "diabetes",
  scenario_label: "diabetes",
  dataset_summary: "Pima Indians diabetes data: 768 patients with clinical vitals and outcome label.",
  confidence: 0.92,
  target_column: "Outcome",
  upload_id: "mock-upload-id",
  row_count: 768,
  suggestions: [
    {
      id: "diabetes_prediction",
      title: "Prediction Model",
      description: "Train a classifier to predict diabetes onset",
      estimated_runtime_seconds: 15,
      analysis_type: "classification",
      target_column: "Outcome",
    },
    {
      id: "diabetes_risk_factors",
      title: "Risk Factor Analysis",
      description: "Find the attributes that drive diabetes risk",
      estimated_runtime_seconds: 10,
      analysis_type: "classification",
      target_column: "Outcome",
    },
    {
      id: "diabetes_clustering",
      title: "Patient Clustering",
      description: "Group patients by similar feature profiles",
      estimated_runtime_seconds: 12,
      analysis_type: "clustering",
      target_column: null,
    },
  ],
};

const MOCK_INSTANTIATE_RESPONSE: InstantiateResponse = {
  workflow_name: "[Genesis] Diabetes Prediction",
  workflow_content: "{}",
  mode: "template",
};

@Injectable({
  providedIn: "root",
})
export class GenesisService {
  constructor(private http: HttpClient) {}

  public upload(file: File, jwtToken: string): Observable<UploadResponse> {
    if (USE_MOCK) {
      return of(MOCK_UPLOAD_RESPONSE).pipe(delay(800));
    }
    const form = new FormData();
    form.append("file", file);
    form.append("jwt_token", jwtToken);
    return this.http.post<UploadResponse>("/api/genesis/upload", form);
  }

  public analyze(req: UploadResponse): Observable<AnalyzeResponse> {
    if (USE_MOCK) {
      return of(MOCK_ANALYZE_RESPONSE).pipe(delay(500));
    }
    return this.http.post<AnalyzeResponse>("/api/genesis/analyze", req);
  }

  public instantiate(
    suggestionId: string,
    datasetId: number,
    filePath: string,
    targetCol: string,
    options?: { mode?: GenesisInstantiateMode; columns?: string[]; uploadId?: string }
  ): Observable<InstantiateResponse> {
    if (USE_MOCK) {
      return of(MOCK_INSTANTIATE_RESPONSE).pipe(delay(300));
    }
    const body: Record<string, unknown> = {
      suggestion_id: suggestionId,
      dataset_id: datasetId,
      file_path: filePath,
      target_column: targetCol,
      columns: options?.columns ?? [],
      mode: options?.mode ?? "agent",
    };
    if (options?.uploadId) {
      body.upload_id = options.uploadId;
    }
    return this.http.post<InstantiateResponse>("/api/genesis/instantiate", body);
  }
}

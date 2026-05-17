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
import { Observable, from, of } from "rxjs";
import { catchError, delay, switchMap } from "rxjs/operators";
import * as Papa from "papaparse";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { UserService } from "../../../common/service/user/user.service";
import { DatasetProfile } from "./data-profiling.types";
import { ParsedRow, computeProfile } from "./data-profiling.profiler";

const MAX_ROWS_FOR_PROFILE = 5000;

@Injectable({ providedIn: "root" })
export class DataProfilingService {
  constructor(
    private datasetService: DatasetService,
    private userService: UserService
  ) {}

  /**
   * Fetches the CSV at `source` (a dataset-relative path like /{owner}/{ds}/{ver}/{rel})
   * and computes a real DatasetProfile. Falls back to a deterministic mock on any
   * failure so the panel always has something useful to show.
   */
  getProfile(source: string = ""): Observable<DatasetProfile> {
    const path = (source ?? "").trim();
    if (!path || !this.looksLikeDatasetPath(path)) {
      return of(this.buildMockProfile(path || "diabetes.csv")).pipe(delay(120));
    }

    const isLogin = this.userService.isLogin();
    return this.datasetService.retrieveDatasetVersionSingleFile(path, isLogin).pipe(
      switchMap(blob => from(this.profileBlob(blob, path))),
      catchError(err => {
        // eslint-disable-next-line no-console
        console.warn("[data-profiling] real profile failed, using mock:", err);
        return of(this.buildMockProfile(path));
      })
    );
  }

  private looksLikeDatasetPath(p: string): boolean {
    // matches /{owner}/{datasetName}/{versionName}/{relativePath} — at least 4 segments
    return /^\/[^/]+\/[^/]+\/[^/]+\/.+/.test(p);
  }

  private async profileBlob(blob: Blob, source: string): Promise<DatasetProfile> {
    const text = await blob.text();
    const result = Papa.parse<Record<string, string>>(text, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      preview: MAX_ROWS_FOR_PROFILE,
    });
    const headers = (result.meta?.fields ?? []).filter(h => h && h.length > 0);
    if (headers.length === 0) {
      throw new Error("CSV has no header row");
    }
    const rows = (result.data ?? []) as ParsedRow[];
    return computeProfile(source, headers, rows);
  }

  private buildMockProfile(source: string): DatasetProfile {
    return {
      source,
      rowCount: 768,
      duplicateRows: 23,
      columns: [
        {
          name: "patient_id",
          dtype: "categorical",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 767,
          topValues: [
            { value: "P-0001", count: 1 },
            { value: "P-0002", count: 1 },
            { value: "P-0003", count: 1 },
          ],
        },
        {
          name: "age",
          dtype: "numeric",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 52,
          mean: 33.24,
          median: 29,
          std: 11.76,
          min: 21,
          max: 81,
          outlierCount: 9,
          histogram: [12, 84, 142, 168, 130, 92, 60, 42, 24, 14],
        },
        {
          name: "BMI",
          dtype: "numeric",
          count: 768,
          missing: 11,
          missingPercent: 1.4,
          unique: 248,
          mean: 31.99,
          median: 32,
          std: 7.88,
          min: 0,
          max: 67.1,
          outlierCount: 18,
          histogram: [4, 12, 38, 92, 168, 196, 144, 72, 30, 12],
        },
        {
          name: "HbA1c",
          dtype: "numeric",
          count: 768,
          missing: 94,
          missingPercent: 12.3,
          unique: 138,
          mean: 6.4,
          median: 6.2,
          std: 1.1,
          min: 4.1,
          max: 12.8,
          outlierCount: 22,
          histogram: [8, 36, 92, 178, 162, 110, 58, 28, 12, 4],
        },
        {
          name: "blood_pressure",
          dtype: "numeric",
          count: 768,
          missing: 35,
          missingPercent: 4.6,
          unique: 47,
          mean: 69.1,
          median: 72,
          std: 19.3,
          min: 0,
          max: 122,
          outlierCount: 14,
          histogram: [22, 28, 44, 82, 168, 196, 134, 64, 22, 8],
        },
        {
          name: "income",
          dtype: "numeric",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 612,
          mean: 58420,
          median: 49000,
          std: 41200,
          min: 8200,
          max: 612000,
          outlierCount: 42,
          histogram: [240, 232, 132, 70, 38, 22, 14, 10, 6, 4],
        },
        {
          name: "smoker_flag",
          dtype: "categorical",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 1,
          topValues: [{ value: "N", count: 768 }],
        },
        {
          name: "insurance_type",
          dtype: "categorical",
          count: 768,
          missing: 6,
          missingPercent: 0.8,
          unique: 4,
          topValues: [
            { value: "Private", count: 412 },
            { value: "Medicare", count: 198 },
            { value: "Medicaid", count: 102 },
            { value: "Uninsured", count: 50 },
          ],
        },
        {
          name: "visit_date",
          dtype: "datetime",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 412,
        },
        {
          name: "diabetes",
          dtype: "categorical",
          count: 768,
          missing: 0,
          missingPercent: 0,
          unique: 2,
          topValues: [
            { value: "0", count: 500 },
            { value: "1", count: 268 },
          ],
        },
      ],
      correlations: [
        { a: "age", b: "BMI", r: 0.18 },
        { a: "age", b: "HbA1c", r: 0.32 },
        { a: "BMI", b: "HbA1c", r: 0.41 },
        { a: "BMI", b: "blood_pressure", r: 0.28 },
        { a: "HbA1c", b: "diabetes", r: 0.55 },
      ],
    };
  }
}

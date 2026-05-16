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

export type ColumnDtype = "numeric" | "categorical" | "datetime" | "text" | "boolean";

export interface TopValue {
  value: string | number;
  count: number;
}

export interface ColumnProfile {
  name: string;
  dtype: ColumnDtype;
  count: number;
  missing: number;
  missingPercent: number;
  unique: number;
  // numeric-only fields (may be undefined for non-numeric columns)
  mean?: number;
  median?: number;
  std?: number;
  min?: number;
  max?: number;
  outlierCount?: number;
  histogram?: number[];
  // categorical-only
  topValues?: TopValue[];
}

export interface CorrelationCell {
  a: string;
  b: string;
  r: number;
}

export interface DatasetProfile {
  source: string;
  rowCount: number;
  duplicateRows: number;
  columns: ColumnProfile[];
  correlations?: CorrelationCell[];
}

export type Severity = "critical" | "warning" | "info";

export interface CleaningSuggestion {
  severity: Severity;
  icon: string;
  title: string;
  reason: string;
  action: "drop_column" | "impute_column" | "remove_duplicates" | "review_outliers";
  column?: string;
  method?: "median" | "mode";
}

export type ColumnRoleKind =
  | "id"
  | "target"
  | "possible_target"
  | "feature"
  | "datetime"
  | "constant";

export interface ColumnRole {
  column: string;
  role: ColumnRoleKind;
  confidence: number;
  suggestion: string;
  dtype: ColumnDtype;
}

export interface QualityScoreBreakdown {
  total: number;
  band: "excellent" | "good" | "needs_attention" | "poor";
  completenessPercent: number;
  duplicatePercent: number;
  outlierPercent: number;
  constantColumns: number;
  highCardinalityColumns: number;
  imbalancedColumns: number;
}

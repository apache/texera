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

import {
  CleaningSuggestion,
  ColumnRole,
  DatasetProfile,
  QualityScoreBreakdown,
  Severity,
} from "./data-profiling.types";

export function computeQualityScore(profile: DatasetProfile): QualityScoreBreakdown {
  let score = 100;

  const avgMissing = profile.columns.length
    ? profile.columns.reduce((sum, c) => sum + c.missingPercent, 0) / profile.columns.length
    : 0;
  score -= Math.min(30, avgMissing * 3);

  const dupePercent = profile.rowCount > 0 ? (profile.duplicateRows / profile.rowCount) * 100 : 0;
  score -= Math.min(15, dupePercent * 1.5);

  const numericCols = profile.columns.filter(c => c.dtype === "numeric");
  let avgOutlierPercent = 0;
  if (numericCols.length > 0) {
    avgOutlierPercent =
      numericCols.reduce((sum, c) => {
        const denom = c.count || 1;
        return sum + ((c.outlierCount ?? 0) / denom) * 100;
      }, 0) / numericCols.length;
    score -= Math.min(15, avgOutlierPercent * 3);
  }

  const constantCols = profile.columns.filter(c => c.unique <= 1).length;
  score -= Math.min(10, constantCols * 5);

  const highCardCols = profile.columns.filter(
    c => c.dtype === "categorical" && c.unique > profile.rowCount * 0.5
  ).length;
  score -= Math.min(10, highCardCols * 5);

  const imbalancedCols = profile.columns.filter(c => {
    if (c.dtype !== "categorical" || !c.topValues || c.topValues.length === 0) return false;
    const denom = c.count || 1;
    return c.topValues[0].count / denom > 0.9;
  }).length;
  score -= Math.min(10, imbalancedCols * 5);

  if (avgMissing === 0) score = Math.min(100, score + 10);

  const total = Math.max(0, Math.round(score));

  let band: QualityScoreBreakdown["band"];
  if (total >= 90) band = "excellent";
  else if (total >= 70) band = "good";
  else if (total >= 50) band = "needs_attention";
  else band = "poor";

  return {
    total,
    band,
    completenessPercent: Math.max(0, Math.round(100 - avgMissing)),
    duplicatePercent: Math.round(dupePercent * 10) / 10,
    outlierPercent: Math.round(avgOutlierPercent * 10) / 10,
    constantColumns: constantCols,
    highCardinalityColumns: highCardCols,
    imbalancedColumns: imbalancedCols,
  };
}

export function qualityScoreColor(band: QualityScoreBreakdown["band"]): string {
  switch (band) {
    case "excellent":
      return "#52c41a";
    case "good":
      return "#faad14";
    case "needs_attention":
      return "#fa8c16";
    case "poor":
      return "#f5222d";
  }
}

export function qualityScoreLabel(band: QualityScoreBreakdown["band"]): string {
  switch (band) {
    case "excellent":
      return "Excellent";
    case "good":
      return "Good, some issues";
    case "needs_attention":
      return "Needs attention";
    case "poor":
      return "Poor quality";
  }
}

export function generateSuggestions(profile: DatasetProfile): CleaningSuggestion[] {
  const suggestions: CleaningSuggestion[] = [];

  for (const col of profile.columns) {
    if (col.missingPercent > 50) {
      suggestions.push({
        severity: "critical",
        icon: "🗑️",
        title: `Drop column "${col.name}"`,
        reason: `${col.missingPercent.toFixed(1)}% missing values — too sparse to be useful`,
        action: "drop_column",
        column: col.name,
      });
    } else if (col.missingPercent > 5 && col.missingPercent <= 50) {
      const method = col.dtype === "numeric" ? "median" : "mode";
      suggestions.push({
        severity: "warning",
        icon: "🔧",
        title: `Impute "${col.name}" missing values`,
        reason: `${col.missingPercent.toFixed(1)}% missing — use ${method} imputation`,
        action: "impute_column",
        column: col.name,
        method,
      });
    }

    if (col.unique <= 1) {
      suggestions.push({
        severity: "warning",
        icon: "🗑️",
        title: `Drop constant column "${col.name}"`,
        reason: `Only ${col.unique} unique value — provides no information`,
        action: "drop_column",
        column: col.name,
      });
    }

    if (col.dtype === "categorical" && col.unique > profile.rowCount * 0.9) {
      const pct = ((col.unique / profile.rowCount) * 100).toFixed(0);
      suggestions.push({
        severity: "info",
        icon: "🏷️",
        title: `"${col.name}" looks like an ID column`,
        reason: `${col.unique} unique values out of ${profile.rowCount} rows (${pct}%) — likely not useful for modeling`,
        action: "drop_column",
        column: col.name,
      });
    }

    if (col.dtype === "numeric" && (col.outlierCount ?? 0) > 0) {
      const outlierPercent = ((col.outlierCount ?? 0) / (col.count || 1)) * 100;
      if (outlierPercent > 5) {
        suggestions.push({
          severity: "warning",
          icon: "📊",
          title: `Review outliers in "${col.name}"`,
          reason: `${col.outlierCount} outliers (${outlierPercent.toFixed(1)}%) detected beyond 3 standard deviations`,
          action: "review_outliers",
          column: col.name,
        });
      }
    }
  }

  if (profile.duplicateRows > 0) {
    const pct = ((profile.duplicateRows / profile.rowCount) * 100).toFixed(1);
    suggestions.push({
      severity: "warning",
      icon: "📋",
      title: `Remove ${profile.duplicateRows} duplicate rows`,
      reason: `${pct}% of rows are exact duplicates`,
      action: "remove_duplicates",
    });
  }

  const order: Record<Severity, number> = { critical: 0, warning: 1, info: 2 };
  suggestions.sort((a, b) => order[a.severity] - order[b.severity]);
  return suggestions;
}

export function suggestionToOperatorHint(s: CleaningSuggestion): string {
  switch (s.action) {
    case "drop_column":
      return `Add a Projection operator that drops column "${s.column}".`;
    case "impute_column":
      return `Add a Missing Value Handler operator for column "${s.column}" using ${s.method} imputation.`;
    case "remove_duplicates":
      return `Add a Distinct (deduplicate) operator to remove exact-duplicate rows.`;
    case "review_outliers":
      return `Review outliers in "${s.column}" (values beyond ±3σ).`;
  }
}

const ID_NAME_PATTERN = /^(id|_id|index|row_?num|record_?id|patient_?id|user_?id)$/i;
const TARGET_NAME_PATTERN = /^(target|label|class|y|outcome|result|diagnosis|survived|is_|has_)/i;
const DATE_NAME_PATTERN = /^(date|time|timestamp|created|updated|year|month|day)/i;

export function detectColumnRoles(profile: DatasetProfile): ColumnRole[] {
  return profile.columns.map(col => {
    const isIdByName = ID_NAME_PATTERN.test(col.name);
    const isIdByCardinality =
      col.dtype === "categorical" && col.unique > profile.rowCount * 0.9;

    const isTargetByName = TARGET_NAME_PATTERN.test(col.name);
    const isTargetByShape =
      col.dtype === "categorical" && col.unique >= 2 && col.unique <= 10;

    const isDateByName = DATE_NAME_PATTERN.test(col.name);

    const isConstant = col.unique <= 1;

    let role: ColumnRole["role"];
    let confidence: number;
    let suggestion: string;

    if (isConstant) {
      role = "constant";
      confidence = 1.0;
      suggestion = "Drop — provides no information";
    } else if (isIdByName || isIdByCardinality) {
      role = "id";
      confidence = isIdByName ? 0.95 : 0.7;
      suggestion = "Drop before modeling — IDs don't generalize";
    } else if (isTargetByName) {
      role = "target";
      confidence = 0.85;
      suggestion = "Use as prediction target variable";
    } else if (isTargetByShape && !isIdByCardinality) {
      role = "possible_target";
      confidence = 0.5;
      suggestion = "Could be a target variable — low cardinality categorical";
    } else if (isDateByName || col.dtype === "datetime") {
      role = "datetime";
      confidence = 0.9;
      suggestion = "Extract features (year, month, day, weekday) or use for time-series split";
    } else {
      role = "feature";
      confidence = 0.8;
      suggestion = "Use as input feature for modeling";
    }

    return { column: col.name, role, confidence, suggestion, dtype: col.dtype };
  });
}

export function roleBadge(role: ColumnRole["role"]): { icon: string; label: string; color: string } {
  switch (role) {
    case "id":
      return { icon: "🏷️", label: "ID", color: "#8c8c8c" };
    case "target":
      return { icon: "🎯", label: "Target", color: "#52c41a" };
    case "possible_target":
      return { icon: "🎯", label: "Possible Target", color: "#95de64" };
    case "feature":
      return { icon: "📊", label: "Feature", color: "#1890ff" };
    case "datetime":
      return { icon: "📅", label: "Datetime", color: "#722ed1" };
    case "constant":
      return { icon: "⚪", label: "Constant", color: "#f5222d" };
  }
}

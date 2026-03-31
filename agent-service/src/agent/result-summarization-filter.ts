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

/**
 * Result statistics parsing.
 *
 * Parses per-column statistics produced by the Python worker (via pandas)
 * into a structured ResultStatistics type used by the agent for summarization.
 */

// ============================================================================
// Result Statistics Types
// ============================================================================

interface NumericColumnStats {
  type: "numeric";
  name: string;
  count: number;
  nullCount: number;
  uniqueCount: number;
  min: number;
  max: number;
  mean: number;
  median: number;
  std: number;
  p25: number;
  p75: number;
}

interface StringColumnStats {
  type: "string";
  name: string;
  count: number;
  nullCount: number;
  uniqueCount: number;
  topValues: { value: string; count: number }[];
}

type ColumnStats = NumericColumnStats | StringColumnStats;

export interface ResultStatistics {
  totalRows: number;
  columns: ColumnStats[];
  sampleRows: Record<string, any>[];
}

// ============================================================================
// Backend Stats Parsing (from Python worker pandas stats)
// ============================================================================

/**
 * Parse backend result statistics into the ResultStatistics format.
 * Each value in backendStats is a JSON string with {data_type, statistics}.
 * Falls back gracefully if parsing fails.
 */
export function parseBackendStats(backendStats: Record<string, string>): ResultStatistics {
  const columns: ColumnStats[] = [];
  let totalRows = 0;

  for (const [colName, statsJson] of Object.entries(backendStats)) {
    try {
      const colStats = JSON.parse(statsJson);
      const dataType: string = colStats.data_type ?? "Unsupported";
      const stats = colStats.statistics || {};

      const count = stats.count ?? 0;
      const nullCount = stats.null ?? 0;
      const uniqueCount = stats.distinct ?? 0;

      if (totalRows === 0 && count > 0) {
        totalRows = count + nullCount;
      }

      // Parse top_10 if present (available for all types when distinct <= 10)
      const topDistinct = stats.top_10 ?? {};
      const topValues = Object.entries(topDistinct)
        .map(([value, cnt]) => ({
          value: value.length > 50 ? value.substring(0, 50) + "..." : value,
          count: cnt as number,
        }))
        .sort((a, b) => b.count - a.count);

      if (dataType === "Numeric") {
        columns.push({
          type: "numeric",
          name: colName,
          count,
          nullCount,
          uniqueCount,
          min: stats.min ?? 0,
          max: stats.max ?? 0,
          mean: stats.mean ?? 0,
          median: stats.median ?? 0,
          std: stats.std ?? 0,
          p25: stats.p25 ?? 0,
          p75: stats.p75 ?? 0,
        });
      } else {
        // DateTime, Boolean, String, etc.
        columns.push({
          type: "string",
          name: colName,
          count,
          nullCount,
          uniqueCount,
          topValues,
        });
      }
    } catch {
      // Skip columns that fail to parse
    }
  }

  return { totalRows, columns, sampleRows: [] };
}

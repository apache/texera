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

import { describe, expect, test } from "bun:test";
import { recommendOperators, type OperatorCatalog } from "./recommender";
import { MAX_RECOMMENDATION_LIMIT } from "./recommender-types";

/**
 * Stub catalog. `known` lists the operator types the "running system" is aware
 * of; anything else is treated as non-existent. Metadata is minimal but enough
 * to exercise the `userFriendlyName` enrichment path.
 */
function stubCatalog(known: string[], initialized = true): OperatorCatalog {
  const set = new Set(known);
  return {
    isInitialized: () => initialized,
    operatorTypeExists: (t: string) => set.has(t),
    getAdditionalMetadata: (t: string) => (set.has(t) ? { userFriendlyName: `${t} (nice)` } : undefined),
  };
}

// A catalog that knows every operator type these tests reference.
const FULL_CATALOG = stubCatalog([
  "Filter",
  "Projection",
  "TypeCasting",
  "KeywordSearch",
  "Aggregate",
  "Sort",
  "Limit",
  "BarChart",
  "LineChart",
  "PythonUDFV2",
  "UnnestString",
]);

describe("recommendOperators (V1 hardcoded)", () => {
  test("returns the specific-rule successors for a known source operator", () => {
    const { recommendations, strategy } = recommendOperators({ operatorType: "CSVFileScan" }, FULL_CATALOG);
    expect(strategy).toBe("hardcoded");
    expect(recommendations.map(r => r.operatorType)).toEqual(["Filter", "Projection", "KeywordSearch"]);
  });

  test("defaults to three suggestions with descending scores", () => {
    const { recommendations } = recommendOperators({ operatorType: "Filter" }, FULL_CATALOG);
    expect(recommendations).toHaveLength(3);
    const scores = recommendations.map(r => r.score);
    expect(scores).toEqual([0.9, 0.75, 0.6]);
    for (let i = 1; i < scores.length; i++) {
      expect(scores[i]).toBeLessThan(scores[i - 1]);
    }
  });

  test("every suggestion carries a non-empty reason", () => {
    const { recommendations } = recommendOperators({ operatorType: "Aggregate" }, FULL_CATALOG);
    expect(recommendations.length).toBeGreaterThan(0);
    for (const r of recommendations) {
      expect(r.reason.length).toBeGreaterThan(0);
    }
  });

  test("terminal sink operators yield no suggestions", () => {
    expect(recommendOperators({ operatorType: "BarChart" }, FULL_CATALOG).recommendations).toEqual([]);
    // Sink detected by naming heuristic even without an explicit table entry.
    expect(recommendOperators({ operatorType: "ScatterMatrixChart" }, FULL_CATALOG).recommendations).toEqual([]);
  });

  test("unknown operator falls back to default successors", () => {
    const { recommendations } = recommendOperators({ operatorType: "TotallyMadeUpOp" }, FULL_CATALOG);
    expect(recommendations.map(r => r.operatorType)).toEqual(["Filter", "Projection", "PythonUDFV2"]);
  });

  test("respects and clamps the limit", () => {
    expect(recommendOperators({ operatorType: "Filter", limit: 1 }, FULL_CATALOG).recommendations).toHaveLength(1);
    // A limit above the max is clamped; Filter's rule only has 3 candidates anyway.
    const big = recommendOperators({ operatorType: "Filter", limit: 99 }, FULL_CATALOG).recommendations;
    expect(big.length).toBeLessThanOrEqual(MAX_RECOMMENDATION_LIMIT);
    // A limit below 1 is clamped up to 1.
    expect(recommendOperators({ operatorType: "Filter", limit: 0 }, FULL_CATALOG).recommendations).toHaveLength(1);
  });

  test("filters out suggestions the catalog does not know", () => {
    // Catalog only knows Projection out of Filter's [Aggregate, Projection, Sort].
    const catalog = stubCatalog(["Projection"]);
    const { recommendations } = recommendOperators({ operatorType: "Filter" }, catalog);
    expect(recommendations.map(r => r.operatorType)).toEqual(["Projection"]);
  });

  test("enriches suggestions with userFriendlyName when the catalog has it", () => {
    const { recommendations } = recommendOperators({ operatorType: "CSVFileScan" }, FULL_CATALOG);
    expect(recommendations[0].userFriendlyName).toBe("Filter (nice)");
  });

  test("without an initialized catalog, returns raw candidates unvalidated", () => {
    const { recommendations } = recommendOperators({ operatorType: "Filter" });
    expect(recommendations.map(r => r.operatorType)).toEqual(["Aggregate", "Projection", "Sort"]);
    expect(recommendations[0].userFriendlyName).toBeUndefined();
  });

  test("never suggests the operator itself", () => {
    const catalog = stubCatalog(["Filter", "Projection", "Sort", "Aggregate"]);
    const { recommendations } = recommendOperators({ operatorType: "Aggregate" }, catalog);
    expect(recommendations.map(r => r.operatorType)).not.toContain("Aggregate");
  });

  test("empty operatorType yields no suggestions", () => {
    expect(recommendOperators({ operatorType: "   " }, FULL_CATALOG).recommendations).toEqual([]);
  });
});

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
import type { OperatorMetadata } from "../api/backend-api";
import { recommendOperators, type OperatorCatalog } from "./operator-recommender";
import { targetPortIdFor, type OperatorCatalogEntry } from "./operator-recommendation-rules";
import { MAX_OPERATOR_RECOMMENDATION_LIMIT } from "./operator-recommendation-types";
import { OPERATOR_CATALOG_FIXTURE } from "./operator-catalog.fixture";

/** Stub catalog serving `metadata`, the way the loaded singleton would. */
function catalogFrom(metadata: OperatorMetadata, initialized = true): OperatorCatalog {
  const byType = new Map(metadata.operators.map(op => [op.operatorType, op]));
  return {
    isInitialized: () => initialized,
    getSchema: (t: string) => byType.get(t)?.jsonSchema,
    getAdditionalMetadata: (t: string) => byType.get(t)?.additionalMetadata,
  };
}

/** A catalog that knows only the listed operators from the fixture. */
function catalogWith(types: string[]): OperatorCatalog {
  return catalogFrom({
    ...OPERATOR_CATALOG_FIXTURE,
    operators: OPERATOR_CATALOG_FIXTURE.operators.filter(op => types.includes(op.operatorType)),
  });
}

/** The fixture catalog with `patch` applied to one operator's metadata. */
function catalogPatching(operatorType: string, patch: Partial<OperatorCatalogEntry>): OperatorCatalog {
  return catalogFrom({
    ...OPERATOR_CATALOG_FIXTURE,
    operators: OPERATOR_CATALOG_FIXTURE.operators.map(op =>
      op.operatorType === operatorType ? { ...op, additionalMetadata: { ...op.additionalMetadata, ...patch } } : op
    ),
  });
}

const CATALOG = catalogFrom(OPERATOR_CATALOG_FIXTURE);

/** Suggested types for `operatorType`; pass `null` for no catalog at all. */
function typesFor(operatorType: string, catalog: OperatorCatalog | null = CATALOG, limit?: number): string[] {
  return recommendOperators({ operatorType, limit }, catalog ?? undefined).recommendations.map(r => r.operatorType);
}

describe("recommendOperators (V1 hardcoded)", () => {
  test("returns the specific-rule successors for a known source operator", () => {
    const { recommendations, strategy } = recommendOperators({ operatorType: "CSVFileScan" }, CATALOG);
    expect(strategy).toBe("hardcoded");
    expect(recommendations.map(r => r.operatorType)).toEqual(["Filter", "Projection", "KeywordSearch"]);
    expect(recommendations.map(r => r.targetPortID)).toEqual(["input-0", "input-0", "input-0"]);
  });

  test("defaults to three suggestions with descending scores", () => {
    const { recommendations } = recommendOperators({ operatorType: "Filter" }, CATALOG);
    expect(recommendations).toHaveLength(3);
    expect(recommendations.map(r => r.score)).toEqual([0.9, 0.75, 0.6]);
  });

  test("every suggestion carries a non-empty reason", () => {
    const { recommendations } = recommendOperators({ operatorType: "Aggregate" }, CATALOG);
    expect(recommendations.length).toBeGreaterThan(0);
    for (const r of recommendations) {
      expect(r.reason.length).toBeGreaterThan(0);
    }
  });

  test("classifies operators from their catalog group", () => {
    expect(typesFor("Aggregate")).toEqual(["BarChart", "LineChart", "Sort"]);
    expect(typesFor("Sort")).toEqual(["BarChart", "Projection", "Limit"]);
    expect(typesFor("HashJoin")).toEqual(["Projection", "Aggregate", "Filter"]);
    expect(typesFor("UnnestString")).toEqual(["Aggregate", "Projection", "Sort"]);
  });

  test("treats any operator without input ports as a source", () => {
    expect(typesFor("PostgreSQLSource")).toEqual(["Filter", "Projection", "TypeCasting"]);
    // In the Python group, but reads no input.
    expect(typesFor("PythonUDFSourceV2")).toEqual(["Filter", "Projection", "TypeCasting"]);
  });

  test("visualizations yield no suggestions, whatever their name", () => {
    for (const viz of ["BarChart", "Scatterplot", "WordCloud"]) {
      expect(typesFor(viz)).toEqual([]);
    }
  });

  test("model-producing operators suggest prediction and testing on the model port", () => {
    for (const trainer of ["SklearnTrainingLogisticRegression", "SklearnLogisticRegression", "SVCTrainer"]) {
      const { recommendations } = recommendOperators({ operatorType: trainer }, CATALOG);
      expect(recommendations.map(r => r.operatorType)).toEqual(["SklearnPrediction", "SklearnTesting"]);
      expect(recommendations.map(r => r.targetPortID)).toEqual(["input-0", "input-0"]);
    }
  });

  test("prediction-producing operators suggest the scorer first", () => {
    expect(typesFor("SklearnPrediction")[0]).toBe("Scorer");
    expect(typesFor("HuggingFaceSentimentAnalysis")[0]).toBe("Scorer");
  });

  test("Sklearn Testing outputs metrics, so it suggests charting them", () => {
    expect(typesFor("SklearnTesting")).toEqual(["BarChart", "Projection"]);
  });

  test("groups with no rule fall back to default successors", () => {
    expect(typesFor("If")).toEqual(["Filter", "Projection", "PythonUDFV2"]);
  });

  test("operators the catalog does not know yield no suggestions", () => {
    expect(typesFor("TotallyMadeUpOp")).toEqual([]);
    // Even with a per-operator rule, since its ports cannot be checked.
    expect(typesFor("CSVFileScan", catalogWith(["Filter", "Projection"]))).toEqual([]);
  });

  test("respects and clamps the limit", () => {
    expect(typesFor("Filter", CATALOG, 1)).toHaveLength(1);
    expect(typesFor("PostgreSQLSource", CATALOG, 99).length).toBeLessThanOrEqual(MAX_OPERATOR_RECOMMENDATION_LIMIT);
    expect(typesFor("Filter", CATALOG, 0)).toHaveLength(1);
  });

  test("filters out suggestions the catalog does not know", () => {
    // Catalog knows Filter and Projection, but none of Filter's other successors.
    expect(typesFor("Filter", catalogWith(["Filter", "Projection"]))).toEqual(["Projection"]);
  });

  test("enriches suggestions with userFriendlyName when the catalog has it", () => {
    const { recommendations } = recommendOperators({ operatorType: "CSVFileScan" }, CATALOG);
    expect(recommendations[1].userFriendlyName).toBe("Projection");
    expect(recommendations[2].userFriendlyName).toBe("Keyword Search");
  });

  test("without an initialized catalog, suggests nothing", () => {
    for (const catalog of [null, catalogFrom(OPERATOR_CATALOG_FIXTURE, false)]) {
      for (const operatorType of ["Filter", "BarChart", "SklearnTrainingLogisticRegression"]) {
        expect(typesFor(operatorType, catalog)).toEqual([]);
      }
    }
  });

  test("skips suggestions with no input port to wire to", () => {
    expect(typesFor("Filter", catalogPatching("Aggregate", { inputPorts: [] }))).toEqual(["Projection", "Sort"]);
    // Unless the operator lets users add input ports.
    const dynamic = catalogPatching("Aggregate", { inputPorts: [], dynamicInputPorts: true });
    expect(typesFor("Filter", dynamic)).toEqual(["Aggregate", "Projection", "Sort"]);
  });

  test("never suggests the operator itself", () => {
    // In a group with no rule, so it gets the fallback, which lists it.
    const catalog = catalogPatching("PythonUDFV2", { operatorGroupName: "Control Block" });
    expect(typesFor("PythonUDFV2", catalog)).toEqual(["Filter", "Projection"]);
  });

  test("suggests for any output port the operator has, and none for others", () => {
    const forPort = (operatorType: string, sourcePortID: string) =>
      recommendOperators({ operatorType, sourcePortID }, CATALOG).recommendations;
    expect(forPort("Split", "output-1").length).toBeGreaterThan(0);
    expect(forPort("Filter", "output-1")).toEqual([]);
    expect(forPort("Filter", "input-0")).toEqual([]);
    // Port IDs are canonical: a leading zero does not name port 0.
    expect(forPort("Filter", "output-00")).toEqual([]);
  });

  test("empty operatorType yields no suggestions", () => {
    expect(recommendOperators({ operatorType: "   " }, CATALOG).recommendations).toEqual([]);
  });
});

describe("targetPortIdFor", () => {
  const prediction = OPERATOR_CATALOG_FIXTURE.operators.find(
    op => op.operatorType === "SklearnPrediction"
  )!.additionalMetadata;

  test("wires a trained model to the target's model port", () => {
    expect(targetPortIdFor("model", prediction)).toBe("input-0");
  });

  test("wires data rows to the target's first port that does not take a model", () => {
    expect(targetPortIdFor("transform", prediction)).toBe("input-1");
  });

  test("defaults to the first input port", () => {
    expect(targetPortIdFor("transform", undefined)).toBe("input-0");
    expect(targetPortIdFor("model", { inputPorts: [{}] })).toBe("input-0");
  });
});

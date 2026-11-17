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
 * Hardcoded "what usually comes next" table for the Version 1 operator
 * recommender (apache/texera#5240). Kept deliberately small and readable: a
 * coarse category per operator, a ranked successor list per category, plus a
 * handful of specific overrides for the most common dataflow patterns.
 *
 * Categories come from the operator catalog's metadata (each operator's group
 * and ports), not from its type name, so new operators are classified by the
 * group they are registered under. Operator type strings here must match the
 * `@JsonSubTypes` names registered on `LogicalOp`. Suggestions are validated
 * against the live operator catalog before being returned, so a stale entry
 * here degrades to "not suggested" rather than to a broken suggestion.
 */

/** The subset of an operator's catalog metadata the rules read. */
export interface OperatorCatalogEntry {
  userFriendlyName?: string;
  operatorGroupName?: string;
  inputPorts?: { displayName?: string }[];
  outputPorts?: { displayName?: string }[];
  dynamicOutputPorts?: boolean;
}

export type OperatorCategory =
  | "source"
  | "transform"
  | "aggregate"
  | "sort"
  | "join"
  | "udf"
  // Trains a model and outputs it as a row, rather than predictions.
  | "model"
  // Outputs the input rows with predicted values added.
  | "prediction"
  // Outputs evaluation metrics.
  | "metrics"
  | "visualization";

/**
 * Category per catalog group name (`OperatorGroupConstants` on the backend).
 * Groups absent here, such as Control Block, fall back to
 * {@link DEFAULT_SUCCESSORS}.
 */
const GROUP_CATEGORY: Record<string, OperatorCategory> = {
  "Data Input": "source",
  "Database Connector": "source",
  "External API": "source",
  Search: "transform",
  "Data Cleaning": "transform",
  Utilities: "transform",
  Aggregate: "aggregate",
  Sort: "sort",
  Join: "join",
  Set: "join",
  "User-defined Functions": "udf",
  Python: "udf",
  Java: "udf",
  R: "udf",
  "Hugging Face": "prediction",
  "Machine Learning General": "metrics",
  // Visualizations output rendered HTML, so nothing downstream can use it.
  Visualization: "visualization",
  Basic: "visualization",
  Statistical: "visualization",
  Scientific: "visualization",
  Financial: "visualization",
  Media: "visualization",
  Advanced: "visualization",
};

/**
 * Scikit-learn groups. Their operators either train a model (and output it) or
 * take a trained model on an input port named "model" (and output predictions).
 */
const SKLEARN_GROUPS = new Set(["Sklearn", "Sklearn Training", "Advanced Sklearn"]);

/** Display name of the input port that takes a trained model. */
const MODEL_PORT_NAME = "model";

/**
 * Operators whose output does not follow from their group and ports. Sklearn
 * Testing takes a model like Sklearn Prediction does, but outputs metrics.
 */
const TYPE_CATEGORY: Record<string, OperatorCategory> = {
  SklearnTesting: "metrics",
};

/** Ranked successor list per category, most likely first. */
const CATEGORY_SUCCESSORS: Record<OperatorCategory, string[]> = {
  source: ["Filter", "Projection", "TypeCasting", "KeywordSearch"],
  transform: ["Aggregate", "Projection", "Sort", "PythonUDFV2"],
  aggregate: ["BarChart", "LineChart", "Sort"],
  sort: ["BarChart", "Projection", "Limit"],
  join: ["Projection", "Aggregate", "Filter"],
  udf: ["Filter", "Projection", "Aggregate"],
  model: ["SklearnPrediction", "SklearnTesting"],
  prediction: ["Scorer", "Filter", "Projection"],
  metrics: ["BarChart", "Projection"],
  // Visualizations are terminal: no successor is suggested.
  visualization: [],
};

/**
 * Per-operator overrides for the highest-value flows. Takes precedence over the
 * category successors above, and applies even when the catalog is unavailable.
 */
const SPECIFIC_SUCCESSORS: Record<string, string[]> = {
  CSVFileScan: ["Filter", "Projection", "KeywordSearch"],
  JSONLFileScan: ["Projection", "Filter", "UnnestString"],
  Filter: ["Aggregate", "Projection", "Sort"],
  Projection: ["Aggregate", "Filter", "Sort"],
  KeywordSearch: ["Aggregate", "Projection", "Filter"],
};

/** Fallback successors for operators with no category-specific rule. */
export const DEFAULT_SUCCESSORS: string[] = ["Filter", "Projection", "PythonUDFV2"];

/**
 * Short rationale per recommended (target) operator, describing why it is a
 * sensible next step. Falls back to a generic phrase for anything not listed.
 */
const RATIONALE: Record<string, string> = {
  Filter: "Keep only the rows you care about",
  Projection: "Select, drop, or rename columns",
  TypeCasting: "Convert column types before further processing",
  KeywordSearch: "Search a text column for keywords",
  Aggregate: "Summarize the data with group-by aggregations",
  Sort: "Order the rows by one or more columns",
  Limit: "Keep only the first N rows",
  BarChart: "Visualize the results as a bar chart",
  LineChart: "Visualize the results as a line chart",
  PythonUDFV2: "Run custom Python logic on the rows",
  UnnestString: "Split a delimited string column into rows",
  SklearnPrediction: "Predict labels for new rows with the trained model",
  SklearnTesting: "Measure how well the trained model performs on test data",
  Scorer: "Evaluate model predictions against labels",
};

const GENERIC_RATIONALE = "A common next step for this operator";

function modelPortIndex(entry: OperatorCatalogEntry | undefined): number {
  return entry?.inputPorts?.findIndex(port => port.displayName === MODEL_PORT_NAME) ?? -1;
}

/**
 * Category for an operator from its catalog metadata. Returns `undefined` when
 * the operator has no metadata (catalog unavailable or unknown type) or sits in
 * a group with no rule, so the caller can fall back to {@link DEFAULT_SUCCESSORS}.
 */
export function categoryOf(
  operatorType: string,
  entry: OperatorCatalogEntry | undefined
): OperatorCategory | undefined {
  const override = TYPE_CATEGORY[operatorType];
  if (override) return override;
  if (!entry) return undefined;

  const group = entry.operatorGroupName ?? "";
  if (SKLEARN_GROUPS.has(group)) {
    return modelPortIndex(entry) >= 0 ? "prediction" : "model";
  }

  const category = GROUP_CATEGORY[group];
  if (category === "visualization") return category;
  // Anything that reads no input produces data from outside the workflow.
  if (entry.inputPorts?.length === 0) return "source";
  return category;
}

/**
 * Ordered candidate successors for an operator, before catalog validation and
 * limit truncation. Specific overrides win over category defaults, which win
 * over the generic fallback; visualizations return an empty list.
 */
export function candidateSuccessors(operatorType: string, category: OperatorCategory | undefined): string[] {
  const specific = SPECIFIC_SUCCESSORS[operatorType];
  if (specific) return specific;
  if (category) return CATEGORY_SUCCESSORS[category];
  return DEFAULT_SUCCESSORS;
}

/**
 * Input port of the suggested operator that the source should be wired to. A
 * trained model goes to the target's "model" port; any other data goes to the
 * target's first port that does not expect a model.
 */
export function targetPortIdFor(
  sourceCategory: OperatorCategory | undefined,
  target: OperatorCatalogEntry | undefined
): string {
  const modelIndex = modelPortIndex(target);
  if (modelIndex < 0) return "input-0";
  if (sourceCategory === "model") return `input-${modelIndex}`;
  const dataIndex = target!.inputPorts!.findIndex((_, i) => i !== modelIndex);
  return `input-${Math.max(dataIndex, 0)}`;
}

/** Human-readable rationale for suggesting `targetType`. */
export function rationaleFor(targetType: string): string {
  return RATIONALE[targetType] ?? GENERIC_RATIONALE;
}

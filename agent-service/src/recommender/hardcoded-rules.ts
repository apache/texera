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
 * Operator type strings here must match the `@JsonSubTypes` names registered on
 * `LogicalOp` (the catalog the frontend and compiler use). Suggestions are
 * validated against the live operator catalog before being returned, so a stale
 * entry here degrades to "not suggested" rather than to a broken suggestion.
 */

export type OperatorCategory = "source" | "transform" | "aggregate" | "join" | "rowOps" | "udf" | "ml" | "sink";

/**
 * Explicit category for operators whose successors don't follow from a naming
 * heuristic. Operators absent here fall back to {@link inferCategory}.
 */
const OPERATOR_CATEGORY: Record<string, OperatorCategory> = {
  // Sources — read data into the workflow.
  CSVFileScan: "source",
  CSVOldFileScan: "source",
  ParallelCSVFileScan: "source",
  JSONLFileScan: "source",
  FileScan: "source",
  ArrowSource: "source",
  TextInput: "source",
  URLFetcher: "source",
  FileLister: "source",
  PostgreSQLSource: "source",
  MySQLSource: "source",
  AsterixDBSource: "source",
  SQLSource: "source",
  TwitterSearch: "source",
  TwitterFullArchiveSearch: "source",
  RedditSearch: "source",
  PythonUDFSourceV2: "source",
  RUDFSource: "source",

  // Transforms — reshape rows/columns.
  Filter: "transform",
  Projection: "transform",
  TypeCasting: "transform",
  Regex: "transform",
  KeywordSearch: "transform",
  SubstringSearch: "transform",
  DictionaryMatcher: "transform",
  UnnestString: "transform",
  Split: "transform",

  // Aggregations / reducers.
  Aggregate: "aggregate",
  PythonTableReducer: "aggregate",
  Scorer: "aggregate",

  // Joins and set operations (multi-input).
  HashJoin: "join",
  IntervalJoin: "join",
  CartesianProduct: "join",
  Union: "join",
  Intersect: "join",
  Difference: "join",
  SymmetricDifference: "join",

  // Ordering / sampling / row-count operators.
  Sort: "rowOps",
  SortPartitions: "rowOps",
  StableMergeSort: "rowOps",
  Limit: "rowOps",
  Distinct: "rowOps",
  RandomKSampling: "rowOps",
  ReservoirSampling: "rowOps",

  // User-defined functions.
  PythonUDFV2: "udf",
  DualInputPortsPythonUDFV2: "udf",
  PythonLambdaFunction: "udf",
  JavaUDF: "udf",
  RUDF: "udf",
};

/** Ranked successor list per category, most-likely first. */
const CATEGORY_SUCCESSORS: Record<OperatorCategory, string[]> = {
  source: ["Filter", "Projection", "TypeCasting", "KeywordSearch"],
  transform: ["Aggregate", "Projection", "Sort", "PythonUDFV2"],
  aggregate: ["BarChart", "LineChart", "Sort"],
  join: ["Projection", "Aggregate", "Filter"],
  rowOps: ["Aggregate", "Projection", "BarChart"],
  udf: ["Filter", "Projection", "Aggregate"],
  ml: ["Scorer", "PythonUDFV2"],
  // Sinks (charts / visualizers) are terminal: no successor is suggested.
  sink: [],
};

/**
 * Per-operator overrides for the highest-value flows. Takes precedence over the
 * category successors above.
 */
const SPECIFIC_SUCCESSORS: Record<string, string[]> = {
  CSVFileScan: ["Filter", "Projection", "KeywordSearch"],
  JSONLFileScan: ["Projection", "Filter", "UnnestString"],
  Filter: ["Aggregate", "Projection", "Sort"],
  Projection: ["Aggregate", "Filter", "Sort"],
  KeywordSearch: ["Aggregate", "Projection", "Filter"],
  Sort: ["BarChart", "Projection", "Limit"],
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
  Scorer: "Evaluate model predictions against labels",
};

const GENERIC_RATIONALE = "A common next step for this operator";

/** Suffixes that reliably identify terminal visualization/sink operators. */
const SINK_SUFFIXES = ["Chart", "Plot", "Visualizer"];

/** Explicit sink operators whose names don't end in a {@link SINK_SUFFIXES}. */
const EXTRA_SINKS = new Set([
  "Histogram",
  "Histogram2D",
  "HeatMap",
  "NetworkGraph",
  "SankeyDiagram",
  "NestedTable",
  "FigureFactoryTable",
  "TablesPlot",
]);

/**
 * Best-effort category for an operator not listed in {@link OPERATOR_CATEGORY},
 * using naming heuristics. Returns `undefined` when nothing matches so the
 * caller can fall back to {@link DEFAULT_SUCCESSORS}.
 */
export function inferCategory(operatorType: string): OperatorCategory | undefined {
  if (isSink(operatorType)) return "sink";
  if (operatorType.startsWith("Sklearn") || operatorType.startsWith("HuggingFace")) return "ml";
  if (operatorType.endsWith("Source")) return "source";
  if (operatorType.endsWith("FileScan")) return "source";
  if (operatorType.endsWith("UDF") || operatorType.endsWith("UDFV2")) return "udf";
  return undefined;
}

/** Whether an operator is a terminal visualization/sink with no useful successor. */
export function isSink(operatorType: string): boolean {
  if (EXTRA_SINKS.has(operatorType)) return true;
  if (OPERATOR_CATEGORY[operatorType] === "sink") return true;
  return SINK_SUFFIXES.some(suffix => operatorType.endsWith(suffix));
}

/** Category for an operator, combining the explicit table and heuristics. */
export function categoryOf(operatorType: string): OperatorCategory | undefined {
  return OPERATOR_CATEGORY[operatorType] ?? inferCategory(operatorType);
}

/**
 * Ordered candidate successors for an operator, before catalog validation and
 * limit truncation. Specific overrides win over category defaults, which win
 * over the generic fallback; terminal sinks return an empty list.
 */
export function candidateSuccessors(operatorType: string): string[] {
  const specific = SPECIFIC_SUCCESSORS[operatorType];
  if (specific) return specific;

  const category = categoryOf(operatorType);
  if (category) return CATEGORY_SUCCESSORS[category];

  return DEFAULT_SUCCESSORS;
}

/** Human-readable rationale for suggesting `targetType`. */
export function rationaleFor(targetType: string): string {
  return RATIONALE[targetType] ?? GENERIC_RATIONALE;
}

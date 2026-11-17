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

import type { OperatorMetadata, OperatorSchema } from "../api/backend-api";

/**
 * A slice of the backend's `/api/resources/operator-metadata` response for the
 * operator recommender tests. Each entry mirrors the group, display name, and
 * ports its descriptor registers on the backend; schemas are left empty since
 * the recommender only checks that one exists.
 */
function op(
  operatorType: string,
  userFriendlyName: string,
  operatorGroupName: string,
  inputPorts: string[],
  outputPortCount = 1
): OperatorSchema {
  return {
    operatorType,
    jsonSchema: { type: "object", properties: {} },
    operatorVersion: "test",
    additionalMetadata: {
      userFriendlyName,
      operatorGroupName,
      inputPorts: inputPorts.map(displayName => (displayName ? { displayName } : {})),
      outputPorts: Array.from({ length: outputPortCount }, () => ({})),
    },
  };
}

export const OPERATOR_CATALOG_FIXTURE: OperatorMetadata = {
  groups: [],
  operators: [
    op("CSVFileScan", "CSV File Scan", "Data Input", []),
    op("JSONLFileScan", "JSONL File Scan", "Data Input", []),
    op("PostgreSQLSource", "PostgreSQL Source", "Database Connector", []),
    op("PythonUDFSourceV2", "1-out Python UDF", "Python", []),
    op("Filter", "Filter", "Data Cleaning", [""]),
    op("Projection", "Projection", "Data Cleaning", [""]),
    op("TypeCasting", "Type Casting", "Data Cleaning", [""]),
    op("Limit", "Limit", "Data Cleaning", [""]),
    op("KeywordSearch", "Keyword Search", "Search", [""]),
    op("UnnestString", "Unnest String", "Utilities", [""]),
    op("Split", "Split", "Utilities", [""], 2),
    op("Aggregate", "Aggregate", "Aggregate", [""]),
    op("Sort", "Sort", "Sort", [""]),
    op("HashJoin", "Hash Join", "Join", ["left", "right"]),
    op("PythonUDFV2", "Python UDF", "Python", [""]),
    op("If", "If", "Control Block", ["Condition", ""], 2),
    op("BarChart", "Bar Chart", "Basic", [""]),
    op("LineChart", "Line Chart", "Basic", [""]),
    op("Scatterplot", "Scatter Plot", "Basic", [""]),
    op("WordCloud", "Word Cloud", "Media", [""]),
    op("SklearnLogisticRegression", "Logistic Regression", "Sklearn", ["training", "testing"]),
    op("SklearnTrainingLogisticRegression", "Training: Logistic Regression", "Sklearn Training", ["training"]),
    op("SVCTrainer", "SVM Classifier", "Advanced Sklearn", ["training", "parameter"]),
    op("SklearnPrediction", "Sklearn Prediction", "Sklearn", ["model", ""]),
    op("SklearnTesting", "Sklearn Testing", "Sklearn", ["model", "data"]),
    op("HuggingFaceSentimentAnalysis", "Hugging Face Sentiment Analysis", "Hugging Face", [""]),
    op("Scorer", "Machine Learning Scorer", "Machine Learning General", [""]),
  ],
};

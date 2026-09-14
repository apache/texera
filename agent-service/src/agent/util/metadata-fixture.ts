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

// Shared operator-metadata fixture for unit tests. Seeds a WorkflowSystemMetadata
// instance with a small, deterministic set of operators (a source, a transform,
// and a Python UDF) so tests do not need a live backend.

import { WorkflowSystemMetadata } from "./workflow-system-metadata";
import type { OperatorMetadata, OperatorSchema } from "../../api/backend-api";

interface PortShape {
  inputPorts: Record<string, unknown>[];
  outputPorts: Record<string, unknown>[];
}

function operator(operatorType: string, description: string, ports: PortShape, jsonSchema: unknown): OperatorSchema {
  return {
    operatorType,
    jsonSchema,
    operatorVersion: "1.0",
    additionalMetadata: {
      userFriendlyName: operatorType,
      operatorGroupName: "Test",
      operatorDescription: description,
      inputPorts: ports.inputPorts,
      outputPorts: ports.outputPorts,
    },
  };
}

export const FIXTURE_METADATA: OperatorMetadata = {
  operators: [
    operator(
      "CSVFileScan",
      "Load CSV data",
      { inputPorts: [], outputPorts: [{}] },
      {
        type: "object",
        properties: {
          fileName: { type: "string" },
          delimiter: { type: "string", default: "," },
        },
        required: ["fileName"],
        additionalProperties: false,
      }
    ),
    operator(
      "Filter",
      "Filter rows",
      { inputPorts: [{}], outputPorts: [{}] },
      {
        type: "object",
        properties: { predicate: { type: "string" } },
        required: [],
        additionalProperties: false,
      }
    ),
    operator(
      "PythonUDFV2",
      "Run Python code",
      { inputPorts: [{}], outputPorts: [{}] },
      {
        type: "object",
        properties: { code: { type: "string" } },
        required: [],
        additionalProperties: false,
      }
    ),
  ],
  groups: [],
};

export function makeMetadataFixture(): WorkflowSystemMetadata {
  const store = new WorkflowSystemMetadata();
  store.loadFromMetadata(FIXTURE_METADATA);
  return store;
}

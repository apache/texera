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
import { extractOperatorInputPortSchemaMap, WorkflowUtilService } from "./workflow-utils";
import { makeMetadataFixture } from "./metadata-fixture";
import { WorkflowState } from "../workflow-state";
import type { OperatorPredicate, OperatorLink, OperatorPortSchemaMap } from "../../types/workflow";

function targetOperator(): OperatorPredicate {
  return {
    operatorID: "tgt",
    operatorType: "Filter",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [{ portID: "input-0", displayName: "Input 0" }],
    outputPorts: [{ portID: "output-0", displayName: "Output 0" }],
    showAdvanced: false,
  };
}

function link(): OperatorLink {
  return {
    linkID: "l1",
    source: { operatorID: "src", portID: "output-0" },
    target: { operatorID: "tgt", portID: "input-0" },
  };
}

describe("extractOperatorInputPortSchemaMap", () => {
  test("resolves the upstream output schema onto the matching input port", () => {
    const outputSchemas: Record<string, OperatorPortSchemaMap> = {
      src: { "0_false": [{ attributeName: "a", attributeType: "string" }] },
    };

    const result = extractOperatorInputPortSchemaMap("tgt", targetOperator(), outputSchemas, [link()]);

    expect(result).toBeDefined();
    expect(result!["0_false"]).toEqual([{ attributeName: "a", attributeType: "string" }]);
  });

  test("returns undefined when the operator has no inbound links", () => {
    expect(extractOperatorInputPortSchemaMap("tgt", targetOperator(), {}, [])).toBeUndefined();
  });

  test("returns undefined when the upstream operator has no known schema", () => {
    // Link exists but there is no entry for "src" in outputSchemas.
    expect(extractOperatorInputPortSchemaMap("tgt", targetOperator(), {}, [link()])).toBeUndefined();
  });
});

describe("WorkflowUtilService.getNewOperatorPredicate", () => {
  test("materializes a source operator with schema defaults and metadata ports", () => {
    const service = new WorkflowUtilService(makeMetadataFixture(), new WorkflowState());

    const op = service.getNewOperatorPredicate("CSVFileScan");

    expect(op.operatorType).toBe("CSVFileScan");
    expect(op.operatorID).toBe("CSVFileScan-operator-1");
    expect(op.inputPorts).toHaveLength(0); // source: no input ports
    expect(op.outputPorts).toHaveLength(1);
    // Ajv useDefaults populates the default delimiter.
    expect(op.operatorProperties.delimiter).toBe(",");
    // No custom name -> falls back to the friendly name.
    expect(op.customDisplayName).toBe("CSVFileScan");
  });

  test("applies a custom display name when provided", () => {
    const service = new WorkflowUtilService(makeMetadataFixture(), new WorkflowState());
    const op = service.getNewOperatorPredicate("Filter", "Drop nulls");
    expect(op.customDisplayName).toBe("Drop nulls");
    expect(op.inputPorts).toHaveLength(1);
  });

  test("throws for an operator type absent from metadata", () => {
    const service = new WorkflowUtilService(makeMetadataFixture(), new WorkflowState());
    expect(() => service.getNewOperatorPredicate("DoesNotExist")).toThrow(/doesn't exist in operator metadata/);
  });
});

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
import { extractOperatorInputPortSchemaMap } from "./workflow-utils";
import type { OperatorLink, OperatorPredicate, PortSchema } from "../../types/workflow";

const TARGET = "target";

function operatorWithInputPorts(count: number): OperatorPredicate {
  return {
    operatorID: TARGET,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: Array.from({ length: count }, (_, i) => ({ portID: `input-${i}` })),
    outputPorts: [],
    showAdvanced: false,
  };
}

function link(linkID: string, sourceOp: string, sourcePort: string, targetPort: string): OperatorLink {
  return {
    linkID,
    source: { operatorID: sourceOp, portID: sourcePort },
    target: { operatorID: TARGET, portID: targetPort },
  };
}

const schemaA: PortSchema = [{ attributeName: "a", attributeType: "string" }];
const schemaB: PortSchema = [{ attributeName: "b", attributeType: "integer" }];
const sourceOperator = (operatorID: string): OperatorPredicate => ({
  ...operatorWithInputPorts(0),
  operatorID,
  outputPorts: [{ portID: "output-0" }],
});

describe("extractOperatorInputPortSchemaMap", () => {
  test("returns undefined when the operator has no input links", () => {
    expect(extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(1), {}, [], [])).toBeUndefined();
  });

  test("maps a single input port to its resolved upstream schema", () => {
    const links = [link("l0", "src", "output-0", "input-0")];
    const outputSchemas = { src: { "0_false": schemaA } };
    expect(
      extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(1), outputSchemas, links, [
        sourceOperator("src"),
      ])
    ).toEqual({
      "0_false": schemaA,
    });
  });

  test("uses saved native port positions after removing middle input and output ports", () => {
    const target = { ...operatorWithInputPorts(2), inputPorts: [{ portID: "input-0" }, { portID: "input-2" }] };
    const source = {
      ...operatorWithInputPorts(0),
      operatorID: "src",
      outputPorts: [{ portID: "output-0" }, { portID: "output-2" }],
    };
    const outputSchemas = { src: { "0_false": schemaA, "1_false": schemaB } };
    const links = [link("gap", "src", "output-2", "input-2")];
    const operators = [source, target];
    const before = JSON.stringify({ operators, links, outputSchemas });

    expect(extractOperatorInputPortSchemaMap(TARGET, target, outputSchemas, links, operators)).toEqual({
      "0_false": undefined,
      "1_false": schemaB,
    });
    expect(JSON.stringify({ operators, links, outputSchemas })).toBe(before);
  });

  test("returns undefined when the only source operator is absent from outputSchemas", () => {
    const links = [link("l0", "missing", "output-0", "input-0")];
    expect(
      extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(1), {}, links, [sourceOperator("missing")])
    ).toBeUndefined();
  });

  test("does not infer a missing source or an undeclared target port from its numeric suffix", () => {
    const target = operatorWithInputPorts(1);
    const schemas = { src: { "0_false": schemaA } };
    expect(
      extractOperatorInputPortSchemaMap(TARGET, target, schemas, [link("missing", "src", "output-0", "input-0")], [])
    ).toBeUndefined();
    expect(
      extractOperatorInputPortSchemaMap(
        TARGET,
        target,
        schemas,
        [link("unknown", "src", "output-0", "input-1")],
        [sourceOperator("src")]
      )
    ).toBeUndefined();
  });

  test("keys each input port separately and routes links by target port number", () => {
    const links = [link("l0", "srcA", "output-0", "input-0"), link("l1", "srcB", "output-0", "input-1")];
    const outputSchemas = {
      srcA: { "0_false": schemaA },
      srcB: { "0_false": schemaB },
    };
    expect(
      extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(2), outputSchemas, links, [
        sourceOperator("srcA"),
        sourceOperator("srcB"),
      ])
    ).toEqual({
      "0_false": schemaA,
      "1_false": schemaB,
    });
  });

  test("leaves a port undefined when its source operator is absent from outputSchemas", () => {
    const links = [link("l0", "srcA", "output-0", "input-0"), link("l1", "missing", "output-0", "input-1")];
    const outputSchemas = { srcA: { "0_false": schemaA } };
    expect(
      extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(2), outputSchemas, links, [
        sourceOperator("srcA"),
        sourceOperator("missing"),
      ])
    ).toEqual({
      "0_false": schemaA,
      "1_false": undefined,
    });
  });

  test("leaves a port undefined when the source portID is unparseable", () => {
    const links = [link("l0", "srcA", "output-0", "input-0"), link("l1", "srcB", "not-a-port", "input-1")];
    const outputSchemas = {
      srcA: { "0_false": schemaA },
      srcB: { "0_false": schemaB },
    };
    expect(
      extractOperatorInputPortSchemaMap(TARGET, operatorWithInputPorts(2), outputSchemas, links, [
        sourceOperator("srcA"),
        sourceOperator("srcB"),
      ])
    ).toEqual({
      "0_false": schemaA,
      "1_false": undefined,
    });
  });
});

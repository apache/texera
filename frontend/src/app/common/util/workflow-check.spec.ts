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
import { testWorkflow, testLink, testOperator } from "../testing/workflow-graph";
import { DEFAULT_WORKFLOW } from "../../workspace/service/workflow-graph/model/workflow-action.service";
import { checkIfWorkflowBroken } from "./workflow-check";
import { assertWorkflowIDs } from "@texera/workflow-macros";
import { graphIDCases } from "../../../../../common/workflow-core/src/test/typescript/macro-test-graph";

describe("workflow graph identities", () => {
  for (const example of graphIDCases) {
    it(example.name, () => {
      const before = JSON.stringify(example.content);
      if (example.error) expect(() => assertWorkflowIDs(example.content)).toThrow(example.error);
      else expect(() => assertWorkflowIDs(example.content)).not.toThrow();
      expect(JSON.stringify(example.content)).toBe(before);
    });
  }
});

describe("checkIfWorkflowBroken", () => {
  const operators = ["source", "target"].map(id => testOperator(id));

  it.each([
    { scenario: "connected operators", links: [testLink("source", "target")], broken: false },
    { scenario: "missing source", links: [testLink("missing", "target")], broken: true },
    { scenario: "missing target", links: [testLink("source", "missing")], broken: true },
    { scenario: "no links", links: [], broken: false },
  ])("checks links for $scenario", ({ links, broken }) => {
    const content = testWorkflow(operators, links);
    expect(checkIfWorkflowBroken({ ...DEFAULT_WORKFLOW, content })).toBe(broken);
  });
});

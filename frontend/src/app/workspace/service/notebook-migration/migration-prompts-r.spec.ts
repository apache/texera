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

import {
  R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION,
  R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT,
  R_MAPPING_PROMPT,
  R_SCRIPT_MAPPING_PROMPT,
} from "./migration-prompts-r";

interface Conversion {
  code: Record<string, string>;
  edges: [string, string][];
  outputs: Record<string, string[]>;
}

function fencedBlock(prompt: string, language: string): string {
  const match = prompt.match(new RegExp("```" + language + "\\n([\\s\\S]*?)\\n```"));
  expect(match).not.toBeNull();
  return match![1];
}

function mappingIn(prompt: string): Record<string, unknown> {
  return JSON.parse(prompt.slice(prompt.indexOf("{"), prompt.lastIndexOf("}") + 1));
}

describe("R migration prompts", () => {
  [
    ["notebook", R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION],
    ["script", R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT],
  ].forEach(([input, example]) => {
    describe(`${input} worked example`, () => {
      const conversion: Conversion = JSON.parse(fencedBlock(example, "json"));
      const udfIds = Object.keys(conversion.code);
      const edgeSources = new Set(conversion.edges.map(([source]) => source));

      it("only references UDFs it defines", () => {
        conversion.edges.flat().forEach(udfId => expect(udfIds).toContain(udfId));
        expect(Object.keys(conversion.outputs).sort()).toEqual([...udfIds].sort());
      });

      it("writes every UDF as a numbered Table API function", () => {
        udfIds.forEach(udfId => {
          const code = conversion.code[udfId];
          expect(code.startsWith(`# ${udfId}\n`)).toBe(true);
          expect(code).toContain("function(table, port)");
          expect(code).not.toContain("coro");
        });
      });

      it("serializes only the outputs another UDF reads", () => {
        udfIds.forEach(udfId => {
          expect(/(?<!un)serialize\(/.test(conversion.code[udfId])).toBe(edgeSources.has(udfId));
        });
      });

      it("gives each UDF at most one upstream UDF", () => {
        const targets = conversion.edges.map(([, target]) => target);
        expect(new Set(targets).size).toBe(targets.length);
      });
    });
  });

  it("maps the notebook example's UDFs to cells it contains", () => {
    const notebook = fencedBlock(R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION, "r");
    Object.values(mappingIn(R_MAPPING_PROMPT) as Record<string, string[]>)
      .flat()
      .forEach(cell => expect(notebook).toContain(`# START ${cell}\n`));
  });

  it("maps the script example's UDFs to non-blank lines it contains", () => {
    const lines = fencedBlock(R_EXAMPLE_OF_MULTIPLE_UDF_CONVERSION_SCRIPT, "r").split("\n");
    Object.values(mappingIn(R_SCRIPT_MAPPING_PROMPT) as Record<string, [number, number][]>)
      .flat()
      .forEach(([first, last]) => {
        expect(first).toBeLessThanOrEqual(last);
        expect(last).toBeLessThanOrEqual(lines.length);
        expect(lines[first - 1]).toMatch(new RegExp(`^ *${first}\\| +\\S`));
        expect(lines[last - 1]).toMatch(new RegExp(`^ *${last}\\| +\\S`));
      });
  });
});

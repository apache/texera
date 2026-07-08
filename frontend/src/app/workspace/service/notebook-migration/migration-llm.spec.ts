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

import { NotebookMigrationLLM, Notebook } from "./migration-llm";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { generateText } from "ai";
import type { Mock } from "vitest";

// The LLM transport and OpenAI client are mocked so the tests exercise only the
// deterministic transformation (parsing, operator/edge construction, cell<->operator mapping).
vi.mock("ai", () => ({ generateText: vi.fn() }));
vi.mock("@ai-sdk/openai", () => ({
  createOpenAI: vi.fn(() => ({ chat: vi.fn(() => ({})) })),
}));

const mockGenerateText = generateText as unknown as Mock;

describe("NotebookMigrationLLM", () => {
  let opIdCounter = 0;
  let stubUtil: WorkflowUtilService;

  // Build a fresh, initialized session with stubbed dependencies. The stubbed
  // getNewOperatorPredicate hands out deterministic ids (PythonUDFV2-0, -1, ...).
  function makeLLM(): NotebookMigrationLLM {
    const stubConfig = {
      env: {
        pythonNotebookMigrationEnabled: true,
        defaultDataTransferBatchSize: 400,
        defaultExecutionMode: "PIPELINED",
      },
    } as unknown as GuiConfigService;

    stubUtil = {
      getNewOperatorPredicate: vi.fn((operatorType: string, customDisplayName?: string) => ({
        operatorID: `${operatorType}-${opIdCounter++}`,
        operatorType,
        operatorVersion: "test-version",
        operatorProperties: { workers: 1, defaultEnv: true, envName: "" },
        inputPorts: [{ portID: "input-0", disallowMultiInputs: false }],
        outputPorts: [{ portID: "output-0" }],
        showAdvanced: false,
        isDisabled: false,
        customDisplayName,
        dynamicInputPorts: true,
        dynamicOutputPorts: true,
      })),
    } as unknown as WorkflowUtilService;

    const llm = new NotebookMigrationLLM(stubConfig, stubUtil);
    // Pass an explicit token so tests don't depend on AuthService/localStorage state.
    llm.initialize("gpt-5-mini", "test-token");
    return llm;
  }

  function codeCell(uuid: string | undefined, source: string) {
    return { cell_type: "code", metadata: uuid === undefined ? {} : { uuid }, source };
  }

  // Queue the two responses convertNotebookToWorkflow consumes, in order.
  function mockResponses(workflowResponse: string, mappingResponse: string) {
    mockGenerateText.mockResolvedValueOnce({ text: workflowResponse }).mockResolvedValueOnce({ text: mappingResponse });
  }

  beforeEach(() => {
    opIdCounter = 0;
    mockGenerateText.mockReset();
  });

  describe("convertNotebookToWorkflow", () => {
    it("builds operators, links, positions, and a bidirectional mapping", async () => {
      const notebook: Notebook = {
        cells: [codeCell("CELL1", "print(1)"), codeCell("CELL2", "print(2)")],
      };
      mockResponses(
        JSON.stringify({
          code: { UDF1: "code1", UDF2: "code2" },
          edges: [["UDF1", "UDF2"]],
          outputs: { UDF1: ["a", "b"], UDF2: ["c"] },
        }),
        JSON.stringify({ UDF1: ["CELL1"], UDF2: ["CELL2"] })
      );

      const { workflowJSON, workflowNotebookMapping } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      expect(workflowJSON.operators.map((op: any) => op.operatorID)).toEqual(["PythonUDFV2-0", "PythonUDFV2-1"]);
      expect(workflowJSON.operators[0].operatorProperties).toMatchObject({
        code: "code1",
        retainInputColumns: false,
      });
      expect(workflowJSON.operatorPositions).toEqual({
        "PythonUDFV2-0": { x: 140, y: 0 },
        "PythonUDFV2-1": { x: 280, y: 0 },
      });
      expect(workflowJSON.links).toHaveLength(1);
      expect(workflowJSON.links[0].source).toEqual({ operatorID: "PythonUDFV2-0", portID: "output-0" });
      expect(workflowJSON.links[0].target).toEqual({ operatorID: "PythonUDFV2-1", portID: "input-0" });
      expect(workflowNotebookMapping.operator_to_cell).toEqual({
        "PythonUDFV2-0": ["CELL1"],
        "PythonUDFV2-1": ["CELL2"],
      });
      expect(workflowNotebookMapping.cell_to_operator).toEqual({
        CELL1: ["PythonUDFV2-0"],
        CELL2: ["PythonUDFV2-1"],
      });
      // Settings come from GUI config defaults, not hardcoded values.
      expect(workflowJSON.settings).toEqual({ dataTransferBatchSize: 400, executionMode: "PIPELINED" });
    });

    // Intermediate UDFs (a source of some edge) keep "binary" for object passing; terminal
    // UDFs (no outgoing edge) default to "string" so the result panel renders typed values.
    it("types intermediate UDF outputs as binary and terminal UDF outputs as string", async () => {
      const notebook: Notebook = { cells: [codeCell("CELL1", "a"), codeCell("CELL2", "b")] };
      mockResponses(
        JSON.stringify({
          code: { UDF1: "code1", UDF2: "code2" },
          edges: [["UDF1", "UDF2"]],
          outputs: { UDF1: ["x"], UDF2: ["y"] },
        }),
        JSON.stringify({ UDF1: ["CELL1"], UDF2: ["CELL2"] })
      );

      const { workflowJSON } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      // UDF1 is a source (intermediate) -> binary; UDF2 is terminal -> string.
      expect(workflowJSON.operators[0].operatorProperties.outputColumns).toEqual([
        { attributeName: "x", attributeType: "binary" },
      ]);
      expect(workflowJSON.operators[1].operatorProperties.outputColumns).toEqual([
        { attributeName: "y", attributeType: "string" },
      ]);
    });

    it("maps multiple cells onto the same UDF, and one cell onto multiple UDFs", async () => {
      const notebook: Notebook = {
        cells: [codeCell("CELL1", "a"), codeCell("CELL2", "b")],
      };
      mockResponses(
        JSON.stringify({ code: { UDF1: "c1", UDF2: "c2" }, edges: [], outputs: {} }),
        JSON.stringify({ UDF1: ["CELL1", "CELL2"], UDF2: ["CELL1"] })
      );

      const { workflowNotebookMapping } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      expect(workflowNotebookMapping.operator_to_cell).toEqual({
        "PythonUDFV2-0": ["CELL1", "CELL2"],
        "PythonUDFV2-1": ["CELL1"],
      });
      expect(workflowNotebookMapping.cell_to_operator).toEqual({
        CELL1: ["PythonUDFV2-0", "PythonUDFV2-1"],
        CELL2: ["PythonUDFV2-0"],
      });
    });

    it("skips (with a warning) an edge that references an unknown UDF id", async () => {
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const notebook: Notebook = { cells: [codeCell("CELL1", "a")] };
      mockResponses(
        JSON.stringify({ code: { UDF1: "c1" }, edges: [["UDF1", "UDFX"]], outputs: {} }),
        JSON.stringify({ UDF1: ["CELL1"] })
      );

      const { workflowJSON } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      // The dangling edge is dropped rather than producing an undefined endpoint.
      expect(workflowJSON.links).toEqual([]);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("UDFX"));
      warn.mockRestore();
    });

    it("skips (with a warning) a mapping entry that references an unknown UDF id", async () => {
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const notebook: Notebook = { cells: [codeCell("CELL1", "a")] };
      mockResponses(
        JSON.stringify({ code: { UDF1: "c1" }, edges: [], outputs: {} }),
        JSON.stringify({ UDF1: ["CELL1"], UDFTYPO: ["CELL1"] })
      );

      const { workflowNotebookMapping } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      // Only the valid UDF id survives in the mapping.
      expect(workflowNotebookMapping.operator_to_cell).toEqual({ "PythonUDFV2-0": ["CELL1"] });
      expect(workflowNotebookMapping.cell_to_operator).toEqual({ CELL1: ["PythonUDFV2-0"] });
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("UDFTYPO"));
      warn.mockRestore();
    });

    it("handles empty code, edges, and outputs", async () => {
      const notebook: Notebook = { cells: [] };
      mockResponses(JSON.stringify({ code: {}, edges: [], outputs: {} }), JSON.stringify({}));

      const { workflowJSON, workflowNotebookMapping } = JSON.parse(await makeLLM().convertNotebookToWorkflow(notebook));

      expect(workflowJSON.operators).toEqual([]);
      expect(workflowJSON.links).toEqual([]);
      expect(workflowNotebookMapping.operator_to_cell).toEqual({});
      expect(workflowNotebookMapping.cell_to_operator).toEqual({});
    });

    it("rejects when a code cell is missing metadata.uuid", async () => {
      const notebook: Notebook = { cells: [codeCell(undefined, "print(1)")] };

      await expect(makeLLM().convertNotebookToWorkflow(notebook)).rejects.toThrow(/metadata\.uuid/);
      // It fails before prompting, so the LLM is never called.
      expect(mockGenerateText).not.toHaveBeenCalled();
    });

    it("joins array-form cell source (nbformat lines) without inserting commas", async () => {
      const notebook: Notebook = {
        cells: [
          {
            cell_type: "code",
            metadata: { uuid: "CELL1" },
            source: ["import pandas as pd\n", "x = 1\n"],
          },
        ],
      };
      mockResponses(
        JSON.stringify({ code: { UDF1: "c1" }, edges: [], outputs: {} }),
        JSON.stringify({ UDF1: ["CELL1"] })
      );

      await makeLLM().convertNotebookToWorkflow(notebook);

      const allPromptContent = mockGenerateText.mock.calls
        .flatMap(call => call[0].messages.map((m: any) => m.content))
        .join("\n");
      expect(allPromptContent).toContain("import pandas as pd\nx = 1\n");
      expect(allPromptContent).not.toContain("import pandas as pd\n,");
    });

    it("resets conversation history between conversions so a prior notebook does not leak", async () => {
      const llm = makeLLM();

      // First conversion (notebook AAA) on the instance.
      mockResponses(
        JSON.stringify({ code: { UDF1: "codeAAA" }, edges: [], outputs: {} }),
        JSON.stringify({ UDF1: ["AAA"] })
      );
      await llm.convertNotebookToWorkflow({ cells: [codeCell("AAA", "a = 1")] });

      // Second conversion (notebook BBB) on the SAME instance, no close()/initialize() between.
      mockResponses(
        JSON.stringify({ code: { UDF1: "codeBBB" }, edges: [], outputs: {} }),
        JSON.stringify({ UDF1: ["BBB"] })
      );
      await llm.convertNotebookToWorkflow({ cells: [codeCell("BBB", "b = 2")] });

      // The 3rd generateText call is the workflow prompt of the second conversion.
      const secondConversionMessages = mockGenerateText.mock.calls[2][0].messages.map((m: any) => m.content).join("\n");

      expect(secondConversionMessages).toContain("# START BBB");
      expect(secondConversionMessages).not.toContain("AAA");
      expect(secondConversionMessages).not.toContain("codeAAA");
    });
  });

  describe("parseJsonResponse", () => {
    // parseJsonResponse is private; cast to access it directly for focused coverage.
    const parse = (raw: string) => (makeLLM() as any).parseJsonResponse(raw, "workflow");

    it("parses bare JSON", () => {
      expect(parse('{"a":1}')).toEqual({ a: 1 });
    });

    it("strips a ```json fence", () => {
      expect(parse('```json\n{"a":1}\n```')).toEqual({ a: 1 });
    });

    it("strips a plain ``` fence", () => {
      expect(parse('```\n{"a":1}\n```')).toEqual({ a: 1 });
    });

    it("tolerates surrounding whitespace and newlines around the fence", () => {
      expect(parse('\n\n  ```json\n{"a":1}\n```  \n\n')).toEqual({ a: 1 });
    });

    it("throws a contextual error on malformed JSON", () => {
      expect(() => parse("not json")).toThrow("Failed to parse LLM workflow response as JSON");
    });

    it("extracts fenced JSON even when surrounded by prose", () => {
      expect(parse('Here is the JSON: ```json\n{"a":1}\n```\nThanks!')).toEqual({ a: 1 });
    });

    it("extracts the outermost object from fence-less prose", () => {
      expect(parse('Sure! {"a":1} hope that helps')).toEqual({ a: 1 });
    });
  });
});

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "../core/api-client";
import { WorkflowRepository } from "./workflow-repository";
import { NotebookGenerator, parseGenerationFiles } from "./notebook-generation";

const content = {
  operators: [],
  links: [],
  commentBoxes: [],
  operatorPositions: {},
  settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
};
const notebook = { cells: [{ cell_type: "code", metadata: {}, source: "print('hello')" }] };
describe("React notebook/script/folder generation", () => {
  it("tags notebook cells and rejects missing cells, malformed JSON and empty Python input", async () => {
    const parsed = await parseGenerationFiles([new File([JSON.stringify(notebook)], "demo.ipynb")]);
    expect(parsed.kind).toBe("notebook");
    if (parsed.kind === "notebook") expect(parsed.notebook.cells[0].metadata.uuid).toBeTypeOf("string");
    await expect(parseGenerationFiles([new File(["{}"], "bad.ipynb")])).rejects.toThrow("notebook");
    await expect(parseGenerationFiles([new File(["broken"], "bad.ipynb")])).rejects.toThrow();
    await expect(parseGenerationFiles([new File(["  "], "empty.py")])).rejects.toThrow("empty");
    await expect(parseGenerationFiles([new File(["x"], "unsupported.txt")])).rejects.toThrow(".ipynb");
  });
  it("assembles Python folders with original exclusions, bounds and deterministic order", async () => {
    function file(path: string, text: string) {
      const file = new File([text], path.split("/").pop()!);
      Object.defineProperty(file, "webkitRelativePath", { value: path });
      return file;
    }
    const parsed = await parseGenerationFiles(
      [file("project/b.py", "b = 2"), file("project/a.py", "a = 1"), file("project/venv/site.py", "excluded")],
      true
    );
    expect(parsed.kind).toBe("folder");
    if (parsed.kind === "folder") {
      expect(parsed.folder.files.map(item => item.path)).toEqual(["a.py", "b.py"]);
      expect(parsed.folder.source).not.toContain("excluded");
      expect(parsed.name).toBe("project");
    }
    await expect(parseGenerationFiles([], true)).rejects.toThrow();
  });
  it("saves the generated workflow before its notebook mapping, caps names and always closes the LLM", async () => {
    const calls: string[] = [];
    const fetch = vi.fn(async (path: string) => {
      calls.push(path);
      if (path === "/api/workflow/create")
        return Response.json({
          workflow: { wid: 4, name: "generated", content },
          ownerId: 7,
          isOwner: true,
          accessLevel: "WRITE",
        });
      return Response.json({ success: true });
    });
    const api = new ApiClient({ fetch });
    const engine = {
      initialize: vi.fn(),
      verifyConnection: vi.fn().mockResolvedValue(true),
      convertNotebookToWorkflow: vi.fn().mockResolvedValue(
        JSON.stringify({
          workflowJSON: content,
          workflowNotebookMapping: { operator_to_cell: {}, cell_to_operator: {} },
        })
      ),
      convertScriptToWorkflow: vi.fn(),
      convertFolderToWorkflow: vi.fn(),
      close: vi.fn(),
    };
    const generator = new NotebookGenerator(
      api,
      new WorkflowRepository(api),
      { pythonNotebookMigrationEnabled: true },
      { createLLM: () => engine, operatorFactory: { getNewOperatorPredicate: vi.fn() } }
    );
    const result = await generator.generate({ kind: "notebook", notebook, name: "A".repeat(200) }, "test-model");
    expect(result.wid).toBe(4);
    expect(calls).toEqual(["/api/workflow/create", "/api/notebook-migration/store-notebook-and-mapping"]);
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body)).name).toHaveLength(128);
    expect(engine.convertNotebookToWorkflow).toHaveBeenCalledWith(notebook, "python");
    expect(engine.close).toHaveBeenCalledTimes(1);
  });
  it("passes the enabled R flag and explicit language to notebook and script conversion", async () => {
    const api = new ApiClient({
      fetch: vi.fn(async (path: string) =>
        path.endsWith("/create") ? Response.json({ workflow: { wid: 4, content } }) : Response.json({ success: true })
      ),
    });
    const rNotebook = { cells: [{ cell_type: "code", metadata: {}, source: 'print("こんにちは")' }] };
    const converted = {
      workflowJSON: content,
      workflowNotebookMapping: { operator_to_cell: {}, cell_to_operator: {} },
    };
    const engine = {
      initialize: vi.fn(),
      verifyConnection: vi.fn().mockResolvedValue(true),
      convertNotebookToWorkflow: vi.fn().mockResolvedValue(JSON.stringify(converted)),
      convertScriptToWorkflow: vi.fn().mockResolvedValue({ ...converted, notebook: rNotebook }),
      convertFolderToWorkflow: vi.fn(),
      close: vi.fn(),
    };
    const createLLM = vi.fn(() => engine);
    const generator = new NotebookGenerator(
      api,
      new WorkflowRepository(api),
      { pythonNotebookMigrationEnabled: true, rNotebookMigrationEnabled: true },
      { createLLM, operatorFactory: { getNewOperatorPredicate: vi.fn() } }
    );
    await expect(
      generator.generate({ kind: "notebook", notebook: rNotebook, name: "r" }, "test", "r")
    ).resolves.toEqual({
      wid: 4,
    });
    await expect(
      generator.generate({ kind: "script", source: rNotebook.cells[0].source, name: "r" }, "test", "r")
    ).resolves.toEqual({ wid: 4 });
    expect(createLLM).toHaveBeenCalledWith(
      expect.objectContaining({ env: expect.objectContaining({ rNotebookMigrationEnabled: true }) }),
      expect.anything()
    );
    expect(engine.convertNotebookToWorkflow).toHaveBeenCalledWith(rNotebook, "r");
    expect(engine.convertScriptToWorkflow).toHaveBeenCalledWith(rNotebook.cells[0].source, "r");
  });
  it.each([false, undefined])("rejects R conversion before making a request when the flag is %s", async flag => {
    const fetch = vi.fn();
    const createLLM = vi.fn();
    const api = new ApiClient({ fetch });
    const generator = new NotebookGenerator(
      api,
      new WorkflowRepository(api),
      { pythonNotebookMigrationEnabled: true, rNotebookMigrationEnabled: flag },
      { createLLM }
    );
    await expect(generator.generate({ kind: "notebook", notebook, name: "r" }, "test", "r")).rejects.toThrow(
      "R notebook migration is disabled"
    );
    await expect(generator.generate({ kind: "script", source: "x <- 1", name: "r" }, "test", "r")).rejects.toThrow(
      "R notebook migration is disabled"
    );
    expect(fetch).not.toHaveBeenCalled();
    expect(createLLM).not.toHaveBeenCalled();
  });
  it("retains the created workflow when notebook attachment fails and closes failed sessions", async () => {
    const api = new ApiClient({
      fetch: vi.fn(async (path: string) =>
        path.endsWith("/create")
          ? Response.json({ workflow: { wid: 4, name: "generated", content } })
          : new Response("offline", { status: 503 })
      ),
    });
    const engine = {
      initialize: vi.fn(),
      verifyConnection: vi.fn().mockResolvedValue(true),
      convertNotebookToWorkflow: vi.fn(),
      convertScriptToWorkflow: vi
        .fn()
        .mockResolvedValue({ workflowJSON: content, workflowNotebookMapping: {}, notebook }),
      convertFolderToWorkflow: vi.fn(),
      close: vi.fn(),
    };
    const generator = new NotebookGenerator(
      api,
      new WorkflowRepository(api),
      { pythonNotebookMigrationEnabled: true },
      { createLLM: () => engine, operatorFactory: { getNewOperatorPredicate: vi.fn() } }
    );
    await expect(
      generator.generate({ kind: "script", source: "print(1)", name: "demo" }, "test")
    ).resolves.toMatchObject({ wid: 4, warning: expect.stringContaining("could not be attached") });
    engine.verifyConnection.mockResolvedValue(false);
    await expect(generator.generate({ kind: "script", source: "print(1)", name: "demo" }, "test")).rejects.toThrow(
      "LLM backend"
    );
    expect(engine.close).toHaveBeenCalledTimes(2);
  });
  it("does not request models or generate when the deployment feature is disabled", async () => {
    const fetch = vi.fn();
    const api = new ApiClient({ fetch });
    const generator = new NotebookGenerator(api, new WorkflowRepository(api), {
      pythonNotebookMigrationEnabled: false,
    });
    await expect(generator.models()).resolves.toEqual([]);
    await expect(generator.generate({ kind: "script", source: "print(1)", name: "demo" }, "test")).rejects.toThrow(
      "disabled"
    );
    expect(fetch).not.toHaveBeenCalled();
  });
});

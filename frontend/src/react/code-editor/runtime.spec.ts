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

import { afterEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { mountCodeEditor, coeditorCursorCss, languageServerUrl } from "./runtime";
import type { MonacoPlatform } from "./platform";

afterEach(() => vi.useRealTimers());

function fixture() {
  const model = {
    getLineCount: () => 2,
    tokenization: { forceTokenization: vi.fn() },
    getValue: () => "value",
    getOffsetAt: () => 0,
  };
  const editor = {
    getModel: () => model,
    updateOptions: vi.fn(),
    layout: vi.fn(),
    trigger: vi.fn(),
    addAction: vi.fn(() => ({ dispose: vi.fn() })),
  };
  const app = {
    start: vi.fn().mockResolvedValue(undefined),
    getEditor: () => editor,
    dispose: vi.fn().mockResolvedValue(undefined),
  };
  const client = { start: vi.fn().mockResolvedValue(undefined), dispose: vi.fn().mockResolvedValue(undefined) };
  const binding = { destroy: vi.fn() };
  const platform = {
    startServices: vi.fn().mockResolvedValue(undefined),
    createApp: vi.fn(() => app),
    createLanguageClient: vi.fn(() => client),
    bind: vi.fn(() => binding),
  };
  const doc = new Y.Doc();
  const code = doc.getText("code");
  code.insert(0, "first\nsecond");
  const options = {
    host: document.createElement("div"),
    operatorId: "PythonUDF-id",
    code,
    language: "python" as const,
    readOnly: false,
  };
  return {
    model,
    editor,
    app,
    client,
    binding,
    platform,
    doc,
    options,
    load: async () => platform as unknown as MonacoPlatform,
  };
}

describe("Monaco runtime", () => {
  it("starts the existing editor stack, binds shared text and tokenizes every line", async () => {
    const test = fixture();
    const mounted = await mountCodeEditor(test.options, test.load);
    expect(test.platform.startServices).toHaveBeenCalledOnce();
    expect(test.platform.createApp.mock.calls[0][0].codeResources.modified).toMatchObject({
      text: "first\nsecond",
      uri: expect.stringMatching(/\.py$/),
    });
    expect(test.app.start).toHaveBeenCalledWith(test.options.host);
    expect(test.platform.bind).toHaveBeenCalledWith(test.options.code, test.model, test.editor, undefined);
    expect(test.model.tokenization.forceTokenization.mock.calls).toEqual([[1], [2]]);
    mounted.updateReadOnly(true);
    expect(test.editor.updateOptions).toHaveBeenLastCalledWith({ readOnly: true });
    mounted.undo();
    mounted.redo();
    expect(test.editor.trigger.mock.calls.map(call => call[1])).toEqual(["undo", "redo"]);
    await mounted.dispose();
    await mounted.dispose();
    expect(test.binding.destroy).toHaveBeenCalledOnce();
    expect(test.client.dispose).toHaveBeenCalledOnce();
    expect(test.app.dispose).toHaveBeenCalledOnce();
    test.doc.destroy();
  });

  it("keeps the editor usable on language-server failure and times out a hung connection", async () => {
    vi.useFakeTimers();
    const test = fixture();
    test.client.start.mockImplementation(() => new Promise(() => {}));
    const mounting = mountCodeEditor(test.options, test.load);
    await vi.advanceTimersByTimeAsync(1001);
    const mounted = await mounting;
    expect(test.platform.bind).toHaveBeenCalledOnce();
    expect(test.client.dispose).toHaveBeenCalledOnce();
    await mounted.dispose();
    expect(vi.getTimerCount()).toBe(0);
    test.doc.destroy();
  });

  it("does not connect Python services for another language or a historical read-only diff", async () => {
    for (const override of [{ language: "r" as const }, { originalCode: "old code" }]) {
      const test = fixture();
      const mounted = await mountCodeEditor({ ...test.options, ...override }, test.load);
      expect(test.platform.createLanguageClient).not.toHaveBeenCalled();
      if ("originalCode" in override) {
        expect(test.platform.createApp.mock.calls[0][0]).toMatchObject({
          useDiffEditor: true,
          diffEditorOptions: { readOnly: true },
        });
        expect(test.platform.bind).not.toHaveBeenCalled();
      }
      await mounted.dispose();
      test.doc.destroy();
    }
  });

  it("disposes a late editor start when the host closes before it finishes", async () => {
    const test = fixture();
    let ready!: () => void;
    test.app.start.mockImplementation(
      () =>
        new Promise<void>(resolve => {
          ready = resolve;
        })
    );
    const controller = new AbortController();
    const mounting = mountCodeEditor({ ...test.options, signal: controller.signal }, test.load);
    await vi.waitFor(() => expect(test.app.start).toHaveBeenCalledOnce());
    controller.abort();
    ready();
    await expect(mounting).rejects.toMatchObject({ name: "AbortError" });
    expect(test.app.dispose).toHaveBeenCalledOnce();
    expect(test.platform.bind).not.toHaveBeenCalled();
    test.doc.destroy();
  });

  it("cleans up partially initialized resources if editor startup fails", async () => {
    const test = fixture();
    test.app.start.mockRejectedValue(new Error("editor unavailable"));
    await expect(mountCodeEditor(test.options, test.load)).rejects.toThrow("editor unavailable");
    expect(test.app.dispose).toHaveBeenCalledOnce();
    test.doc.destroy();
  });

  it("uses secure same-origin websockets and validates the configured direct language-server port", () => {
    expect(languageServerUrl("https://texera.example/path", "")).toBe("wss://texera.example/python-language-server");
    expect(languageServerUrl("http://localhost:4200/", "9000")).toBe("ws://localhost:9000/python-language-server");
    expect(() => languageServerUrl("https://texera.example", "bad")).toThrow("Invalid language-server port");
  });

  it("limits coeditor cursor styles to numeric identities and safe colors", () => {
    expect(coeditorCursorCss([{ clientId: "12", color: "rgba(1, 2, 3, 0.8)" }])).toContain(".yRemoteSelection-12");
    expect(
      coeditorCursorCss([
        { clientId: "x</style>", color: "red" },
        { clientId: "12", color: "red; background:url(x)" },
      ])
    ).toBe("");
  });
});

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

import { describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { ApiClient } from "../core/api-client";
import { createCodeEditorAi, attachAnnotationActions } from "./ai-assistant";
import type { MonacoEditor } from "./platform";

describe("code editor AI annotations", () => {
  it("uses the existing service bodies and unwraps validated annotation/range responses", async () => {
    const fetch = vi.fn(async url =>
      url.endsWith("isenabled")
        ? new Response("OpenAI")
        : url.endsWith("annotationresult")
          ? Response.json({ choices: [{ message: { content: " : int " } }] })
          : Response.json({
              underlying: {
                result: {
                  value: [
                    {
                      underlying: {
                        name: { value: "x" },
                        startLine: { value: 1 },
                        startColumn: { value: 1 },
                        endLine: { value: 1 },
                        endColumn: { value: 2 },
                      },
                    },
                  ],
                },
              },
            })
    );
    const ai = createCodeEditorAi(new ApiClient({ fetch }));
    expect(await ai.enabled()).toBe(true);
    expect(await ai.annotate({ code: "x", lineNumber: 1, allcode: "x = 1" })).toBe(": int");
    expect(await ai.locate({ selectedCode: "x", startLine: 1 })).toEqual([
      { name: "x", startLine: 1, startColumn: 1, endLine: 1, endColumn: 2 },
    ]);
    expect(fetch.mock.calls.map(call => call[0])).toEqual([
      "/api/aiassistant/isenabled",
      "/api/aiassistant/annotationresult",
      "/api/aiassistant/annotate-argument",
    ]);
  });

  it("inserts only accepted annotations into shared text and disposes registered editor actions", async () => {
    const doc = new Y.Doc();
    const code = doc.getText("code");
    code.insert(0, "x");
    const actions = new Map<string, { run: () => Promise<void> }>();
    const disposed = vi.fn();
    const range = { startLineNumber: 1, startColumn: 1, endLineNumber: 1, endColumn: 2 };
    const editor = {
      getSelection: () => range,
      getModel: () => ({
        getValue: () => code.toString(),
        getValueInRange: () => "x",
        getOffsetAt: position => position.column - 1,
      }),
      addAction: action => {
        actions.set(action.id, action);
        return { dispose: disposed };
      },
    } as unknown as MonacoEditor;
    const ai = {
      enabled: vi.fn(async () => true),
      annotate: vi.fn(async () => ": int"),
      locate: vi.fn(async () => []),
    };
    const prompt = vi.fn().mockResolvedValueOnce(false).mockResolvedValueOnce(true);
    const binding = await attachAnnotationActions(editor, code, ai, prompt, () => false);
    await actions.get("type-annotation-action")!.run();
    expect(code.toString()).toBe("x");
    await actions.get("type-annotation-action")!.run();
    expect(code.toString()).toBe("x: int");
    binding.dispose();
    expect(disposed).toHaveBeenCalledTimes(2);
    doc.destroy();
  });

  it("rejects malformed server content and does not change a read-only shared model", async () => {
    const ai = createCodeEditorAi(new ApiClient({ fetch: vi.fn(async () => Response.json({ choices: [] })) }));
    await expect(ai.annotate({ code: "x", lineNumber: 1, allcode: "x" })).rejects.toThrow("valid message content");
    const doc = new Y.Doc();
    const code = doc.getText("code");
    code.insert(0, "x");
    const actions = new Map();
    const editor = {
      addAction: action => {
        actions.set(action.id, action);
        return { dispose() {} };
      },
    } as unknown as MonacoEditor;
    const api = { enabled: vi.fn(async () => true), annotate: vi.fn(), locate: vi.fn() };
    const binding = await attachAnnotationActions(editor, code, api, vi.fn(), () => true);
    await actions.get("type-annotation-action").run();
    expect(api.annotate).not.toHaveBeenCalled();
    expect(code.toString()).toBe("x");
    binding.dispose();
    doc.destroy();
  });
});

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
import { attachCodeDebugger, type DebugSnapshot } from "./debugger";
import type { MonacoEditor } from "./platform";

function setup() {
  let snapshot: DebugSnapshot = { enabled: true, breakpoints: [{ line: 2, condition: "x > 1", hit: true }] };
  let changed!: () => void;
  let mouse!: (event: unknown) => void;
  const click = vi.fn();
  const setCondition = vi.fn();
  const request = vi.fn();
  const unsubscribe = vi.fn();
  const source = {
    getSnapshot: () => snapshot,
    subscribe: (listener: () => void) => {
      changed = listener;
      return unsubscribe;
    },
    toggle: click,
    setCondition,
  };
  const editor = {
    onMouseDown: (listener: typeof mouse) => {
      mouse = listener;
      return { dispose: vi.fn() };
    },
  } as unknown as MonacoEditor;
  const library = {
    mouseDownDisposable: { dispose: vi.fn() },
    lineNumberAndDecorationIdMap: new Map([[2, "id"]]),
    createSpecifyDecoration: vi.fn(),
    clearBreakpoints: vi.fn(),
    setLineHighlight: vi.fn(),
    removeHighlight: vi.fn(),
    dispose: vi.fn(),
    createBreakpointDecoration: vi.fn(),
  };
  return {
    source,
    editor,
    library,
    click,
    setCondition,
    request,
    unsubscribe,
    change: (next: DebugSnapshot) => {
      snapshot = next;
      changed();
    },
    mouse: (event: unknown) => mouse(event),
  };
}

describe("Monaco debugger bridge", () => {
  it("renders shared conditions/hits and routes left/right gutter clicks to runtime callbacks", async () => {
    const test = setup();
    const binding = await attachCodeDebugger(test.editor, test.source, test.request, async () => ({
      create: () => test.library,
      gutterType: 2,
    }));
    expect(test.library.createSpecifyDecoration).toHaveBeenCalledWith(expect.objectContaining({ startLineNumber: 2 }));
    expect(test.library.setLineHighlight).toHaveBeenCalledWith(2);
    expect(test.library.createBreakpointDecoration({ startLineNumber: 2 }, 0).options.glyphMarginClassName).toBe(
      "monaco-conditional-breakpoint"
    );
    test.mouse({
      target: { type: 2, detail: { isAfterLines: false }, position: { lineNumber: 2 } },
      event: { leftButton: true },
    });
    expect(test.click).toHaveBeenCalledWith(2);
    test.mouse({
      target: { type: 2, detail: { isAfterLines: false }, position: { lineNumber: 2 } },
      event: { leftButton: false },
    });
    expect(test.request).toHaveBeenCalledWith(2, "x > 1");
    binding.dispose();
    expect(test.library.dispose).toHaveBeenCalledOnce();
    expect(test.unsubscribe).toHaveBeenCalledOnce();
  });

  it("removes interactions while inactive and ignores invalid margin clicks", async () => {
    const test = setup();
    const binding = await attachCodeDebugger(test.editor, test.source, test.request, async () => ({
      create: () => test.library,
      gutterType: 2,
    }));
    test.mouse({
      target: { type: 2, detail: { isAfterLines: true }, position: { lineNumber: 9 } },
      event: { leftButton: true },
    });
    expect(test.click).not.toHaveBeenCalled();
    test.change({ enabled: false, breakpoints: [] });
    expect(test.library.dispose).toHaveBeenCalledOnce();
    binding.dispose();
    expect(test.library.dispose).toHaveBeenCalledOnce();
  });
});

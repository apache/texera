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

import type * as monaco from "monaco-editor";
import type { MonacoEditor } from "./platform";

export interface DebugBreakpoint {
  line: number;
  condition?: string;
  hit?: boolean;
}
export interface DebugSnapshot {
  enabled: boolean;
  breakpoints: readonly DebugBreakpoint[];
}
export interface CodeDebugSource {
  getSnapshot(): DebugSnapshot;
  subscribe(listener: () => void): () => void;
  toggle(line: number): void | Promise<void>;
  setCondition(line: number, condition: string): void | Promise<void>;
}
interface BreakpointApi {
  mouseDownDisposable?: monaco.IDisposable;
  lineNumberAndDecorationIdMap: Map<number, string>;
  createBreakpointDecoration: (
    range: monaco.IRange,
    state: number
  ) => { range: monaco.IRange; options: monaco.editor.IModelDecorationOptions };
  createSpecifyDecoration(range: monaco.IRange): void;
  clearBreakpoints(): void;
  setLineHighlight(line: number): void;
  removeHighlight(): void;
  dispose(): void;
}
interface BreakpointPlatform {
  create(editor: MonacoEditor): BreakpointApi;
  gutterType: number;
}

async function loadBreakpointPlatform(): Promise<BreakpointPlatform> {
  const [{ MonacoBreakpoint }, monaco] = await Promise.all([import("monaco-breakpoints"), import("monaco-editor")]);
  return {
    create: editor =>
      new MonacoBreakpoint({
        editor,
        hoverMessage: {
          added: { value: "Click to remove the breakpoint." },
          unAdded: { value: "Click to add a breakpoint at this line." },
        },
      }) as unknown as BreakpointApi,
    gutterType: monaco.editor.MouseTargetType.GUTTER_GLYPH_MARGIN,
  };
}

/** Keeps the existing breakpoint library's line tracking while runtime owns the shared breakpoint state. */
export async function attachCodeDebugger(
  editor: MonacoEditor,
  source: CodeDebugSource,
  requestCondition: (line: number, condition: string) => void,
  load: () => Promise<BreakpointPlatform> = loadBreakpointPlatform,
  onError?: (message: string) => void,
  signal?: AbortSignal
) {
  const platform = await load();
  let library: BreakpointApi | undefined;
  let disposed = false;
  if (signal?.aborted) return { dispose() {} };
  const stop = () => {
    library?.mouseDownDisposable?.dispose();
    library?.dispose();
    library = undefined;
  };
  const render = () => {
    if (disposed) return;
    const state = source.getSnapshot();
    if (!state.enabled) {
      stop();
      return;
    }
    if (!library) {
      library = platform.create(editor);
      library.createBreakpointDecoration = (range, kind) => {
        const condition = source
          .getSnapshot()
          .breakpoints.find(breakpoint => breakpoint.line === range.startLineNumber)?.condition;
        return {
          range,
          options: {
            glyphMarginClassName:
              kind === 0
                ? condition?.trim()
                  ? "monaco-conditional-breakpoint"
                  : "monaco-breakpoint"
                : "monaco-hover-breakpoint",
          },
        };
      };
      library.mouseDownDisposable?.dispose();
      library.mouseDownDisposable = editor.onMouseDown(event => {
        const target = event.target as monaco.editor.IMouseTargetMargin;
        if (
          disposed ||
          !source.getSnapshot().enabled ||
          target.type !== platform.gutterType ||
          target.detail?.isAfterLines ||
          !target.position
        )
          return;
        const line = target.position.lineNumber;
        if (!Number.isSafeInteger(line) || line < 1) return;
        if (event.event.leftButton) {
          try {
            void Promise.resolve(source.toggle(line)).catch(reason =>
              onError?.(reason instanceof Error ? reason.message : "Breakpoint could not be changed.")
            );
          } catch (reason) {
            onError?.(reason instanceof Error ? reason.message : "Breakpoint could not be changed.");
          }
        } else if (library?.lineNumberAndDecorationIdMap.has(line)) {
          requestCondition(
            line,
            source.getSnapshot().breakpoints.find(breakpoint => breakpoint.line === line)?.condition ?? ""
          );
        }
      });
    }
    library.clearBreakpoints();
    library.removeHighlight();
    for (const breakpoint of state.breakpoints) {
      if (!Number.isSafeInteger(breakpoint.line) || breakpoint.line < 1) continue;
      library.createSpecifyDecoration({
        startLineNumber: breakpoint.line,
        startColumn: 1,
        endLineNumber: breakpoint.line,
        endColumn: 1,
      });
      if (breakpoint.hit) library.setLineHighlight(breakpoint.line);
    }
  };
  const unsubscribe = source.subscribe(render);
  render();
  return {
    dispose() {
      if (disposed) return;
      disposed = true;
      unsubscribe();
      stop();
    },
  };
}

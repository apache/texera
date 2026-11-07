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
import * as Y from "yjs";
import type { ApiClient } from "../core/api-client";
import type { MonacoEditor } from "./platform";

export interface UnannotatedArgument {
  name: string;
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}
export interface AnnotationProposal {
  code: string;
  suggestion: string;
  range: monaco.IRange;
}
export interface CodeEditorAi {
  enabled(signal?: AbortSignal): Promise<boolean>;
  annotate(input: { code: string; lineNumber: number; allcode: string }, signal?: AbortSignal): Promise<string>;
  locate(input: { selectedCode: string; startLine: number }, signal?: AbortSignal): Promise<UnannotatedArgument[]>;
}

export function createCodeEditorAi(api: Pick<ApiClient, "get" | "post">): CodeEditorAi {
  return {
    async enabled(signal) {
      try {
        return (await api.get("aiassistant/isenabled", { responseType: "text", signal })) === "OpenAI";
      } catch {
        return false;
      }
    },
    async annotate(input, signal) {
      const result = await api.post<{ choices?: { message?: { content?: unknown } }[] }>(
        "aiassistant/annotationresult",
        input,
        { signal }
      );
      const content = result?.choices?.[0]?.message?.content;
      if (typeof content !== "string" || !content.trim())
        throw new Error("OpenAI response does not contain valid message content");
      return content.trim();
    },
    async locate(input, signal) {
      const result = await api.post<{
        underlying?: { result?: { value?: { underlying?: Record<string, { value?: unknown }> }[] } };
      }>("aiassistant/annotate-argument", input, { signal });
      const rows = result?.underlying?.result?.value;
      if (!Array.isArray(rows)) throw new Error("Invalid unannotated argument response");
      return rows.map(row => {
        const data = row.underlying;
        const candidate = Object.fromEntries(
          ["name", "startLine", "startColumn", "endLine", "endColumn"].map(key => [key, data?.[key]?.value])
        );
        if (
          typeof candidate.name !== "string" ||
          !["startLine", "startColumn", "endLine", "endColumn"].every(
            key => Number.isSafeInteger(candidate[key]) && Number(candidate[key]) > 0
          )
        )
          throw new Error("Invalid unannotated argument response");
        return candidate as unknown as UnannotatedArgument;
      });
    },
  };
}

/** Annotation acceptance writes through Y.Text; relative anchors survive intervening coeditor edits. */
export async function attachAnnotationActions(
  editor: MonacoEditor,
  code: Y.Text,
  ai: CodeEditorAi,
  prompt: (proposal: AnnotationProposal) => Promise<boolean>,
  readOnly: () => boolean,
  onError?: (message: string) => void,
  signal?: AbortSignal
) {
  const controller = new AbortController();
  const actions: monaco.IDisposable[] = [];
  let disposed = false;
  let busy = false;
  const annotate = async (selected: string, range: monaco.IRange): Promise<number> => {
    const model = editor.getModel();
    if (!model || disposed || readOnly()) return 0;
    const anchor = Y.createRelativePositionFromTypeIndex(
      code,
      model.getOffsetAt({ lineNumber: range.endLineNumber, column: range.endColumn })
    );
    const suggestion = await ai.annotate(
      { code: selected, lineNumber: range.startLineNumber, allcode: model.getValue() },
      controller.signal
    );
    if (disposed) return 0;
    const accepted = await prompt({ code: selected, suggestion, range });
    if (!accepted || disposed || readOnly() || !code.doc) return 0;
    const position = Y.createAbsolutePositionFromRelativePosition(anchor, code.doc);
    if (!position) return 0;
    code.insert(position.index, suggestion);
    return suggestion.length;
  };
  const run = async (all: boolean) => {
    if (disposed || busy || readOnly()) return;
    const selection = editor.getSelection();
    const model = editor.getModel();
    if (!selection || !model) return;
    busy = true;
    try {
      if (!all) await annotate(model.getValueInRange(selection), selection);
      else {
        const argumentsToAnnotate = await ai.locate(
          { selectedCode: model.getValueInRange(selection), startLine: selection.startLineNumber },
          controller.signal
        );
        let priorLine: number | undefined;
        let offset = 0;
        for (const argument of argumentsToAnnotate) {
          if (disposed || readOnly()) break;
          if (priorLine !== argument.startLine) offset = 0;
          const range = {
            startLineNumber: argument.startLine,
            startColumn: argument.startColumn + offset,
            endLineNumber: argument.endLine,
            endColumn: argument.endColumn + offset,
          };
          const highlight = editor.createDecorationsCollection?.([
            {
              range,
              options: { hoverMessage: { value: "Argument without Annotation" }, className: "annotation-highlight" },
            },
          ]);
          try {
            offset += await annotate(argument.name, range);
          } finally {
            highlight?.clear();
          }
          priorLine = argument.startLine;
        }
      }
    } catch (reason) {
      if (!disposed) onError?.(reason instanceof Error ? reason.message : "Type annotation failed.");
    } finally {
      busy = false;
    }
  };
  const enabled = await ai.enabled(controller.signal);
  if (signal?.aborted) {
    controller.abort();
    return { dispose() {} };
  }
  if (enabled)
    actions.push(
      editor.addAction({
        id: "type-annotation-action",
        label: "Add Type Annotation",
        contextMenuGroupId: "1_modification",
        contextMenuOrder: 1,
        run: () => run(false),
      })
    );
  actions.push(
    editor.addAction({
      id: "all-type-annotation-action",
      label: "Add All Type Annotations",
      contextMenuGroupId: "1_modification",
      contextMenuOrder: 1.1,
      run: () => run(true),
    })
  );
  return {
    dispose() {
      if (disposed) return;
      disposed = true;
      controller.abort();
      actions.forEach(action => action.dispose());
    },
  };
}

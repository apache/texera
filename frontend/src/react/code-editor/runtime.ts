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

import type { EditorAppConfig } from "monaco-languageclient/editorApp";
import type { Awareness } from "y-protocols/awareness";
import type { Text as YText } from "yjs";
import type { EditorApplication, LanguageClient, MonacoEditor, MonacoPlatform } from "./platform";

export interface CodeEditorOptions {
  host: HTMLElement;
  operatorId: string;
  language: "python" | "r" | "java" | "javascript";
  code: YText;
  awareness?: Awareness;
  readOnly?: boolean;
  originalCode?: string;
  pythonLanguageServerPort?: string;
  signal?: AbortSignal;
  onLanguageServerError?: (message: string) => void;
}
export interface MountedCodeEditor {
  editor: MonacoEditor | undefined;
  updateReadOnly(readOnly: boolean): void;
  layout(): void;
  undo(): void;
  redo(): void;
  dispose(): Promise<void>;
}
const suffixes = { python: ".py", r: ".r", java: ".java", javascript: ".js" };

export function languageServerUrl(base: string, port = ""): string {
  const url = new URL("/python-language-server", base);
  if (port !== "") {
    if (!/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535)
      throw new Error("Invalid language-server port");
    url.port = port;
  }
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function coeditorCursorCss(coeditors: readonly { clientId: string; color?: string }[]): string {
  return coeditors
    .filter(
      peer =>
        /^\d{1,10}$/.test(peer.clientId) &&
        peer.color &&
        /^(?:#[0-9a-fA-F]{3,8}|rgba?\([\d.,\s]+\)|hsla?\([\d.,%\s]+\))$/.test(peer.color)
    )
    .map(
      peer =>
        `.yRemoteSelection-${peer.clientId}{background-color:${peer.color!.replace("0.8", "0.5")}}.yRemoteSelectionHead-${peer.clientId}::after{border-color:${peer.color}}.yRemoteSelectionHead-${peer.clientId}{border-color:${peer.color}}`
    )
    .join("\n");
}

/** Mount completion always owns disposal, including failed and superseded asynchronous starts. */
export async function mountCodeEditor(
  options: CodeEditorOptions,
  load: () => Promise<MonacoPlatform> = async () => (await import("./platform")).loadMonacoPlatform()
): Promise<MountedCodeEditor> {
  const aborted = () => {
    if (options.signal?.aborted) throw new DOMException("Operation aborted", "AbortError");
  };
  aborted();
  let app: EditorApplication | undefined;
  let client: LanguageClient | undefined;
  let binding: { destroy(): void } | undefined;
  let disposed = false;
  const dispose = async () => {
    if (disposed) return;
    disposed = true;
    binding?.destroy();
    binding = undefined;
    const pending = [client?.dispose(), app?.dispose()];
    client = undefined;
    app = undefined;
    await Promise.allSettled(pending);
  };
  try {
    const platform = await load();
    aborted();
    await platform.startServices();
    aborted();
    const file = `in-memory-${options.operatorId}${suffixes[options.language]}`;
    const config: EditorAppConfig = {
      codeResources: { modified: { text: options.code.toString(), uri: file } },
      editorOptions: { readOnly: options.readOnly === true, automaticLayout: true, glyphMargin: true },
    };
    const diff = options.originalCode !== undefined;
    if (diff) {
      config.codeResources!.original = {
        text: options.originalCode!,
        uri: `in-memory-${options.operatorId}-version${suffixes[options.language]}`,
      };
      config.useDiffEditor = true;
      config.diffEditorOptions = { readOnly: true };
    }
    app = platform.createApp(config);
    await app.start(options.host);
    aborted();
    const editor = app.getEditor();
    if (!diff && editor) {
      if (options.language === "python") {
        client = platform.createLanguageClient({
          languageId: "python",
          webSocketUrl: languageServerUrl(document.baseURI, options.pythonLanguageServerPort),
        });
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            client.start(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => reject(new Error("Language server connection timed out")), 1000);
            }),
          ]);
        } catch (error) {
          options.onLanguageServerError?.(error instanceof Error ? error.message : "Language server is unavailable.");
          await client.dispose().catch(() => undefined);
          client = undefined;
        } finally {
          if (timer !== undefined) clearTimeout(timer);
        }
        aborted();
      }
      editor.updateOptions({ readOnly: options.readOnly === true });
      const model = editor.getModel();
      if (model) {
        binding = platform.bind(options.code, model, editor, options.awareness);
        const tokenization = (model as unknown as { tokenization?: { forceTokenization?: (line: number) => void } })
          .tokenization;
        if (tokenization?.forceTokenization)
          for (let line = 1; line <= model.getLineCount(); line++) tokenization.forceTokenization(line);
      }
    }
    return {
      editor,
      updateReadOnly: value => {
        if (!disposed && !diff) editor?.updateOptions({ readOnly: value });
      },
      layout: () => {
        if (!disposed) {
          if (diff) app?.getDiffEditor?.()?.layout();
          else editor?.layout();
        }
      },
      undo: () => {
        if (!disposed && !diff) editor?.trigger("texera", "undo", null);
      },
      redo: () => {
        if (!disposed && !diff) editor?.trigger("texera", "redo", null);
      },
      dispose,
    };
  } catch (error) {
    await dispose();
    throw error;
  }
}

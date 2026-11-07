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
import type { EditorAppConfig } from "monaco-languageclient/editorApp";
import type { Awareness } from "y-protocols/awareness";
import type { Text as YText } from "yjs";

export type MonacoEditor = monaco.editor.IStandaloneCodeEditor;
export interface EditorApplication {
  start(host: HTMLElement): Promise<unknown>;
  getEditor(): MonacoEditor | undefined;
  getDiffEditor?(): monaco.editor.IStandaloneDiffEditor | undefined;
  dispose(): Promise<unknown>;
}
export interface LanguageClient {
  start(): Promise<unknown>;
  dispose(): Promise<unknown>;
}
export interface MonacoPlatform {
  startServices(): Promise<void>;
  createApp(config: EditorAppConfig): EditorApplication;
  createLanguageClient(config: { languageId: string; webSocketUrl: string }): LanguageClient;
  bind(code: YText, model: monaco.editor.ITextModel, editor: MonacoEditor, awareness?: Awareness): { destroy(): void };
}

let platformPromise: Promise<MonacoPlatform> | undefined;
let servicesPromise: Promise<void> | undefined;
let vscodeStarted = false;

/** Heavy browser-only modules are loaded only when a UDF editor is opened. */
export function loadMonacoPlatform(): Promise<MonacoPlatform> {
  platformPromise ??= (async () => {
    const [{ EditorApp }, vscode, languageClient, { MonacoBinding }] = await Promise.all([
      import("monaco-languageclient/editorApp"),
      import("monaco-languageclient/vscodeApiWrapper"),
      import("monaco-languageclient/lcwrapper"),
      import("y-monaco"),
    ]);
    return {
      startServices() {
        servicesPromise ??= (async () => {
          if (!vscodeStarted) {
            await new vscode.MonacoVscodeApiWrapper({
              $type: "extended",
              viewsConfig: { $type: "EditorService" },
              userConfiguration: { json: JSON.stringify({ "workbench.colorTheme": "Default Dark Modern" }) },
              monacoWorkerFactory: () => {
                const environment = vscode.getEnhancedMonacoEnvironment();
                environment.getWorker = (_workerId, label) => {
                  if (label === "editorWorkerService")
                    return new Worker(new URL("./workers/editor.worker.ts", import.meta.url), { type: "module" });
                  if (label === "extensionHostWorkerMain")
                    return new Worker(new URL("./workers/extension-host.worker.ts", import.meta.url), {
                      type: "module",
                    });
                  if (label === "TextMateWorker")
                    return new Worker(new URL("./workers/textmate.worker.ts", import.meta.url), { type: "module" });
                  throw new Error(`No worker configured for label: ${label}`);
                };
              },
            }).start();
            vscodeStarted = true;
          }
          const extensions = await Promise.all([
            import("@codingame/monaco-vscode-python-default-extension"),
            import("@codingame/monaco-vscode-java-default-extension"),
          ]);
          await Promise.all(extensions.map(extension => extension.whenReady?.()));
        })().catch(reason => {
          servicesPromise = undefined;
          throw reason;
        });
        return servicesPromise;
      },
      createApp: config => new EditorApp(config),
      createLanguageClient: config =>
        new languageClient.LanguageClientWrapper({
          languageId: config.languageId,
          connection: {
            options: {
              $family: "WebSocket",
              webSocketUrl: config.webSocketUrl,
              realization: () => new languageClient.LcWebSocket(),
            },
          },
          clientOptions: { documentSelector: [config.languageId] },
        }),
      bind: (code, model, editor, awareness) => new MonacoBinding(code, model, new Set([editor]), awareness),
    } satisfies MonacoPlatform;
  })().catch(reason => {
    platformPromise = undefined;
    throw reason;
  });
  return platformPromise;
}

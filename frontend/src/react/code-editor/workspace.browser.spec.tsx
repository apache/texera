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

import { afterEach, expect, it, vi } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { page, userEvent } from "vitest/browser";
import * as Y from "yjs";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import type { Workflow } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import { WorkflowSession } from "../workspace/workflow-session";
import { CanvasModel } from "../workspace/canvas-model";
import { WorkflowCanvas } from "../workspace/workflow-canvas";
import { OperatorProperties } from "../workspace/operator-properties";
import { ApiClient } from "../core/api-client";

const disposals: (() => void)[] = [];
afterEach(() => {
  cleanup();
  disposals
    .splice(0)
    .reverse()
    .forEach(dispose => dispose());
});
const metadata: OperatorMetadata = {
  groups: [],
  operators: [
    {
      operatorType: "PythonUDFSourceV2",
      operatorVersion: "1",
      additionalMetadata: {
        userFriendlyName: "Python Source",
        operatorGroupName: "Test",
        inputPorts: [],
        outputPorts: [{}],
      },
      jsonSchema: {
        type: "object",
        properties: {
          code: { type: "string", title: "Code", description: "Input your code here" },
          uiParameters: { type: "array", items: { type: "object" } },
        },
      },
    },
  ],
};
function setup() {
  const shared = new SharedModel(undefined, undefined, undefined, { connect: false });
  const workflow = {
    wid: 42,
    name: "Browser workflow",
    description: "",
    readonly: false,
    creationTime: 1,
    lastModifiedTime: 1,
    isPublished: 0,
    content: {
      operators: [
        {
          operatorID: "browser-op",
          operatorType: "PythonUDFSourceV2",
          operatorVersion: "1",
          customDisplayName: "Browser Python",
          operatorProperties: { code: 'print("initial")', uiParameters: [] },
          inputPorts: [],
          outputPorts: [{ portID: "output-0" }],
          showAdvanced: false,
        },
      ],
      links: [],
      commentBoxes: [],
      operatorPositions: { "browser-op": { x: 120, y: 80 } },
      settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
    },
  } as Workflow;
  const session = new WorkflowSession(workflow, { shared, save: async value => value, autosaveMs: 0 });
  disposals.push(
    () => shared.destroy(),
    () => session.dispose()
  );
  const canvas = new CanvasModel(session, metadata, error => {
    throw error;
  });
  disposals.push(() => canvas.dispose());
  const api = new ApiClient({
    fetch: async () => new Response("false", { headers: { "Content-Type": "text/plain" } }),
  });
  return { shared, session, canvas, api };
}

it("keeps real JointJS selection, Monaco edits, shared Y.Text convergence and reopen behavior connected", async () => {
  const test = setup(),
    selected = vi.fn();
  const peer = new SharedModel(undefined, undefined, undefined, { connect: false });
  disposals.push(() => peer.destroy());
  Y.applyUpdate(peer.yDoc, Y.encodeStateAsUpdate(test.shared.yDoc));
  const peerSession = new WorkflowSession(test.session.getSnapshot().workflow, {
    shared: peer,
    save: async value => value,
    autosaveMs: 0,
  });
  disposals.push(() => peerSession.dispose());
  let connected = true;
  const forward = (update: Uint8Array) => {
    if (connected) Y.applyUpdate(peer.yDoc, update);
  };
  const backward = (update: Uint8Array) => {
    if (connected) Y.applyUpdate(test.shared.yDoc, update);
  };
  test.shared.yDoc.on("update", forward);
  peer.yDoc.on("update", backward);
  disposals.push(() => {
    test.shared.yDoc.off("update", forward);
    peer.yDoc.off("update", backward);
  });
  render(
    <div style={{ width: 1000, height: 700 }}>
      <WorkflowCanvas
        model={test.canvas}
        session={test.session}
        metadata={metadata}
        selected={[]}
        onSelect={selected}
        onError={reason => {
          throw reason;
        }}
      />
      <OperatorProperties
        api={test.api}
        user={{ uid: 1, name: "Browser user", role: "REGULAR", expiresAt: 9999999999 }}
        config={{}}
        getToken={() => null}
        session={test.session}
        metadata={metadata}
        operatorId="browser-op"
      />
    </div>
  );
  const operator = document.querySelector('[model-id="browser-op"]')!;
  expect(operator).toBeTruthy();
  await act(async () => {
    await page.elementLocator(operator).click();
  });
  expect(selected).toHaveBeenCalledWith("browser-op", false);
  await act(async () => {
    await page.getByRole("button", { name: "Edit code" }).click();
  });
  await waitFor(
    () => {
      const failure = document.querySelector(".react-code-editor-window > p[role=alert]");
      if (failure) throw new Error(failure.textContent ?? "Code editor initialization failed");
      expect(screen.queryByText("Loading code editor…")).toBeNull();
      expect(document.querySelector(".monaco-editor textarea")).toBeTruthy();
    },
    { timeout: 25000 }
  );
  expect(document.querySelector(".react-code-editor-window > p[role=alert]")).toBeNull();
  const monaco = await import("monaco-editor");
  const editorModel = monaco.editor
    .getModels()
    .find(model => model.uri.toString().includes("in-memory-browser-op.py"))!;
  expect(editorModel).toBeTruthy();
  expect(editorModel.getValue()).toBe('print("initial")');
  const sharedCode = test.session.graph.getSharedOperatorPropertyType("browser-op").get("code") as Y.Text;
  const peerCode = peerSession.graph.getSharedOperatorPropertyType("browser-op").get("code") as Y.Text;
  const textarea = document.querySelector(".monaco-editor textarea") as HTMLTextAreaElement;
  await act(async () => {
    await page.elementLocator(textarea).click({ force: true });
    await userEvent.keyboard("{Escape}{Control>}{End}{/Control}{Enter}# keyboard edit");
  });
  await waitFor(() => expect(sharedCode.toString()).toContain("# keyboard edit"));
  expect(peerCode.toString()).toBe(sharedCode.toString());
  connected = false;
  act(() => {
    editorModel.applyEdits([
      {
        range: new monaco.Range(
          editorModel.getLineCount(),
          editorModel.getLineMaxColumn(editorModel.getLineCount()),
          editorModel.getLineCount(),
          editorModel.getLineMaxColumn(editorModel.getLineCount())
        ),
        text: " local",
      },
    ]);
    peerCode.insert(0, "# remote 日本語\n");
  });
  connected = true;
  act(() => {
    Y.applyUpdate(test.shared.yDoc, Y.encodeStateAsUpdate(peer.yDoc));
    Y.applyUpdate(peer.yDoc, Y.encodeStateAsUpdate(test.shared.yDoc));
  });
  expect(peerCode.toString()).toBe(sharedCode.toString());
  // Monaco renders the platform's line endings; peers and saved code retain their exact shared text.
  expect(editorModel.getValue().replace(/\r\n/g, "\n")).toBe(sharedCode.toString().replace(/\r\n/g, "\n"));
  expect(sharedCode.toString()).toContain("# remote 日本語");
  expect(sharedCode.toString()).toContain("local");
  await act(async () => {
    await page.getByRole("button", { name: "Close code editor" }).click();
  });
  expect(screen.queryByRole("dialog")).toBeNull();
  const saved = sharedCode.toString();
  await act(async () => {
    await page.getByRole("button", { name: "Edit code" }).click();
  });
  await waitFor(() => expect(screen.queryByText("Loading code editor…")).toBeNull(), { timeout: 15000 });
  expect(
    monaco.editor
      .getModels()
      .find(model => model.uri.toString().includes("in-memory-browser-op.py"))
      ?.getValue()
      .replace(/\r\n/g, "\n")
  ).toBe(saved.replace(/\r\n/g, "\n"));
  expect(test.session.graph.getOperator("browser-op").operatorProperties.code).toBe(saved);
});

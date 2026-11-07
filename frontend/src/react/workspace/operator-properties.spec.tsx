/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { OperatorProperties } from "./operator-properties";
import { FormView } from "./form-view";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import type { Workflow } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import type { WorkflowRuntime, RuntimeSnapshot } from "./workflow-runtime";
import { ApiClient } from "../core/api-client";
import type { CodeEditorSession } from "../code-editor/code-editor";
import {
  ExecutionState,
  OperatorState,
  type OperatorRuntimeStatus,
} from "../../core/workspace/types/execute-workflow.interface";

const capture = vi.hoisted(() => ({ editor: undefined as CodeEditorSession | undefined }));
vi.mock("../code-editor/code-editor", () => ({
  CodeareaField: ({ session }: { session: CodeEditorSession }) => {
    capture.editor = session;
    return <button type="button">Real code editor</button>;
  },
}));
const entries: { session: WorkflowSession; model: SharedModel }[] = [];
afterEach(() => {
  cleanup();
  entries.splice(0).forEach(({ session, model }) => {
    session.dispose();
    model.destroy();
  });
  capture.editor = undefined;
});
const code =
  'class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        self.UiParameter("k", AttributeType.INT)\n';
const metadata: OperatorMetadata = {
  groups: [],
  operators: [
    {
      operatorType: "PythonUDFV2",
      operatorVersion: "1",
      additionalMetadata: {
        userFriendlyName: "Python UDF",
        operatorGroupName: "UDF",
        inputPorts: [{}],
        outputPorts: [{}],
      },
      jsonSchema: {
        type: "object",
        properties: {
          code: { type: "string", description: "Input your code here", title: "Code" },
          text: { type: "string", title: "Text" },
          attribute: { type: "string", title: "Input column", autofill: "attributeName", autofillAttributeOnPort: 0 },
          uiParameters: { type: "array", items: { type: "object" }, title: "Parameters" },
        },
      },
    },
  ],
};
function build(readonly = false) {
  const target = {
    operatorID: "target",
    operatorType: "PythonUDFV2",
    operatorVersion: "1",
    operatorProperties: { code, text: "first", attribute: "old", uiParameters: [] },
    inputPorts: [{ portID: "input-0" }],
    outputPorts: [{ portID: "output-0" }],
    showAdvanced: false,
  };
  const source = { ...target, operatorID: "source", operatorType: "Scan", operatorProperties: {} };
  const workflow = {
    wid: 42,
    name: "Workflow",
    description: "",
    readonly,
    creationTime: 1,
    lastModifiedTime: 1,
    isPublished: 0,
    content: {
      operators: [source, target],
      links: [
        {
          linkID: "link",
          source: { operatorID: "source", portID: "output-0" },
          target: { operatorID: "target", portID: "input-0" },
        },
      ],
      operatorPositions: { source: { x: 0, y: 0 }, target: { x: 300, y: 0 } },
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
    },
  } as Workflow;
  const model = new SharedModel(42, undefined, undefined, { connect: false });
  const session = new WorkflowSession(workflow, { shared: model, save: async workflow => workflow, autosaveMs: 0 });
  entries.push({ session, model });
  const listeners = new Set<() => void>();
  let runtimeState = {
    compiling: false,
    operators: {},
    workers: {},
    console: {},
    compilation: {
      operatorOutputSchemas: { source: { "0_false": [{ attributeName: "new", attributeType: "string" }] } },
      operatorErrors: {},
    },
  } as unknown as RuntimeSnapshot;
  const runtime = {
    canControl: () => true,
    getSnapshot: () => runtimeState,
    subscribe: (fn: () => void) => {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    socket: { subscribeEvents: () => () => {}, send: vi.fn() },
  } as unknown as WorkflowRuntime;
  const api = new ApiClient({
    fetch: vi.fn(async () => new Response("[]", { headers: { "Content-Type": "application/json" } })),
  });
  const props = {
    api,
    user: { uid: 1, name: "A", role: "REGULAR" as const, expiresAt: 9999999999 },
    config: {},
    getToken: () => null,
    session,
    runtime,
    metadata,
    operatorId: "target",
  };
  return {
    props,
    session,
    model,
    update: (patch: Partial<RuntimeSnapshot>) => {
      runtimeState = { ...runtimeState, ...patch };
      listeners.forEach(fn => fn());
    },
  };
}
describe("integrated operator properties", () => {
  it("requires computing-unit write permission before unlocking paused logic", () => {
    const test = build();
    test.session.setExecutionLocked(true);
    test.update({ state: ExecutionState.Paused });
    const supported = {
      ...metadata,
      operators: metadata.operators.map(schema => ({
        ...schema,
        additionalMetadata: { ...schema.additionalMetadata, supportReconfiguration: true },
      })),
    };
    test.props.runtime.canControl = () => false;
    render(
      <OperatorProperties
        {...test.props}
        metadata={supported}
      />
    );
    expect(screen.getByRole("button", { name: "Unlock for Logic Change" })).toHaveProperty("disabled", true);
    expect(test.props.runtime.socket.send).not.toHaveBeenCalled();
  });
  it("rebinds code, awareness and debugger services when the active document changes", () => {
    const test = build();
    render(<OperatorProperties {...test.props} />);
    const liveCode = capture.editor!.code,
      liveAwareness = capture.editor!.awareness,
      liveDebug = capture.editor!.debug;
    const history = test.session.getSnapshot().workflow.content;
    act(() => test.session.enterPreview(history));
    expect(capture.editor?.code).not.toBe(liveCode);
    expect(capture.editor?.awareness).not.toBe(liveAwareness);
    expect(capture.editor?.debug).not.toBe(liveDebug);
    expect(capture.editor?.readOnly).toBe(true);
    act(() => test.session.exitPreview());
    expect(capture.editor?.code).toBe(liveCode);
    expect(capture.editor?.awareness).toBe(liveAwareness);
    expect(capture.editor?.readOnly).toBe(false);
  });
  it("unlocks only supported paused operator logic, keeps structure locked, and confirms the exact runtime protocol", () => {
    const test = build();
    test.session.setExecutionLocked(true);
    test.update({ state: ExecutionState.Paused });
    const supported = {
      ...metadata,
      operators: metadata.operators.map(schema => ({
        ...schema,
        additionalMetadata: { ...schema.additionalMetadata, supportReconfiguration: true },
      })),
    };
    render(
      <OperatorProperties
        {...test.props}
        metadata={supported}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Unlock for Logic Change" }));
    expect(screen.getByRole("textbox", { name: "Text" })).toHaveProperty("disabled", false);
    expect(test.session.getSnapshot().canEdit).toBe(false);
    fireEvent.change(screen.getByRole("textbox", { name: "Text" }), { target: { value: "reconfigured" } });
    expect(test.session.graph.getOperator("target").operatorProperties.text).toBe("reconfigured");
    expect(capture.editor?.readOnly).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Confirm Logic Change" }));
    expect(test.props.runtime.socket.send).toHaveBeenCalledWith("ModifyLogicRequest", {
      operator: expect.objectContaining({ operatorID: "target", operatorType: "PythonUDFV2", text: "reconfigured" }),
    });
    expect(screen.getByRole("textbox", { name: "Text" })).toHaveProperty("disabled", true);
  });
  it("keeps paused reconfiguration locked for unsupported, completed, or read-only operators", () => {
    const test = build();
    test.session.setExecutionLocked(true);
    test.update({ state: ExecutionState.Paused });
    const { rerender } = render(<OperatorProperties {...test.props} />);
    expect(screen.getByRole("button", { name: "Unlock for Logic Change" })).toHaveProperty("disabled", true);
    const supported = {
      ...metadata,
      operators: metadata.operators.map(schema => ({
        ...schema,
        additionalMetadata: { ...schema.additionalMetadata, supportReconfiguration: true },
      })),
    };
    act(() =>
      test.update({ operators: { target: { operatorState: OperatorState.Completed } as OperatorRuntimeStatus } })
    );
    rerender(
      <OperatorProperties
        {...test.props}
        metadata={supported}
      />
    );
    expect(screen.getByRole("button", { name: "Unlock for Logic Change" })).toHaveProperty("disabled", true);
    act(() => test.update({ state: ExecutionState.Running }));
    rerender(<OperatorProperties {...test.props} />);
    expect(screen.queryByRole("button", { name: "Unlock for Logic Change" })).toBeNull();
    const readonly = build(true);
    readonly.update({ state: ExecutionState.Paused });
    rerender(<OperatorProperties {...readonly.props} />);
    expect(screen.queryByRole("button", { name: "Unlock for Logic Change" })).toBeNull();
  });
  it("locks Form View values and authoring while execution owns the workflow", () => {
    const test = build();
    test.session.setFormBinding({
      fields: [{ id: "text", operatorID: "target", propertyKey: "text", displayName: "Text input" }],
    });
    render(
      <FormView
        session={test.session}
        metadata={metadata}
        onCanvas={() => {}}
      />
    );
    act(() => test.session.setExecutionLocked(true));
    expect(screen.getByRole("textbox", { name: "Text input" })).toHaveProperty("disabled", true);
    expect(screen.queryByRole("button", { name: "Edit form" })).toBeNull();
    expect(test.session.graph.getOperator("target").operatorProperties.text).toBe("first");
  });
  it("uses the same real code widget for a saved Form View code binding", () => {
    const test = build();
    test.session.setFormBinding({
      fields: [{ id: "code", operatorID: "target", propertyKey: "code", displayName: "Shared UDF" }],
    });
    render(
      <FormView
        session={test.session}
        metadata={metadata}
        propertyServices={test.props}
        onCanvas={() => {}}
      />
    );
    expect(screen.getByRole("button", { name: "Real code editor" })).toBeTruthy();
    expect(capture.editor?.code).toBe(test.session.graph.getSharedOperatorPropertyType("target").get("code"));
  });
  it("edits the shared graph, applies upstream choices and prunes stale attributes", async () => {
    const test = build();
    render(<OperatorProperties {...test.props} />);
    await waitFor(() => expect(test.session.graph.getOperator("target").operatorProperties.attribute).toBe(""));
    expect(screen.getByRole("option", { name: "new" })).toBeTruthy();
    fireEvent.change(screen.getByRole("textbox", { name: "Text" }), { target: { value: "日本語" } });
    expect(test.session.graph.getOperator("target").operatorProperties.text).toBe("日本語");
    act(() =>
      test.session.setProperties("target", {
        ...test.session.graph.getOperator("target").operatorProperties,
        text: "remote",
      })
    );
    expect((screen.getByRole("textbox", { name: "Text" }) as HTMLInputElement).value).toBe("remote");
  });
  it("binds Monaco to the existing code Y.Text and synchronizes UiParameter declarations outside the dialog", async () => {
    const test = build();
    render(<OperatorProperties {...test.props} />);
    await waitFor(() =>
      expect(test.session.graph.getOperator("target").operatorProperties.uiParameters).toHaveLength(1)
    );
    expect(capture.editor?.code).toBe(test.session.graph.getSharedOperatorPropertyType("target").get("code"));
    expect(capture.editor?.awareness).toBe(test.model.awareness);
    expect(capture.editor?.language).toBe("python");
    expect(capture.editor?.ai).toBeDefined();
    expect(capture.editor?.debug).toBeDefined();
    const sharedCode = capture.editor!.code;
    act(() =>
      test.model.yDoc.transact(() => {
        sharedCode.delete(0, sharedCode.length);
        sharedCode.insert(0, code.replace('"k"', '"other"'));
      })
    );
    await waitFor(() =>
      expect(test.session.graph.getOperator("target").operatorProperties.uiParameters).toEqual([
        { attribute: { attributeName: "other", attributeType: "integer" }, value: "" },
      ])
    );
  });
  it("keeps read-only workflows unchanged and reports missing operators/descriptors", () => {
    const test = build(true);
    const { rerender } = render(<OperatorProperties {...test.props} />);
    expect(screen.getByRole("textbox", { name: "Text" })).toHaveProperty("disabled", true);
    expect(capture.editor?.readOnly).toBe(true);
    expect(test.session.graph.getOperator("target").operatorProperties.attribute).toBe("old");
    expect(test.session.graph.getOperator("target").operatorProperties.uiParameters).toEqual([]);
    rerender(
      <OperatorProperties
        {...test.props}
        operatorId="missing"
      />
    );
    expect(screen.getByRole("status").textContent).toContain("Select a step");
    rerender(
      <OperatorProperties
        {...test.props}
        operatorId="source"
      />
    );
    expect(screen.getByRole("alert").textContent).toContain("metadata");
  });
  it("reports conflicting multi-link schemas without deleting unknown references", () => {
    const test = build();
    test.session.addOperator({ ...test.session.graph.getOperator("source"), operatorID: "second" }, { x: 0, y: 100 });
    test.session.addLink({
      linkID: "second-link",
      source: { operatorID: "second", portID: "output-0" },
      target: { operatorID: "target", portID: "input-0" },
    });
    render(<OperatorProperties {...test.props} />);
    expect(screen.getByRole("alert").textContent).toContain("Multiple links with different schemas");
    expect(test.session.graph.getOperator("target").operatorProperties.attribute).toBe("old");
  });
});

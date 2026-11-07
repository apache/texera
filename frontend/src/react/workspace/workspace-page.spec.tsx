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
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WorkspaceEditor } from "./workspace-page";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import { ExecutionMode } from "../../core/common/type/workflow";
import { ApiClient } from "../core/api-client";
import type { WorkspaceController } from "./workspace-controller";
import type { CanvasActions, WorkflowCanvasProps } from "./workflow-canvas";

const canvas = vi.hoisted(() => ({ ready: undefined as WorkflowCanvasProps["onReady"] }));
vi.mock("./workflow-canvas", () => ({
  WorkflowCanvas: (props: WorkflowCanvasProps) => {
    canvas.ready = props.onReady;
    return <div aria-label="Workflow canvas" />;
  },
}));
vi.mock("./operator-properties", () => ({
  OperatorProperties: ({ operatorId }: { operatorId: string }) => <div>Properties for {operatorId}</div>,
}));
vi.mock("../results/results-panel", () => ({ ResultsPanel: () => <div>Results</div> }));
const sessions: WorkflowSession[] = [];
const originalContext = HTMLCanvasElement.prototype.getContext;
beforeEach(() => {
  HTMLCanvasElement.prototype.getContext = (() => null) as typeof originalContext;
});
afterEach(() => {
  cleanup();
  sessions.splice(0).forEach(session => session.dispose());
  HTMLCanvasElement.prototype.getContext = originalContext;
});
function setup(readonly = false, autoLayout = false) {
  const save = vi.fn(async value => value);
  const session = new WorkflowSession(
    {
      wid: 42,
      name: "Example",
      description: "",
      readonly,
      isPublished: 0,
      creationTime: 1,
      lastModifiedTime: 1,
      content: {
        operators: [],
        links: [],
        commentBoxes: [],
        operatorPositions: {},
        settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
      },
    },
    { shared: new SharedModel(42, undefined, undefined, { connect: false }), save, autosaveMs: 0 }
  );
  sessions.push(session);
  const metadata = {
    operators: [
      {
        operatorType: "Test",
        operatorVersion: "1",
        jsonSchema: { type: "object" as const, properties: { value: { type: "number" as const, default: 0 } } },
        additionalMetadata: {
          userFriendlyName: "Test source",
          operatorGroupName: "Sources",
          inputPorts: [],
          outputPorts: [{}],
        },
      },
    ],
    groups: [],
  };
  const selectComputingUnit = vi.fn();
  const selectWarehouse = vi.fn();
  const controller = { selectComputingUnit, selectWarehouse } as unknown as WorkspaceController;
  const onView = vi.fn();
  const onAutoLayoutApplied = vi.fn();
  const onBack = vi.fn();
  const props = {
    api: new ApiClient({ fetch: vi.fn() }),
    user: { uid: 7, name: "Test", email: "test@example.org", role: "REGULAR" },
    config: { formViewEnabled: true },
    getToken: () => null,
    controller,
    snapshot: { loading: false, session, metadata, units: [], warehouses: [] },
    view: "canvas" as const,
    autoLayout,
    onAutoLayoutApplied,
    onView,
    onBack,
  };
  render(<WorkspaceEditor {...props} />);
  return { session, save, onView, onBack, props };
}
describe("React workspace integration", () => {
  it("applies requested notebook layout once after the canvas is ready and preserves read-only previews", () => {
    const { props } = setup(false, true);
    const autoLayout = vi.fn();
    const actions = { autoLayout } as unknown as CanvasActions;
    act(() => {
      canvas.ready?.(actions);
      canvas.ready?.(actions);
    });
    expect(autoLayout).toHaveBeenCalledOnce();
    expect(props.onAutoLayoutApplied).toHaveBeenCalledOnce();
    cleanup();
    setup(true, true);
    const forbidden = vi.fn();
    act(() => canvas.ready?.({ autoLayout: forbidden } as unknown as CanvasActions));
    expect(forbidden).not.toHaveBeenCalled();
  });
  it("adds a metadata-defined operator to the shared graph, edits, saves and switches view", async () => {
    const { session, save, onView } = setup();
    fireEvent.click(screen.getByRole("button", { name: "Add Test source" }));
    const op = session.graph.getAllOperators()[0];
    expect(op.operatorProperties).toEqual({ value: 0 });
    expect(screen.getByText(`Properties for ${op.operatorID}`)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Workflow name"), { target: { value: "Renamed 日本語" } });
    fireEvent.blur(screen.getByLabelText("Workflow name"));
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Save", exact: true }));
    });
    expect(save).toHaveBeenCalled();
    expect(save.mock.calls.at(-1)?.[0].name).toBe("Renamed 日本語");
    fireEvent.click(screen.getByRole("button", { name: "Form view", exact: true }));
    expect(onView).toHaveBeenCalledWith("form");
    expect(session.graph.getAllOperators()).toHaveLength(1);
  });
  it("prevents graph mutations and run without an execution connection in a read-only version", () => {
    const { session } = setup(true);
    expect((screen.getByRole("button", { name: "Add Test source" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: "Run", exact: true }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByLabelText("Workflow name") as HTMLInputElement).disabled).toBe(true);
    expect(session.graph.getAllOperators()).toHaveLength(0);
  });
});

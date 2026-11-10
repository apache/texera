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

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { OperatorControls, createOperatorControls } from "./operator-controls";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import type { Workflow } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
const models: { session: WorkflowSession; shared: SharedModel }[] = [];
afterEach(() => {
  cleanup();
  models.splice(0).forEach(({ session, shared }) => {
    session.dispose();
    shared.destroy();
  });
});
const metadata: OperatorMetadata = {
  groups: [],
  operators: [
    {
      operatorType: "Dynamic",
      operatorVersion: "1",
      additionalMetadata: {
        userFriendlyName: "Dynamic",
        operatorGroupName: "Test",
        inputPorts: [{}],
        outputPorts: [{}],
        dynamicInputPorts: true,
        dynamicOutputPorts: true,
        allowPortCustomization: true,
      },
      jsonSchema: { type: "object" },
    },
  ],
};
function build(readonly = false) {
  const operator = {
    operatorID: "op",
    operatorType: "Dynamic",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: [{ portID: "input-0", displayName: "in", isDynamicPort: false }],
    outputPorts: [{ portID: "output-0", displayName: "out", isDynamicPort: false }],
    showAdvanced: false,
    dynamicInputPorts: true,
    dynamicOutputPorts: true,
  };
  const workflow = {
    wid: 42,
    name: "Test",
    description: "",
    creationTime: 1,
    lastModifiedTime: 1,
    isPublished: 0,
    readonly,
    content: {
      operators: [operator],
      links: [],
      commentBoxes: [],
      operatorPositions: { op: { x: 0, y: 0 } },
      settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
    },
  } as Workflow;
  const shared = new SharedModel(42, undefined, undefined, { connect: false });
  const session = new WorkflowSession(workflow, { shared, save: async value => value, autosaveMs: 0 });
  models.push({ session, shared });
  return { session, metadata, operatorId: "op" };
}
describe("operator advanced and port controls", () => {
  it("does not retain a preview graph in controls that outlive the preview", () => {
    const props = build();
    props.session.enterPreview(props.session.getSnapshot().workflow.content);
    const actions = createOperatorControls(props.session, metadata, "op");
    expect(() => actions.reuse(true)).toThrow(/edited/i);
    props.session.exitPreview();
    actions.reuse(true);
    expect(props.session.graph.getOperator("op").markedForReuse).toBe(true);
  });
  it("adds/removes dynamic ports using canonical IDs, guards fixed ports and keeps changes undoable", () => {
    const props = build(),
      actions = createOperatorControls(props.session, metadata, "op");
    expect(() => actions.removeLastPort(true)).toThrow("dynamic");
    actions.addPort(true, true);
    actions.addPort(false);
    expect(props.session.graph.getOperator("op").inputPorts[1]).toMatchObject({
      portID: "input-1",
      isDynamicPort: true,
      disallowMultiInputs: true,
      dependencies: [],
    });
    expect(props.session.graph.getOperator("op").outputPorts[1]).toMatchObject({
      portID: "output-1",
      isDynamicPort: true,
    });
    actions.removeLastPort(true);
    expect(props.session.graph.getOperator("op").inputPorts).toHaveLength(1);
    expect(props.session.getSnapshot().canUndo).toBe(true);
  });
  it("edits shared names, partition requirements, typed dependencies and reuse flags", () => {
    const props = build();
    render(<OperatorControls {...props} />);
    fireEvent.change(screen.getByRole("textbox", { name: "Operator name" }), { target: { value: "日本語" } });
    fireEvent.change(screen.getByRole("textbox", { name: "Input 1 name" }), { target: { value: "Rows" } });
    fireEvent.change(screen.getByRole("combobox", { name: "Input 1 partition type" }), { target: { value: "hash" } });
    fireEvent.change(screen.getByRole("textbox", { name: "Input 1 hash attributes" }), { target: { value: "key,β" } });
    fireEvent.click(screen.getByRole("button", { name: "Add Input 1 dependency" }));
    fireEvent.change(screen.getByRole("spinbutton", { name: "Input 1 dependency 1 id" }), { target: { value: "2" } });
    fireEvent.click(screen.getByRole("checkbox", { name: "Input 1 dependency 1 internal" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Reuse cached result" }));
    const op = props.session.graph.getOperator("op");
    expect(op.customDisplayName).toBe("日本語");
    expect(op.markedForReuse).toBe(true);
    expect(op.inputPorts[0]).toMatchObject({
      displayName: "Rows",
      partitionRequirement: { type: "hash", hashAttributeNames: ["key", "β"] },
      dependencies: [{ id: 2, internal: true }],
    });
  });
  it("removes links connected to a removed port in the same shared transaction", () => {
    const props = build(),
      actions = createOperatorControls(props.session, metadata, "op");
    actions.addPort(true);
    props.session.addOperator({ ...props.session.graph.getOperator("op"), operatorID: "source" }, { x: 0, y: 100 });
    props.session.addLink({
      linkID: "link",
      source: { operatorID: "source", portID: "output-0" },
      target: { operatorID: "op", portID: "input-1" },
    });
    actions.removeLastPort(true);
    expect(props.session.graph.hasLinkWithID("link")).toBe(false);
  });
  it("rejects malformed partitions/dependencies and locks all changes during execution/read-only", () => {
    const props = build(),
      actions = createOperatorControls(props.session, metadata, "op");
    expect(() =>
      actions.setPort("input-0", {
        partitionInfo: { type: "range", rangeAttributeNames: [], rangeMin: NaN, rangeMax: 2 },
        dependencies: [],
      })
    ).toThrow("partition");
    expect(() =>
      actions.setPort("input-0", { partitionInfo: { type: "none" }, dependencies: [{ id: -1, internal: false }] })
    ).toThrow("dependency");
    props.session.setExecutionLocked(true);
    expect(() => actions.addPort(true)).toThrow("edited");
    render(<OperatorControls {...props} />);
    expect(screen.getByRole("button", { name: "Add input port" })).toHaveProperty("disabled", true);
    const readonlyActions = createOperatorControls(build(true).session, metadata, "op");
    expect(() => readonlyActions.reuse(true)).toThrow("edited");
    expect(() => readonlyActions.renamePort("input-0", "changed")).toThrow("edited");
  });
});

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

import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { FormView, formResultIds } from "./form-view";
import { WorkflowSession } from "./workflow-session";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import type { Workflow } from "../../core/common/type/workflow";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
const resources: { session: WorkflowSession; model: SharedModel }[] = [];
afterEach(() => {
  cleanup();
  resources.splice(0).forEach(({ session, model }) => {
    session.dispose();
    model.destroy();
  });
});
const metadata: OperatorMetadata = {
  groups: [],
  operators: [
    {
      operatorType: "Test",
      operatorVersion: "1",
      additionalMetadata: {
        userFriendlyName: "Test step",
        operatorGroupName: "Test",
        inputPorts: [{}],
        outputPorts: [{}],
      },
      jsonSchema: {
        type: "object",
        required: ["text"],
        properties: {
          text: { type: "string", title: "Text" },
          count: { type: "integer", title: "Count" },
          options: {
            type: "object",
            title: "Settings",
            properties: { visible: { type: "string", title: "Visible" }, secret: { type: "string", title: "Secret" } },
          },
        },
      },
    },
  ],
};
function build(readonly = false, save: (workflow: Workflow) => Promise<Workflow> = async workflow => workflow) {
  const operator = (operatorID: string, name: string) => ({
    operatorID,
    operatorType: "Test",
    operatorVersion: "1",
    customDisplayName: name,
    operatorProperties: { text: "initial", count: 2, options: { visible: "shown", secret: "kept" } },
    inputPorts: [{ portID: "input-0" }],
    outputPorts: [{ portID: "output-0" }],
    showAdvanced: false,
    ...(operatorID === "a" ? { viewResult: true } : {}),
  });
  const workflow = {
    wid: 42,
    name: "Workflow",
    description: "",
    readonly,
    creationTime: 1,
    lastModifiedTime: 1,
    isPublished: 0,
    content: {
      operators: [operator("a", "First step"), operator("b", "Second step")],
      links: [
        {
          linkID: "ab",
          source: { operatorID: "a", portID: "output-0" },
          target: { operatorID: "b", portID: "input-0" },
        },
      ],
      commentBoxes: [],
      operatorPositions: { a: { x: 0, y: 0 }, b: { x: 300, y: 0 } },
      settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
      formBinding: {
        instruction: { title: "Instructions", body: "**Read this first**" },
        fields: [
          { id: "count", operatorID: "a", propertyKey: "count", displayName: "Count input" },
          { id: "text", operatorID: "a", propertyKey: "text", displayName: "Search text", helpText: "Enter a phrase" },
          {
            id: "settings",
            operatorID: "a",
            propertyKey: "options",
            displayName: "Settings input",
            overrides: { secret: { hidden: true }, visible: { displayName: "Visible value" } },
          },
        ],
      },
    },
  } as unknown as Workflow;
  const model = new SharedModel(42, undefined, undefined, { connect: false });
  const session = new WorkflowSession(workflow, { shared: model, save, autosaveMs: 0 });
  resources.push({ session, model });
  return session;
}
describe("React Form View", () => {
  it("renders saved instructions, ordered bindings and nested overrides from the live graph", () => {
    const session = build();
    const { container } = render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    expect(container.querySelector("strong")?.textContent).toBe("Read this first");
    expect(
      screen.getAllByRole("group", { name: /^Form input / }).map(element => element.getAttribute("aria-label"))
    ).toEqual(["Form input Count input", "Form input Search text", "Form input Settings input"]);
    expect(screen.getByLabelText("Visible value")).toBeTruthy();
    expect(screen.queryByLabelText("Secret")).toBeNull();
    expect(screen.getByText("Enter a phrase")).toBeTruthy();
  });
  it("writes filled inputs to shared operator properties while retaining unexposed and hidden values", () => {
    const session = build();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    fireEvent.change(screen.getByLabelText(/^Search text/), { target: { value: "日本語" } });
    expect(session.graph.getOperator("a").operatorProperties).toMatchObject({
      text: "日本語",
      count: 2,
      options: { secret: "kept" },
    });
    act(() => session.setProperties("a", { ...session.graph.getOperator("a").operatorProperties, text: "remote" }));
    expect((screen.getByLabelText(/^Search text/) as HTMLInputElement).value).toBe("remote");
  });
  it("authors names, help, order, exposure and instructions without changing execution properties", async () => {
    const session = build();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Edit form" }));
    fireEvent.click(screen.getByRole("button", { name: "Move Count input down" }));
    expect(
      session
        .getSnapshot()
        .workflow.content.formBinding?.fields.slice(0, 2)
        .map(field => field.id)
    ).toEqual(["text", "count"]);
    fireEvent.change(screen.getByLabelText("Rename Text"), { target: { value: "New input name" } });
    fireEvent.change(screen.getByLabelText("Help for New input name"), { target: { value: "New help" } });
    fireEvent.change(screen.getByLabelText("Instruction body"), { target: { value: "## Updated instruction" } });
    expect(session.getSnapshot().workflow.content.formBinding?.instruction?.body).toBe("## Updated instruction");
    expect(session.getSnapshot().workflow.content.formBinding?.fields[0]).toMatchObject({
      id: "text",
      displayName: "New input name",
      helpText: "New help",
    });
    fireEvent.change(screen.getByLabelText("Operator to expose"), { target: { value: "b" } });
    fireEvent.click(screen.getByRole("button", { name: "Add input" }));
    expect(session.getSnapshot().workflow.content.formBinding?.fields.some(field => field.operatorID === "b")).toBe(
      true
    );
    fireEvent.click(screen.getByRole("button", { name: "Remove Count input" }));
    expect(session.getSnapshot().workflow.content.formBinding?.fields.some(field => field.id === "count")).toBe(false);
    expect(session.graph.getOperator("a").operatorProperties.count).toBe(2);
  });
  it("keeps viewer result choices local, while author choices save only shownResultIds", () => {
    const session = build();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
        renderResults={ids => <output data-testid="results">{ids.join(",")}</output>}
      />
    );
    expect(screen.getByTestId("results").textContent).toBe("b");
    fireEvent.click(screen.getByRole("button", { name: "Show results from First step" }));
    expect(screen.getByTestId("results").textContent).toBe("b,a");
    expect(session.getSnapshot().workflow.content.formBinding?.shownResultIds).toBeUndefined();
    fireEvent.click(screen.getByRole("button", { name: "Edit form" }));
    fireEvent.click(screen.getByRole("button", { name: "Show results from First step" }));
    expect(session.getSnapshot().workflow.content.formBinding?.shownResultIds).toEqual(["b", "a"]);
    expect(session.graph.getOperator("a").viewResult).toBe(true);
    expect(session.graph.getOperator("b").viewResult).toBeUndefined();
  });
  it("hides broken inputs from readers and keeps them repairable for the author", () => {
    const session = build();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    act(() => session.deleteOperators(["a"]));
    expect(screen.queryByLabelText("Search text")).toBeNull();
    expect(session.getSnapshot().workflow.content.formBinding?.fields).toHaveLength(3);
    fireEvent.click(screen.getByRole("button", { name: "Edit form" }));
    expect(screen.getAllByText(/the step it belonged to was removed/)).toHaveLength(3);
    expect((screen.getByRole("button", { name: "Add input" }) as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Remove Search text" }));
    expect(session.getSnapshot().workflow.content.formBinding?.fields).toHaveLength(2);
  });
  it("keeps a read-only workflow inert and permits the canvas handover without saving", async () => {
    const save = vi.fn(async (workflow: Workflow) => workflow),
      session = build(true, save),
      onCanvas = vi.fn();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={onCanvas}
      />
    );
    expect(screen.queryByRole("button", { name: "Edit form" })).toBeNull();
    expect((screen.getByLabelText(/^Search text/) as HTMLInputElement).disabled).toBe(true);
    fireEvent.change(screen.getByLabelText(/^Search text/), { target: { value: "blocked" } });
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("initial");
    fireEvent.click(screen.getByRole("button", { name: "Open canvas" }));
    await waitFor(() => expect(onCanvas).toHaveBeenCalledTimes(1));
    expect(save).not.toHaveBeenCalled();
  });
  it("retains the form and surfaces a failed save before a canvas handover", async () => {
    const session = build(false, async () => {
        throw new Error("Save offline");
      }),
      onCanvas = vi.fn();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={onCanvas}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Open canvas" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Save offline"));
    expect(onCanvas).not.toHaveBeenCalled();
  });

  it("honors an explicitly empty result default and excludes disabled operators from terminal results", () => {
    const session = build();
    expect(formResultIds(session, []).shown).toEqual([]);
    session.setDisabled("b", true);
    expect(formResultIds(session).terminals).toEqual(["a"]);
    expect(formResultIds(session, ["b", "a", "a", "missing"]).shown).toEqual(["a"]);
    expect(session.graph.getOperator("a").viewResult).toBe(true);
  });

  it("keeps an unchanged text control focused across schema refreshes and updates nested presentation only", () => {
    const session = build();
    const { rerender } = render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    const input = screen.getByLabelText(/^Search text/) as HTMLInputElement;
    input.focus();
    fireEvent.change(input, { target: { value: "draft" } });
    rerender(
      <FormView
        session={session}
        metadata={structuredClone(metadata)}
        onCanvas={vi.fn()}
      />
    );
    expect(document.activeElement).toBe(input);
    expect(input.value).toBe("draft");
    fireEvent.click(screen.getByRole("button", { name: "Edit form" }));
    fireEvent.change(screen.getByLabelText("Rename Visible"), { target: { value: "Updated nested name" } });
    const binding = session.getSnapshot().workflow.content.formBinding?.fields.find(field => field.id === "settings");
    expect(binding?.overrides?.visible).toEqual({ displayName: "Updated nested name" });
    fireEvent.click(screen.getByRole("button", { name: "Show Secret" }));
    expect(
      session.getSnapshot().workflow.content.formBinding?.fields.find(field => field.id === "settings")?.overrides
        ?.secret
    ).toBeUndefined();
    expect(session.graph.getOperator("a").operatorProperties.options).toEqual({ visible: "shown", secret: "kept" });
  });

  it("opens the preview inspector read-only until the author explicitly enables editing", () => {
    const session = build();
    render(
      <FormView
        session={session}
        metadata={metadata}
        onCanvas={vi.fn()}
      />
    );
    fireEvent.click(screen.getByText("Workflow preview", { selector: "summary" }));
    fireEvent.click(screen.getByRole("button", { name: "Inspect Second step" }));
    const inspector = screen.getByRole("group", { name: "Operator properties" });
    expect((inspector.querySelector("input") as HTMLInputElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Edit form" }));
    expect((inspector.querySelector("input") as HTMLInputElement).disabled).toBe(false);
  });

  it("resolves a bound property's sibling references without exposing the sibling as another input", () => {
    const session = build(),
      referenced = structuredClone(metadata);
    referenced.operators[0].jsonSchema.properties!.template = {
      type: "string",
      title: "Template source",
      minLength: 3,
    };
    referenced.operators[0].jsonSchema.properties!.text = { $ref: "#/properties/template" };
    render(
      <FormView
        session={session}
        metadata={referenced}
        onCanvas={vi.fn()}
      />
    );
    const input = screen.getByLabelText(/^Search text/) as HTMLInputElement;
    fireEvent.change(input, { target: { value: "x" } });
    expect(input.getAttribute("aria-invalid")).toBe("true");
    fireEvent.change(input, { target: { value: "updated" } });
    expect(session.graph.getOperator("a").operatorProperties.text).toBe("updated");
    expect(screen.queryByLabelText(/^Template source/)).toBeNull();
  });
});

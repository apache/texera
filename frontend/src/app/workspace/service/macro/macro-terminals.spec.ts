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

import * as Y from "yjs";
import * as joint from "jointjs";
import { TestBed } from "@angular/core/testing";
import { WorkflowActionService, DEFAULT_WORKFLOW } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { macroWorkspaceTestProviders } from "./macro-workspace.spec-harness";
import { mockSentimentPredicate } from "../workflow-graph/model/mock-workflow-data";
import { WorkflowContent, exportedWorkflow } from "../../../common/type/workflow";
import { extractMacro, readMacroInterface } from "../../../common/util/workflow-macro";

describe("Saved macro terminals", () => {
  let actions: WorkflowActionService, undo: UndoRedoService, content: WorkflowContent;
  const node = (input: boolean) =>
    actions
      .getTexeraGraph()
      .getAllOperators()
      .find(op => op.operatorType === (input ? "MacroInput" : "MacroOutput"))!;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: macroWorkspaceTestProviders,
    });
    actions = TestBed.inject(WorkflowActionService);
    actions.suspendWorkflow().shared.wsProvider.disconnect();
    undo = TestBed.inject(UndoRedoService);
    content = extractMacro(
      {
        ...actions.getWorkflowContent(),
        operators: ["a", "b"].map(operatorID => ({ ...mockSentimentPredicate, operatorID, operatorVersion: "Nlp1" })),
        operatorPositions: { a: { x: 100, y: 100 }, b: { x: 300, y: 250 } },
      },
      ["a", "b"],
      "macro"
    );
    actions.reloadWorkflow({ ...DEFAULT_WORKFLOW, content }, false);
  });
  afterEach(() => actions.destroySharedModel());

  it("persists normal operators, links and positions through export and reopening", () => {
    const saved = actions.getWorkflowContent();
    expect(saved.operators.filter(op => op.operatorType.startsWith("Macro")).length).toBe(2);
    expect(actions.getTexeraGraph().hasOperator(node(true).operatorID)).toBe(true);
    expect(saved.operatorPositions[node(false).operatorID]).toBeDefined();
    const exported = JSON.parse(JSON.stringify(exportedWorkflow(saved, undefined)));
    actions.openWorkflowDraft({ ...DEFAULT_WORKFLOW, content: exported });
    expect(actions.getWorkflowContent()).toEqual(saved);
    expect(readMacroInterface(exported)).toEqual(actions.getMacroInterface());
  });

  for (const input of [true, false]) {
    it(`removes the selected ${input ? "input" : "output"} port and its links, restoring both with Undo/Redo`, () => {
      const terminal = node(input),
        direction = input ? "outputPorts" : "inputPorts";
      const first = terminal[direction][0].portID;
      actions.highlightPorts(false, { operatorID: terminal.operatorID, portID: first });
      const before = actions.getWorkflowContent();
      actions.removePort(terminal.operatorID, !input);
      expect(node(input)[direction].some(p => p.portID === first)).toBe(false);
      expect(
        (actions.getJointGraph().getCell(terminal.operatorID) as joint.dia.Element).getPorts().some(p => p.id === first)
      ).toBe(false);
      expect(actions.getWorkflowContent().links.length).toBe(before.links.length - 1);
      undo.undoAction();
      expect(actions.getWorkflowContent()).toEqual(before);
      expect(
        (actions.getJointGraph().getCell(terminal.operatorID) as joint.dia.Element)
          .getPorts()
          .filter(port => port.group === (input ? "out" : "in"))
          .map(port => port.id)
      ).toEqual(node(input)[direction].map(port => port.portID));
      undo.redoAction();
      expect(node(input)[direction].some(p => p.portID === first)).toBe(false);
      undo.undoAction();
      actions.addPort(terminal.operatorID, !input);
      const added = node(input)[direction].at(-1)!;
      expect(added.portID).not.toBe(first);
      expect(added.isDynamicPort).toBe(true);
    });
  }

  it("uses the native shared port name editor and preserves wiring on rename", () => {
    const terminal = node(true),
      port = terminal.outputPorts[0];
    const before = actions.getWorkflowContent().links;
    const record = actions
      .getTexeraGraph()
      .getSharedPortDescriptionType({ operatorID: terminal.operatorID, portID: port.portID })!;
    if (!record.has("displayName")) record.set("displayName", new Y.Text());
    const name = record.get("displayName")!;
    name.delete(0, name.length);
    name.insert(0, "Rows 東京");
    expect(actions.getMacroInterface()?.inputs[0].displayName).toBe("Rows 東京");
    expect(actions.getWorkflowContent().links).toEqual(before);
  });

  it("exposes a chosen native port once through normal graph commands", () => {
    const target = { operatorID: "a", portID: "input-0" };
    actions.macroTerminals.expose(target);
    expect(actions.getMacroInterface()?.inputs.length).toBe(2);
    actions.removePort(node(true).operatorID, false);
    const count = actions.getMacroInterface()!.inputs.length;
    actions.macroTerminals.expose(target, true);
    expect(actions.getMacroInterface()?.inputs.length).toBe(count + 1);
    undo.undoAction();
    expect(actions.getMacroInterface()?.inputs.length).toBe(count);
  });

  it("stores terminal layout in the normal undoable position map", () => {
    const before = actions.getWorkflowContent().operatorPositions;
    actions.autoLayoutWorkflow();
    const after = actions.getWorkflowContent().operatorPositions;
    expect(after).not.toEqual(before);
    expect(after[node(true).operatorID]).toEqual(
      actions.getJointGraphWrapper().getElementPosition(node(true).operatorID)
    );
    undo.undoAction();
    expect(actions.getWorkflowContent().operatorPositions).toEqual(before);
    undo.redoAction();
    expect(actions.getWorkflowContent().operatorPositions).toEqual(after);
  });
});

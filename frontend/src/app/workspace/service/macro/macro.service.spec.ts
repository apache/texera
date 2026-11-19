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
import { setMacroGraph, testMacro, testPort } from "../../../common/testing/macro-graph";
import { testBinding, testWorkflow } from "../../../common/testing/workflow-graph";

import { editMacroGraph } from "../../../../../../common/workflow-core/src/test/typescript/macro-test-graph";

import { of, Subject, throwError } from "rxjs";
import { MacroService } from "./macro.service";
import { TestBed } from "@angular/core/testing";
import { HttpTestingController } from "@angular/common/http/testing";
import { commonTestImports } from "../../../common/testing/test-utils";
import { AppSettings } from "../../../common/app-setting";
import { macroWorkspaceTestProviders } from "./macro-workspace.spec-harness";
import { WorkflowActionService, DEFAULT_WORKFLOW } from "../workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { mockSentimentPredicate, mockResultPredicate } from "../workflow-graph/model/mock-workflow-data";
import { macroInstance, macroType, readMacroInterface } from "../../../common/util/workflow-macro";
import { UndoRedoService } from "../undo-redo/undo-redo.service";

describe("MacroService", () => {
  const op: OperatorPredicate = {
    ...mockSentimentPredicate,
    operatorID: "leaf",
    operatorVersion: "Nlp1",
    operatorProperties: { attribute: "original" },
  };
  let actions: WorkflowActionService, persist: any, versions: any, service: MacroService;
  const content = () => actions.getWorkflowContent();
  const catalogRequest = () =>
    TestBed.inject(HttpTestingController).expectOne(`${AppSettings.getApiEndpoint()}/workflow/macros`);
  const load = (content: WorkflowContent, wid = 1) =>
    actions.reloadWorkflow({ ...DEFAULT_WORKFLOW, wid, name: "Workflow", content }, false);
  beforeEach(() => {
    versions = {
      pinMacro: vi.fn((wid: number, id: string) => {
        const [content, name] = persist.createWorkflow.mock.lastCall;
        return of({ wid, id, version: 5, revision: 1, content, name });
      }),
    };
    TestBed.configureTestingModule({
      imports: commonTestImports,
      providers: [...macroWorkspaceTestProviders, { provide: WorkflowVersionService, useValue: versions }],
    });
    persist = TestBed.inject(WorkflowPersistService);
    vi.spyOn(persist, "createWorkflow").mockImplementation((content, name) =>
      of({ workflow: { wid: 12, name, content } })
    );
    actions = TestBed.inject(WorkflowActionService);
    actions.suspendWorkflow().shared.wsProvider.disconnect();
    service = TestBed.inject(MacroService);
    load(
      testWorkflow([op], [], {
        operatorPositions: { leaf: { x: 25, y: 40 } },
        formBinding: testBinding("leaf", "attribute", "value", "Value"),
      })
    );
    actions.getJointGraphWrapper().highlightOperators("leaf");
    vi.spyOn(actions, "deleteOperatorsAndLinks");
  });
  afterEach(() => actions.destroySharedModel());

  it("creates an independent version before collapsing, preserving property bindings", async () => {
    const d = await service.createFromSelection("Clean");
    expect(d.version).toBe(5);
    expect(d.wid).toBe(12);
    expect(persist.createWorkflow).toHaveBeenCalledOnce();
    expect(content().operators).toHaveLength(1);
    expect(content().operators[0].operatorType).toBe(macroType(d));
    expect(content().formBinding!.fields[0].operatorID).toBe(content().operators[0].operatorID);
    expect(content().formBinding!.fields[0].propertyKey).toBe("value");
  });

  it("resolves the latest source on insertion while keeping existing usages and history ordinals", async () => {
    const previous = await service.createFromSelection("Clean");
    versions.pinMacro.mockReturnValueOnce(of({ ...previous, version: 64, revision: 3 }));
    const before = content();
    const latest = await service.latestFor(macroType(previous));
    expect(latest.version).toBe(64);
    expect(latest.revision).toBe(3);
    expect(versions.pinMacro).toHaveBeenCalledWith(12, previous.id, undefined);
    versions.pinMacro.mockReturnValueOnce(of({ ...previous, revision: 2 }));
    expect((await service.pin(previous, 5)).revision).toBe(2);
    expect(versions.pinMacro).toHaveBeenCalledWith(12, previous.id, 5);
    versions.pinMacro.mockReturnValueOnce(throwError(() => new Error("Macro source identity differs")));
    await expect(service.latestFor(macroType(previous))).rejects.toThrow(/source/i);
    versions.pinMacro.mockReturnValueOnce(throwError(() => new Error("Macro revision is not in its source history")));
    await expect(service.pin(previous, 64)).rejects.toThrow("Macro revision is not in its source history");
    expect(content()).toEqual(before);
  });

  it("leaves the selection intact on save failure or while locked", async () => {
    persist.createWorkflow.mockReturnValueOnce(throwError(() => new Error("save failed")));
    await expect(service.createFromSelection("Clean")).rejects.toThrow("save failed");
    versions.pinMacro.mockReturnValueOnce(throwError(() => new Error("Saved version is not a macro definition")));
    await expect(service.createFromSelection("Clean")).rejects.toThrow("Saved version is not a macro definition");
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
    await expect(service.createFromSelection("   ")).rejects.toThrow(/name/);
    actions.disableWorkflowModification();
    await expect(service.createFromSelection("Clean")).rejects.toThrow(/read.only/i);
  });

  it("remaps a parent macro's exposed ports when its operators are collapsed into a nested macro", async () => {
    setMacroGraph(actions, {
      id: "parent",
      inputs: [testPort("in", "leaf", "input-0")],
      outputs: [testPort("out", "leaf", "output-0")],
    });
    actions.getJointGraphWrapper().highlightOperators("leaf");
    const nested = await service.createFromSelection("Nested");
    const instance = content().operators.find(op => op.operatorType === macroType(nested))!;
    expect(actions.getMacroInterface()).toMatchObject({
      id: "parent",
      inputs: [testPort("in", instance.operatorID, instance.inputPorts[0].portID)],
      outputs: [testPort("out", instance.operatorID, instance.outputPorts[0].portID)],
    });
  });

  it.each(["unavailable", "ordinary workflow"])("discovers valid macros alongside a %s", async other => {
    const created = await service.createFromSelection("Clean");
    const usages = [{ id: created.id, wid: 42, label: "Workflow / Clean", path: ["selected"] }];
    const errors = other === "unavailable" ? ["Other: not available"] : [];
    const refreshed = service.refresh();
    expect(service.refresh()).toBe(refreshed);
    catalogRequest().flush({ definitions: [created], usages, errors });
    if (other === "unavailable") await expect(refreshed).rejects.toThrow(/Other/);
    else await expect(refreshed).resolves.toEqual([created]);
    expect(service.library$.value).toEqual([created]);
    expect(service.usages$.value).toEqual(usages);
    TestBed.inject(HttpTestingController).expectNone(req => req.url.includes("/workflow/"));
  });

  it.each(["edit", "workflow"])("does not collapse after a concurrent %s during creation", async change => {
    const response = new Subject<{ workflow: Workflow }>();
    persist.createWorkflow.mockReturnValue(response);
    const creating = service.createFromSelection("Clean");
    await vi.waitFor(() => expect(persist.createWorkflow).toHaveBeenCalledOnce());
    const saved = persist.createWorkflow.mock.calls[0][0];
    if (change === "edit") actions.setOperatorProperty("leaf", { attribute: "new edit" });
    else load(content(), 2);
    vi.mocked(actions.deleteOperatorsAndLinks).mockClear(); // Ignore the explicit reload's normal cleanup.
    response.next({ workflow: { wid: 12, name: "Clean", content: saved } as Workflow });
    response.complete();
    await expect(creating).rejects.toThrow(/changed/i);
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
    if (change === "edit") expect(content().operators[0].operatorProperties.attribute).toBe("new edit");
    else expect(content().operators).toEqual([op]);
  });

  it("forks a portable definition into a newly versioned editable copy", async () => {
    const original = await service.createFromSelection("Clean");
    const copy = await service.copyDefinition(original, []);
    expect(copy.id).not.toBe(original.id);
    expect(copy.content.operators).toEqual(original.content.operators);
    expect(copy.content.macroInterface?.id).toBe(copy.id);
    expect(service.library$.value).toContain(copy);
    expect(original.content.macroInterface?.id).toBe(original.id);
  });

  it.each(["inputs", "outputs"] as const)(
    "pins, adopts and copies unfinished %s without making them runnable",
    async direction => {
      const original = await service.createFromSelection("Clean");
      const draft = structuredClone(original.content);
      editMacroGraph(draft, boundary => {
        boundary[direction] = [{ portID: "pending", targets: [] }];
      });
      versions.pinMacro.mockReturnValueOnce(of({ ...original, version: 6, revision: 2, content: draft }));
      const latest = await service.latestFor(macroType(original));
      expect(latest.version).toBe(6);
      await service.adoptVersion(content().operators[0].operatorID, latest);
      expect(content().operators[0].operatorType).toBe(macroType(latest));

      const copy = await service.copyDefinition(latest, []);
      expect(readMacroInterface(copy.content)![direction][0].targets).toEqual([]);
      expect(copy.id).not.toBe(original.id);
      expect(readMacroInterface(original.content)![direction].every(p => p.targets.length > 0)).toBe(true);
    }
  );

  it("adopts changed schemas without discarding retained overrides, leaving validation to Texera", async () => {
    const previous = await service.createFromSelection("Clean");
    const latest = { ...previous, version: 6 };
    const id = content().operators[0].operatorID;
    const editor = actions.getJointGraphWrapper();
    editor.highlightOperators(id);
    actions.setOperatorProperty(id, { value: "mine" });
    const before = content();
    const moved = vi.fn();
    actions.suspendWorkflow().shared.elementPositionMap.observe(moved);
    service.adoptVersion(id, latest);
    expect(moved).not.toHaveBeenCalled();
    expect(editor.getCurrentHighlightedOperatorIDs()).toContain(id);
    const adopted = content();
    const undo = TestBed.inject(UndoRedoService);
    undo.undoAction();
    expect(content()).toEqual(before);
    undo.redoAction();
    expect(content()).toEqual(adopted);
    expect(content().operators[0].operatorType).toBe(macroType(latest));
    expect(content().operators[0].operatorProperties).toEqual({ value: "mine" });
    const next = structuredClone(latest);
    next.version = 7;
    next.content.operators[0] = { ...mockResultPredicate, operatorID: "leaf" };
    next.content = { ...next.content, formBinding: testBinding("leaf", "limit", "value", "Value") };
    service.adoptVersion(content().operators[0].operatorID, next);
    expect(content().operators[0].operatorType).toBe(macroType(next));
    expect(content().operators[0].operatorProperties).toEqual({ value: "mine" });
  });

  it("adopts a version with its bundled nested definitions while preserving overrides", async () => {
    const previous = await service.createFromSelection("Clean");
    actions.setOperatorProperty(content().operators[0].operatorID, { value: "mine" });
    const { inputs, outputs } = readMacroInterface(previous.content)!;
    const child = testMacro("child", [op], {
      inputs,
      outputs,
      formBinding: testBinding("leaf", "attribute", "forward", "Value"),
    });
    const latest = testMacro(previous.id, [macroInstance(child, "leaf")], {
      version: 6,
      inputs,
      outputs,
      macroDependencies: [child],
      formBinding: testBinding("leaf", "forward", "value", "Value"),
    });
    service.adoptVersion(content().operators[0].operatorID, latest);
    expect(content().operators[0].operatorType).toBe(macroType(latest));
    expect(content().operators[0].operatorProperties).toEqual({ value: "mine" });
    expect(content().macroDependencies!.find(d => d.id === child.id)!.content.formBinding).toEqual(
      child.content.formBinding
    );
  });

  it("rejects a conflicting local revision on adoption and on library refresh", async () => {
    const previous = await service.createFromSelection("Clean");
    const conflict = structuredClone(previous);
    conflict.content.operators[0] = { ...op, operatorProperties: { attribute: "different deployment" } };
    const before = JSON.stringify(content());
    expect(() => service.adoptVersion(content().operators[0].operatorID, conflict)).toThrow(
      /Conflicting macro version/
    );
    expect(JSON.stringify(content())).toBe(before);
    service.library$.next([]);
    const refreshed = service.refresh();
    catalogRequest().flush({ definitions: [conflict], usages: [], errors: [] });
    await expect(refreshed).rejects.toThrow(/Conflicting macro version/);
    expect(service.library$.value).toEqual([]);
    expect(content().macroDependencies).toEqual([previous]);
  });
  it("stores repeated creation as a flat bundle while preserving the saved bodies", async () => {
    const savedBodies = new Map<string, WorkflowContent>();
    const bundleSizes: number[] = [];
    for (let depth = 1; depth <= 12; depth++) {
      actions.getJointGraphWrapper().highlightOperators(...content().operators.map(op => op.operatorID));
      const definition = await service.createFromSelection(`Nested ${depth}`);
      const submitted: WorkflowContent = persist.createWorkflow.mock.lastCall[0];
      const { macroDependencies = [], ...body } = structuredClone(submitted);
      expect(macroDependencies.every(d => !d.content.macroDependencies)).toBe(true);
      savedBodies.set(definition.id, body);
      const dependencies = content().macroDependencies!;
      expect(dependencies.every(d => !d.content.macroDependencies)).toBe(true);
      expect(new Map(dependencies.map(d => [d.id, d.content]))).toEqual(savedBodies);
      bundleSizes.push(JSON.stringify(content()).length);
    }
    expect(bundleSizes[7]).toBeLessThan(20000);
    expect(bundleSizes[11]).toBeLessThan(bundleSizes[7] * 1.6);
  });
});

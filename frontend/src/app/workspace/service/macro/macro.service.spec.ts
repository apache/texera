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
import { of, Subject, throwError } from "rxjs";
import { MacroService } from "./macro.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { ExecutionMode, Workflow, WorkflowContent } from "../../../common/type/workflow";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { expandMacros, macroInstance, macroType } from "../../../common/util/workflow-macro";

describe("MacroService", () => {
  const op: OperatorPredicate = {
    operatorID: "leaf",
    operatorType: "Filter",
    operatorVersion: "1",
    inputPorts: [{ portID: "input-0" }],
    outputPorts: [{ portID: "output-0" }],
    operatorProperties: { value: "original" },
    showAdvanced: false,
  };
  let content: WorkflowContent;
  let actions: any, persist: any, versions: any, metadata: any, service: MacroService;
  beforeEach(() => {
    content = {
      operators: [op],
      links: [],
      operatorPositions: { leaf: { x: 25, y: 40 } },
      commentBoxes: [],
      settings: { dataTransferBatchSize: 10, executionMode: ExecutionMode.PIPELINED },
      formBinding: { fields: [{ id: "value", operatorID: "leaf", propertyKey: "value", displayName: "Value" }] },
    };
    actions = {
      checkWorkflowModificationEnabled: () => true,
      getWorkflowContent: () => content,
      bundleMacroActions: (fn: () => void) => fn(),
      getWorkflow: () => ({ wid: 1, name: "Workflow", content }),
      getWorkflowMetadata: () => ({ wid: 1 }),
      getJointGraphWrapper: () => ({ getCurrentHighlightedOperatorIDs: () => ["leaf"] }),
      getMacroDependencies: () => content.macroDependencies ?? [],
      getTexeraGraph: () => ({
        bundleActions: (fn: () => void) => fn(),
        getOperator: (id: string) => content.operators.find(o => o.operatorID === id),
      }),
      deleteOperatorsAndLinks: vi.fn(() => {
        content = { ...content, operators: [], links: [] };
      }),
      addOperatorsAndLinks: vi.fn((ops: { op: OperatorPredicate }[], links: WorkflowContent["links"]) => {
        content = { ...content, operators: ops.map(o => o.op), links };
      }),
      setMacroDependencies: vi.fn(d => {
        content = { ...content, macroDependencies: d };
      }),
      setFormBinding: vi.fn(f => {
        content = { ...content, formBinding: f };
      }),
    };
    persist = {
      createWorkflow: vi.fn((c: WorkflowContent, name: string) => of({ workflow: { wid: 12, name, content: c } })),
      retrieveWorkflowsBySessionUser: vi.fn(() => of([])),
      retrieveWorkflow: vi.fn(),
    };
    versions = {
      retrieveVersionsOfWorkflow: vi.fn(() => of([{ vId: 5 }])),
      // History applies each save's inverse patch inclusively: the creation version returns {}.
      retrieveWorkflowByVersion: vi.fn((wid: number, vid: number) =>
        of({ wid, name: "Clean", content: vid > 5 ? persist.createWorkflow.mock.calls.at(-1)[0] : {} })
      ),
    };
    metadata = {
      registerMacros: vi.fn(),
      getOperatorSchema: vi.fn(() => ({ jsonSchema: { properties: { value: { type: "string" } } } })),
    };
    service = new MacroService(
      actions as WorkflowActionService,
      persist as WorkflowPersistService,
      versions as WorkflowVersionService,
      metadata as OperatorMetadataService
    );
  });

  it("creates an independent version before collapsing, preserving property bindings", async () => {
    const d = await service.createFromSelection("Clean");
    expect(d.version).toBe(5);
    expect(d.wid).toBe(12);
    expect(persist.createWorkflow).toHaveBeenCalledOnce();
    expect(content.operators).toHaveLength(1);
    expect(content.operators[0].operatorType).toBe(macroType(d));
    expect(content.formBinding!.fields[0].operatorID).toBe(content.operators[0].operatorID);
    expect(content.formBinding!.fields[0].propertyKey).toBe("value");
    expect(expandMacros(content).content.operators[0].operatorProperties).toEqual(op.operatorProperties);
  });

  it("leaves the selection intact on save failure or while locked", async () => {
    persist.createWorkflow.mockReturnValue(throwError(() => new Error("save failed")));
    await expect(service.createFromSelection("Clean")).rejects.toThrow("save failed");
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
    actions.checkWorkflowModificationEnabled = () => false;
    await expect(service.createFromSelection("Clean")).rejects.toThrow(/read.only/i);
    await expect(service.createFromSelection("   ")).rejects.toThrow();
  });

  it("remaps a parent macro's exposed ports when its operators are collapsed into a nested macro", async () => {
    content = {
      ...content,
      macroInterface: {
        id: "parent",
        inputs: [{ portID: "in", targets: [{ operatorID: "leaf", portID: "input-0" }] }],
        outputs: [{ portID: "out", targets: [{ operatorID: "leaf", portID: "output-0" }] }],
      },
    };
    actions.setMacroInterface = vi.fn(boundary => {
      content = { ...content, macroInterface: boundary };
    });
    await service.createFromSelection("Nested");
    const instance = content.operators[0];
    expect(content.macroInterface!.id).toBe("parent");
    expect(content.macroInterface!.inputs[0].targets).toEqual([
      { operatorID: instance.operatorID, portID: instance.inputPorts[0].portID },
    ]);
    expect(content.macroInterface!.outputs[0].targets).toEqual([
      { operatorID: instance.operatorID, portID: instance.outputPorts[0].portID },
    ]);
    expect(() => expandMacros(content)).not.toThrow();
  });

  it("keeps valid library entries available when another saved definition cannot be loaded", async () => {
    const created = await service.createFromSelection("Clean");
    service.library$.next([]);
    persist.retrieveWorkflowsBySessionUser.mockReturnValue(
      of([{ workflow: { wid: 12, name: "Clean" } }, { workflow: { wid: 99, name: "Broken" } }])
    );
    persist.retrieveWorkflow.mockImplementation((wid: number) =>
      wid === 12 ? of({ wid, name: "Clean", content: created.content }) : throwError(() => new Error("not available"))
    );
    versions.retrieveWorkflowByVersion.mockReturnValue(of({ wid: 12, content: created.content }));
    await expect(service.refresh()).rejects.toThrow(/Broken/);
    expect(service.library$.value.map(d => d.id)).toEqual([created.id]);
  });

  it("rejects a saved snapshot without a macro interface without changing the selection", async () => {
    versions.retrieveWorkflowByVersion.mockReturnValue(of({ wid: 12, content: {} }));
    await expect(service.createFromSelection("Clean")).rejects.toThrow("Saved version is not a macro definition");
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
  });

  it("never overwrites changes made while the creation request is in flight", async () => {
    const response = new Subject<{ workflow: Workflow }>();
    persist.createWorkflow.mockReturnValue(response);
    const creating = service.createFromSelection("Clean");
    const saved = persist.createWorkflow.mock.calls[0][0];
    content = { ...content, operators: [{ ...op, operatorProperties: { value: "new edit" } }] };
    response.next({ workflow: { wid: 12, name: "Clean", content: saved } as Workflow });
    response.complete();
    await expect(creating).rejects.toThrow(/changed/i);
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
    expect(content.operators[0].operatorProperties.value).toBe("new edit");
  });

  it("does not collapse a different workflow with identical content while a save is in flight", async () => {
    const response = new Subject<{ workflow: Workflow }>();
    persist.createWorkflow.mockReturnValue(response);
    const creating = service.createFromSelection("Clean");
    const saved = persist.createWorkflow.mock.calls[0][0];
    actions.getWorkflowMetadata = () => ({ wid: 2 });
    response.next({ workflow: { wid: 12, name: "Clean", content: saved } as Workflow });
    response.complete();
    await expect(creating).rejects.toThrow(/changed/i);
    expect(actions.deleteOperatorsAndLinks).not.toHaveBeenCalled();
    expect(content.operators).toEqual([op]);
  });

  it("discovers only accessible definitions and resolves the exact latest version", async () => {
    const created = await service.createFromSelection("Clean");
    const macroWorkflow = { wid: 12, name: "Clean", content: created.content };
    persist.retrieveWorkflowsBySessionUser.mockReturnValue(of([{ workflow: { wid: 12 } }, { workflow: { wid: 99 } }]));
    persist.retrieveWorkflow.mockImplementation((wid: number) =>
      of(wid === 12 ? macroWorkflow : { wid, content: { ...created.content, macroInterface: undefined } })
    );
    versions.retrieveWorkflowByVersion.mockReturnValue(of(macroWorkflow));
    const library = await service.refresh();
    expect(library).toHaveLength(1);
    expect(versions.retrieveWorkflowByVersion).toHaveBeenCalledWith(12, 6);
    expect(library[0].id).toBe(created.id);
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

  it("adopts compatible versions explicitly and rejects overrides invalid under a new property schema", async () => {
    const previous = await service.createFromSelection("Clean");
    const latest = { ...previous, version: 6 };
    content = { ...content, operators: [{ ...content.operators[0], operatorProperties: { value: "mine" } }] };
    vi.spyOn(service, "refresh").mockResolvedValue([latest]);
    await service.adoptLatest(content.operators[0].operatorID);
    expect(content.operators[0].operatorType).toBe(macroType(latest));
    expect(content.operators[0].operatorProperties).toEqual({ value: "mine" });
    vi.mocked(service.refresh).mockResolvedValue([{ ...latest, version: 7 }]);
    metadata.getOperatorSchema.mockReturnValue({ jsonSchema: { properties: { value: { type: "number" } } } });
    const before = JSON.stringify(content);
    await expect(service.adoptLatest(content.operators[0].operatorID)).rejects.toThrow(/properties.*incompatible/i);
    expect(JSON.stringify(content)).toBe(before);
  });

  it("rejects a local version that conflicts with an imported pinned snapshot", async () => {
    const previous = await service.createFromSelection("Clean");
    const latest = {
      ...previous,
      content: { ...previous.content, operators: [{ ...op, operatorProperties: { value: "different deployment" } }] },
    };
    vi.spyOn(service, "refresh").mockResolvedValue([latest]);
    const before = JSON.stringify(content);
    await expect(service.adoptLatest(content.operators[0].operatorID)).rejects.toThrow(/Conflicting macro version/);
    expect(JSON.stringify(content)).toBe(before);
  });

  it("validates adoption against the version's bundled schemas instead of another library entry", async () => {
    const previous = await service.createFromSelection("Clean");
    content = { ...content, operators: [{ ...content.operators[0], operatorProperties: { value: "mine" } }] };
    const child = {
      ...previous,
      id: "child",
      content: {
        ...previous.content,
        macroInterface: { ...previous.content.macroInterface!, id: "child" },
        formBinding: { fields: [{ id: "forward", operatorID: "leaf", propertyKey: "value", displayName: "Value" }] },
      },
    };
    const latest = {
      ...previous,
      version: 6,
      content: {
        ...previous.content,
        operators: [macroInstance(child, "leaf")],
        macroDependencies: [child],
        formBinding: { fields: [{ id: "value", operatorID: "leaf", propertyKey: "forward", displayName: "Value" }] },
      },
    };
    vi.spyOn(service, "refresh").mockResolvedValue([latest]);
    metadata.getOperatorSchema.mockImplementation((_type: string, definitions: any[] = []) => ({
      jsonSchema: { properties: { forward: { type: definitions.some(d => d.id === child.id) ? "string" : "number" } } },
    }));
    await service.adoptLatest(content.operators[0].operatorID);
    expect(content.operators[0].operatorType).toBe(macroType(latest));
    expect(content.operators[0].operatorProperties).toEqual({ value: "mine" });
    expect(expandMacros(content).content.operators[0].operatorProperties).toEqual({ value: "mine" });
  });

  it("excludes conflicting library versions instead of resolving them to the current workflow's snapshot", async () => {
    const previous = await service.createFromSelection("Clean");
    const latest = {
      ...previous.content,
      operators: [{ ...op, operatorProperties: { value: "different deployment" } }],
    };
    service.library$.next([]);
    persist.retrieveWorkflowsBySessionUser.mockReturnValue(of([{ workflow: { wid: 12, name: "Clean" } }]));
    persist.retrieveWorkflow.mockReturnValue(of({ wid: 12, name: "Clean", content: latest }));
    versions.retrieveWorkflowByVersion.mockReturnValue(of({ wid: 12, content: latest }));
    await expect(service.refresh()).rejects.toThrow(/Conflicting macro version/);
    expect(service.library$.value).toEqual([]);
    expect(content.macroDependencies).toEqual([previous]);
  });
});

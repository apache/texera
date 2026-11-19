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
import { Text } from "yjs";
import type { MockInstance } from "vitest";
import { updateYTypeFromObject } from "../../types/shared-editing.interface";
import { testPort, testMacro, setMacroGraph, expansionFixture } from "../../../common/testing/macro-graph";
import { testBinding, testWorkflow, testLink } from "../../../common/testing/workflow-graph";
import {
  isMacroTerminal,
  readMacroInterface,
} from "../../../../../../common/workflow-core/src/main/typescript/workflow-macros";

import { macroWorkspaceTestProviders } from "./macro-workspace.spec-harness";
import { MacroExpansionService } from "./macro-expansion.service";
import { TestBed } from "@angular/core/testing";
import { of, throwError, Subject, switchMap, take } from "rxjs";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { TexeraWebsocketEvent } from "../../types/workflow-websocket.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { OperatorState, OperatorRuntimeStatus } from "../../types/execute-workflow.interface";
import { OperatorPredicate } from "../../types/workflow-common.interface";
import { MacroNavigationService } from "./macro-navigation.service";
import { MacroService } from "./macro.service";
import { WorkflowActionService, DEFAULT_WORKFLOW } from "../workflow-graph/model/workflow-action.service";
import {
  MacroSaveStep,
  WorkflowPersistService,
} from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import {
  mockScanPredicate,
  mockSentimentPredicate,
  mockResultPredicate,
} from "../workflow-graph/model/mock-workflow-data";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import {
  MacroDefinition,
  macroInstance,
  macroType,
  replaceMacroVersion,
  macroRuntimeID,
  MacroExpansion,
} from "../../../common/util/workflow-macro";

import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { FormBindingService } from "../form-binding/form-binding.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";

describe("MacroNavigationService", () => {
  let actions: WorkflowActionService;
  let service: MacroNavigationService;
  let sourceFor: MockInstance<MacroService["sourceFor"]>;
  let persist: ReturnType<typeof persistence>;
  let status: WorkflowStatusService;
  let undo: UndoRedoService;
  let events: Subject<TexeraWebsocketEvent>;
  let saved: Map<number, Workflow>;
  let revisions: Map<number, number>;
  let leaf: MacroDefinition;
  let parent: MacroDefinition;
  let root: Workflow;
  let originalRoot: Workflow;
  const sourceMacro = (
    id: "leaf" | "parent",
    operators: OperatorPredicate[],
    options: Parameters<typeof testMacro>[2] = {}
  ) => testMacro(id, operators, { name: id === "leaf" ? "Leaf" : "Parent", wid: id === "leaf" ? 20 : 10, ...options });
  const siblings = (definition: MacroDefinition, first: string, second: string, properties = {}) => [
    { ...macroInstance(definition, first), operatorProperties: properties },
    macroInstance(definition, second),
  ];
  const expectEditablePin = (definition: MacroDefinition) => {
    expect(actions.checkWorkflowModificationEnabled()).toBe(true);
    expect(actions.getWorkflowContent().operators).toEqual(definition.content.operators);
    expect(actions.openWorkflowDraft).toHaveBeenLastCalledWith(expect.anything(), false);
    expect(service.readOnlyReason).toBe("");
  };
  const siblingRoot = (...dependencies: MacroDefinition[]) =>
    testWorkflow(siblings(parent, "selected", "other"), [], {
      macroDependencies: [parent, ...dependencies],
    });
  const pinned = (bundle: WorkflowContent, op: OperatorPredicate) =>
    bundle.macroDependencies!.find(d => macroType(d) === op.operatorType)!;
  const unavailableRevision = (head?: MacroDefinition) =>
    sourceFor.mockResolvedValue({
      metadata: saved.get(10)!,
      head,
      revisions: head ? [head.version] : [],
    });
  const expectNoSave = () => {
    expect(persist.persistWorkflow).not.toHaveBeenCalled();
    expect(persist.persistMacroPath).not.toHaveBeenCalled();
  };
  const editScan = (tableName: string) => actions.setOperatorProperty(mockScanPredicate.operatorID, { tableName });
  const scanLeaves = (tableName?: string): [string[], OperatorPredicate][] => {
    const scan = leaf.content.operators[0];
    const edited = tableName === undefined ? scan : { ...scan, operatorProperties: { tableName } };
    return ["selected", "other"].flatMap(instance =>
      ["child", "sibling"].map((child): [string[], OperatorPredicate] => [
        [instance, child, scan.operatorID],
        instance === "selected" && child === "child" ? edited : scan,
      ])
    );
  };
  const cacheExpansion = (content: WorkflowContent, leaves = scanLeaves(), links: WorkflowContent["links"] = []) => {
    const response = expansionFixture(content, leaves, links);
    TestBed.inject(MacroExpansionService).remember(content, response);
    return response;
  };
  const completedScan: OperatorRuntimeStatus = {
    operatorState: OperatorState.Completed,
    aggregatedInputRowCount: 0,
    aggregatedOutputRowCount: 4,
    numWorkers: 1,
  };
  const publishStatistics = (
    expanded: MacroExpansion,
    sample = (_path: string[]): OperatorRuntimeStatus => completedScan
  ) => {
    const leaves = Object.fromEntries(Object.entries(expanded.paths).map(([id, path]) => [id, sample(path)]));
    status.setExternalStatus(
      { ...leaves, selected: { ...completedScan, numWorkers: 2 }, other: completedScan },
      root.content
    );
  };
  const recordedExecution = async (samples: Record<string, OperatorRuntimeStatus>, eid: number) => {
    vi.mocked(TestBed.inject(WorkflowExecutionsService).retrieveMacroContent).mockReturnValue(of(root.content));
    status.setExternalStatus(samples, eid);
    await vi.waitFor(() => expect(status.getRuntimeContent()).toEqual(root.content));
  };
  const openPath = async (...ids: string[]) => {
    for (const id of ids) await service.open(id);
  };
  const bodyTypes = (content: WorkflowContent) =>
    content.operators.filter(op => !isMacroTerminal(op)).map(op => op.operatorType);
  const editNestedScan = async (tableName: string) => {
    await openPath("selected", "child");
    editScan(tableName);
  };
  const expectRejectedSave = async (reason: string, head: number) => {
    const before = structuredClone(saved);
    await editNestedScan("draft");
    persist.persistMacroPath.mockReturnValueOnce(throwError(() => new Error(reason)));
    await expect(service.save()).rejects.toThrow(reason);
    const [steps] = persist.persistMacroPath.mock.lastCall!;
    expect(steps[0].head?.version).toBe(head);
    expect(steps.map(step => step.workflow.wid)).toEqual([20, 10, 1]);
    expect(saved).toEqual(before);
    expect(actions.getWorkflowContent().operators[0].operatorProperties.tableName).toBe("draft");
  };
  const startPendingSave = async () => {
    const release = new Subject<void>();
    const write = persist.persistMacroPath.getMockImplementation()!;
    persist.persistMacroPath.mockImplementationOnce((steps, rootBefore) =>
      release.pipe(
        take(1),
        switchMap(() => write(steps, rootBefore))
      )
    );
    const nextWrite = persist.persistMacroPath.mock.calls.length + 1;
    const saving = service.save();
    await vi.waitFor(() => expect(persist.persistMacroPath).toHaveBeenCalledTimes(nextWrite));
    return { saving, release: () => release.next() };
  };
  const install = (content = root.content) => {
    root = { ...root, content };
    originalRoot = structuredClone(root);
    for (const { wid, name, content } of [leaf, parent]) saved.set(wid!, { ...DEFAULT_WORKFLOW, wid, name, content });
    saved.set(1, root);
    actions.reloadWorkflow(structuredClone(root), false);
  };
  const forwardedSettings = (output = false) => {
    leaf = sourceMacro("leaf", [mockScanPredicate], {
      formBinding: testBinding(mockScanPredicate.operatorID, "tableName", "table", "Table"),
      outputs: output ? [testPort("output-0", mockScanPredicate.operatorID)] : [],
    });
    parent = sourceMacro("parent", siblings(leaf, "child", "sibling", { table: "child override" }), {
      macroDependencies: [leaf],
      formBinding: testBinding("child", "table", "forward", "Forward"),
      outputs: output ? [testPort("output-0", "child")] : [],
    });
    const usages = siblings(parent, "selected", "other", { forward: "root override" });
    if (output) usages.push(mockResultPredicate);
    const links = output
      ? [testLink("selected", mockResultPredicate.operatorID, "output-0", "input-0", "connection")]
      : [];
    install(
      testWorkflow(usages, links, {
        macroDependencies: [parent, leaf],
        formBinding: testBinding("selected", "forward", "form", "Table"),
      })
    );
  };
  const saveRevision = (workflow: Workflow) => {
    const wid = workflow.wid!;
    const version = (revisions.get(wid) ?? 1) + 1;
    revisions.set(wid, version);
    saved.set(wid, structuredClone(workflow));
    return { version, revision: version, content: workflow.content };
  };
  const persistence = () => ({
    retrieveWorkflow: vi.fn((id: number) => of(structuredClone(saved.get(id)))),
    persistWorkflow: vi.fn((w: Workflow) => {
      saveRevision(w);
      return of(w);
    }),
    persistMacroPath: vi.fn((steps: MacroSaveStep[], _rootBefore: Workflow) => {
      let child: MacroDefinition | undefined;
      return of(
        steps.map(({ workflow, operatorID }) => {
          const content = child ? replaceMacroVersion(workflow.content, operatorID!, child) : workflow.content;
          const receipt = saveRevision({ ...workflow, content });
          if (content.macroInterface)
            child = { ...receipt, id: content.macroInterface.id, wid: workflow.wid, name: workflow.name };
          return receipt;
        })
      );
    }),
  });
  beforeEach(() => {
    events = new Subject<TexeraWebsocketEvent>();
    saved = new Map();
    revisions = new Map();
    persist = persistence();
    TestBed.configureTestingModule({
      providers: [
        ...macroWorkspaceTestProviders,
        { provide: WorkflowPersistService, useValue: persist },
        { provide: WorkflowExecutionsService, useValue: { retrieveMacroContent: vi.fn() } },
        { provide: ComputingUnitStatusService, useValue: { getSelectedComputingUnitValue: () => undefined } },
        WorkflowStatusService,
        { provide: WorkflowWebsocketService, useValue: { websocketEvent: () => events } },
      ],
    });
    actions = TestBed.inject(WorkflowActionService);
    actions.suspendWorkflow().shared.wsProvider.disconnect();
    sourceFor = vi.spyOn(TestBed.inject(MacroService), "sourceFor").mockImplementation(async definition => {
      const { content, ...metadata } = saved.get(definition.wid!)!;
      const version = revisions.get(definition.wid!) ?? 1;
      return {
        metadata,
        revisions: [version],
        head: { ...definition, name: metadata.name, version, revision: version, content: structuredClone(content) },
      };
    });
    TestBed.inject(DynamicSchemaService);
    const open = actions.openWorkflowDraft.bind(actions);
    vi.spyOn(actions, "openWorkflowDraft").mockImplementation(workflow => open(workflow, false));
    leaf = sourceMacro("leaf", [mockScanPredicate]);
    parent = sourceMacro("parent", siblings(leaf, "child", "sibling"), {
      macroDependencies: [leaf],
    });
    root = { ...DEFAULT_WORKFLOW, wid: 1, name: "Root", content: siblingRoot(leaf) };
    install();
    service = TestBed.inject(MacroNavigationService);
    status = TestBed.inject(WorkflowStatusService);
    undo = TestBed.inject(UndoRedoService);
  });
  afterEach(() => {
    service.clear();
    actions.destroySharedModel();
    expect(root).toEqual(originalRoot); // Every edit must preserve the submitted graph and all its pins.
  });

  it.each(["property", "interface"] as const)("saves a nested %s edit along only the selected path", async edit => {
    const rootDoc = actions.suspendWorkflow().shared;
    await openPath("selected", "child");
    expect(service.parents.map(p => p.definition.name)).toEqual(["Parent", "Leaf"]);
    expectNoSave();
    if (edit === "property") {
      editScan("changed");
      await service.save();
      expect(saved.get(20)!.content.operators[0].operatorProperties.tableName).toBe("changed");
    } else {
      setMacroGraph(actions, {
        ...actions.getMacroInterface()!,
        outputs: [testPort("new-output", mockScanPredicate.operatorID, "output-0")],
      });
      actions.setFormBinding(testBinding(mockScanPredicate.operatorID, "tableName", "table", "Table"));
    }
    await service.back(0);
    const latest = saved.get(1)!.content;
    expect(actions.suspendWorkflow().shared).toBe(rootDoc);
    expect(actions.getWorkflowContent()).toEqual(latest);
    expect(service.parents).toHaveLength(0);
    expect(actions.getWorkflowMetadata().wid).toBe(1);
    expect(bodyTypes(latest)).toEqual(["WorkflowMacro:parent:2", macroType(parent)]);
    const nextParent = pinned(latest, latest.operators[0]);
    expect(bodyTypes(nextParent.content)).toEqual(["WorkflowMacro:leaf:2", macroType(leaf)]);
    if (edit === "interface") {
      const nextLeaf = pinned(latest, nextParent.content.operators[0]);
      expect(readMacroInterface(nextLeaf.content)!.outputs[0].portID).toBe("new-output");
      expect(nextParent.content.operators[0].outputPorts[0].portID).toBe("new-output");
      expect(nextLeaf.content.formBinding?.fields[0].id).toBe("table");
    }
  });

  it("propagates removed nested ports and parameters, validates the parent, and undoes the entire adoption", async () => {
    forwardedSettings(true);
    const validation = TestBed.inject(ValidationWorkflowService);
    const forms = TestBed.inject(FormBindingService);
    const before = structuredClone(actions.getWorkflowContent());

    expect(validation.validateOperator(mockResultPredicate.operatorID).isValid).toBe(true);
    expect(forms.resolveFields()[0].brokenReason).toBeUndefined();
    await openPath("selected", "child");
    setMacroGraph(actions, { ...actions.getMacroInterface()!, outputs: [] });
    actions.setFormBinding({ fields: [] });
    await service.back(1);
    expect(actions.getMacroInterface()!.outputs).toEqual([{ portID: "output-0", targets: [] }]);
    expect(actions.getFormBinding()).toEqual({ fields: [] });
    expect(actions.getWorkflowContent().operators[0].operatorProperties).toEqual({});
    expect(actions.getWorkflowContent().operators[1].operatorType).toBe(macroType(leaf));
    await service.back(0);
    const changed = actions.getWorkflowContent();
    expect(changed.links).toEqual(before.links);

    expect(changed.operators[0].operatorProperties).toEqual({});
    expect(changed.operators[1].operatorType).toBe(macroType(parent));
    expect(validation.validateOperator(mockResultPredicate.operatorID)).toEqual({ isValid: true });
    expect(forms.resolveFields()).toEqual([]);

    undo.undoAction();
    expect(actions.getWorkflowContent()).toEqual(before);
    expect(validation.validateOperator(mockResultPredicate.operatorID).isValid).toBe(true);
    expect(forms.resolveFields()[0].brokenReason).toBeUndefined();
    undo.redoAction();
    expect(actions.getWorkflowContent()).toEqual(changed);

    expect(readMacroInterface(saved.get(20)!.content)!.outputs).toEqual([]);
  });

  it.each([false, true])(
    "restores forwarding after nested autosave and Undo/Redo (awaiting sync: %s)",
    async pendingSync => {
      forwardedSettings();

      await openPath("selected", "child");
      if (pendingSync)
        for (const { session } of service.parents) {
          session.shared.wsProvider.shouldConnect = true;
          session.shared.wsProvider.synced = false;
        }
      const forms = TestBed.inject(FormBindingService);
      actions.bundleMacroActions(() => forms.removeBinding("table"));
      await service.save();
      expect(saved.get(10)!.content.formBinding).toEqual({ fields: [] });
      expect(saved.get(1)!.content.formBinding).toEqual({ fields: [] });

      editScan("edited after removal");
      await service.save();
      undo.undoAction();
      await service.save();
      expect(forms.getConfig().fields).toEqual([]);
      undo.undoAction();
      expect(forms.getConfig()).toEqual(leaf.content.formBinding);
      await service.save();
      expect(saved.get(10)!.content.formBinding).toEqual(parent.content.formBinding);
      expect(saved.get(1)!.content.formBinding).toEqual(root.content.formBinding);
      expect(saved.get(1)!.content.operators[0].operatorProperties).toEqual({ forward: "root override" });
      undo.redoAction();
      await service.save();
      expect(saved.get(1)!.content.formBinding).toEqual({ fields: [] });
      undo.undoAction();
      await service.save();
      await service.back(0);
      expect(forms.resolveFields()[0]).toMatchObject({ value: "root override" });
      expect(forms.resolveFields()[0].brokenReason).toBeUndefined();
    }
  );

  it("preserves forwarding when a field removal is undone while its save is in flight", async () => {
    forwardedSettings();

    await openPath("selected", "child");
    actions.bundleMacroActions(() => TestBed.inject(FormBindingService).removeBinding("table"));
    const { saving, release } = await startPendingSave();
    undo.undoAction();
    release();
    await saving;
    await service.save();
    await service.back(0);
    expect(actions.getFormBinding()).toEqual(root.content.formBinding);
    expect(TestBed.inject(FormBindingService).resolveFields()[0]).toMatchObject({ value: "root override" });
    expect(actions.getWorkflowContent().operators[1]).toEqual(root.content.operators[1]);
  });

  it("keeps incompatible retained values visible to ordinary validation and Form View", async () => {
    leaf = sourceMacro("leaf", [mockScanPredicate, mockResultPredicate], {
      links: [testLink("1", "3", "output-0", "input-0", "internal")],
      formBinding: testBinding("1", "tableName", "parameter", "Parameter"),
    });
    install(
      testWorkflow([{ ...macroInstance(leaf, "selected"), operatorProperties: { parameter: "保留" } }], [], {
        macroDependencies: [leaf],
        formBinding: testBinding("selected", "parameter", "form", "Parameter"),
      })
    );
    const validation = TestBed.inject(ValidationWorkflowService);
    const forms = TestBed.inject(FormBindingService);
    expect(validation.validateOperator("selected").isValid).toBe(true);
    await service.open("selected");
    actions.setFormBinding(testBinding("3", "limit", "parameter", "Limit"));
    await service.back(0);
    expect(actions.getWorkflowContent().operators[0].operatorProperties).toEqual({ parameter: "保留" });
    expect(validation.validateOperator("selected")).toEqual({ isValid: false, messages: { type: "must be integer" } });
    expect(forms.resolveFields()[0]).toMatchObject({ value: "保留", schema: { type: "integer" } });
    forms.writeValue(forms.getConfig().fields[0], 7);
    expect(validation.validateOperator("selected").isValid).toBe(true);
  });

  it("joins the source document with its own dependency bundle and current name", async () => {
    saved.set(10, { ...saved.get(10)!, name: "Renamed definition" });
    await service.open("selected");
    expect(actions.getMacroDependencies().map(d => d.id)).toEqual(["leaf"]);
    expect(actions.getWorkflow().name).toBe("Renamed definition");
    expect(service.hasUnsavedChanges).toBe(false);
    await service.back(0);
    expectNoSave();
    expect(actions.getMacroDependencies()).toHaveLength(2);
  });

  it("repeatedly saves and undoes an earlier self-revision without changing other pins", async () => {
    const earlier = sourceMacro("parent", [mockScanPredicate], { name: "Leaf" });
    parent = sourceMacro(
      "parent",
      [{ ...macroInstance(earlier, "inner"), customDisplayName: "Macro3" }, macroInstance(earlier, "sibling")],
      { version: 4, macroDependencies: [earlier] }
    );
    revisions.set(10, 4);
    install(siblingRoot(earlier));

    const before = structuredClone(actions.getWorkflowContent());

    await service.open("selected");
    expect(actions.checkWorkflowModificationEnabled()).toBe(true);
    await service.open("inner");
    expect(service.current!.instanceName).toBe("Macro3");
    expectEditablePin(earlier);
    await service.save();
    expectNoSave();

    editScan("旧版 café 東京");
    undo.undoAction();
    expect(actions.getWorkflowContent().operators[0].operatorProperties).toEqual(mockScanPredicate.operatorProperties);
    undo.redoAction();
    await service.save();
    expect(service.current!.definition.version).toBe(5);
    expect(revisions.get(10)).toBe(6);
    expect(bodyTypes(saved.get(10)!.content)).toEqual(["WorkflowMacro:parent:5", "WorkflowMacro:parent:1"]);
    expect(bodyTypes(saved.get(1)!.content)).toEqual(["WorkflowMacro:parent:6", "WorkflowMacro:parent:4"]);
    editScan("second edit");
    await service.save();
    expect(service.current!.definition.version).toBe(7);
    expect(revisions.get(10)).toBe(8);
    await service.back(1);
    const outer = structuredClone(actions.getWorkflowContent());
    expect(bodyTypes(outer)).toEqual(["WorkflowMacro:parent:7", "WorkflowMacro:parent:1"]);
    undo.undoAction();
    expect(actions.getWorkflowContent().operators[0].operatorType).toBe("WorkflowMacro:parent:5");
    undo.redoAction();
    expect(actions.getWorkflowContent()).toEqual(outer);
    await service.back(0);
    const after = structuredClone(actions.getWorkflowContent());
    expect(bodyTypes(after)).toEqual(["WorkflowMacro:parent:8", "WorkflowMacro:parent:4"]);
    undo.undoAction();
    undo.undoAction();
    expect(actions.getWorkflowContent()).toEqual(before);
    undo.redoAction();
    undo.redoAction();
    expect(actions.getWorkflowContent()).toEqual(after);
  });

  it("opens the exact older pin without saving, then forks it in the source history and preserves sibling usages", async () => {
    root.content.operators[0] = { ...root.content.operators[0], customDisplayName: "Outer alias" };
    parent.content.operators[0] = { ...parent.content.operators[0], customDisplayName: "Macro3" };
    leaf.revision = 1;
    install();
    revisions.set(20, 9);
    const latest = structuredClone(saved.get(20)!);
    latest.content.operators[0] = { ...mockScanPredicate, operatorProperties: { tableName: "latest source" } };
    saved.set(20, latest);

    await openPath("selected", "child");
    expect(service.breadcrumb(0)).toBe("Root");
    expect(service.breadcrumb(1)).toBe("Outer alias · Revision 1");
    expect(service.current!.instanceName).toBe("Macro3");
    expect(service.hint).toContain("Revision 1");
    expectEditablePin(leaf);
    await service.save();
    expectNoSave();

    await service.back(0);
    expect(service.readOnlyReason).toBe("");
    await openPath("selected", "child");
    editScan("from old revision");
    await service.back(0);
    const updated = saved.get(1)!;
    const latestParent = pinned(updated.content, updated.content.operators[0]);
    expect(bodyTypes(latestParent.content)).toEqual(["WorkflowMacro:leaf:10", "WorkflowMacro:leaf:1"]);
    expect(updated.content.operators[1].operatorType).toBe(macroType(parent));
    expect(saved.get(20)!.content.operators[0].operatorProperties.tableName).toBe("from old revision");
  });

  it.each([false, true])(
    "edits a self-pin from its directly opened source, recovering a failed parent save: %s",
    async failParent => {
      const source: Workflow = {
        ...saved.get(20)!,
        content: sourceMacro("leaf", [macroInstance(leaf, "inner")], { macroDependencies: [leaf] }).content,
      };
      saved.set(20, source);
      revisions.set(20, 4);
      actions.reloadWorkflow(source, false);
      const before = structuredClone(actions.getWorkflowContent());

      await service.open("inner");
      expectEditablePin(leaf);
      editScan("edited self");
      if (failParent) {
        persist.persistMacroPath.mockReturnValueOnce(throwError(() => new Error("parent save failed")));
        await expect(service.save()).rejects.toThrow("parent save failed");
      }
      await service.back(0);
      const after = structuredClone(actions.getWorkflowContent());
      expect(after).toEqual(saved.get(20)!.content);
      expect(after.operators[0].operatorType).not.toBe(macroType(leaf));

      undo.undoAction();
      expect(actions.getWorkflowContent()).toEqual(before);
      undo.redoAction();
      expect(actions.getWorkflowContent()).toEqual(after);
    }
  );

  it("creates new revisions through an indirect self-path A4 → B3 → A1 without changing other usages", async () => {
    const earlier = sourceMacro("parent", [mockScanPredicate], { name: "Leaf" });
    leaf = sourceMacro("leaf", [macroInstance(earlier, "inner")], { version: 3, macroDependencies: [earlier] });
    parent = sourceMacro("parent", [macroInstance(leaf, "child")], {
      version: 4,
      macroDependencies: [leaf, earlier],
    });
    revisions.set(10, 4);
    revisions.set(20, 3);
    install(siblingRoot(leaf, earlier));

    await openPath("selected", "child", "inner");
    expectNoSave();
    editScan("indirect edit");
    await service.back(0);
    expect(revisions.get(10)).toBe(6);
    expect(revisions.get(20)).toBe(4);
    expect(saved.get(10)!.content.operators[0].operatorType).toBe("WorkflowMacro:leaf:4");
    expect(saved.get(20)!.content.operators[0].operatorType).toBe("WorkflowMacro:parent:5");
    expect(bodyTypes(actions.getWorkflowContent())).toEqual(["WorkflowMacro:parent:6", "WorkflowMacro:parent:4"]);
  });

  it.each([
    ["read-only source", /read-only access/i, () => saved.set(10, { ...saved.get(10)!, readonly: true })],
    ["missing source", /source unavailable/i, () => saved.delete(10)],
    [
      "foreign source",
      /source unavailable/i,
      () => sourceFor.mockRejectedValue(new Error("Macro source identity differs")),
    ],
    ["read-only parent", /parent workflow/i, () => actions.disableWorkflowModification()],
    ["missing history", /could not be verified/i, () => unavailableRevision()],
    ["foreign revision", /could not be verified/i, () => unavailableRevision(testMacro("foreign", [], { version: 2 }))],
  ] as const)("opens the pinned graph for a %s and leaves it unchanged", async (_reason, message, changeAccess) => {
    const before = structuredClone(actions.getWorkflowContent());
    changeAccess();
    await service.open("selected");
    expect(actions.checkWorkflowModificationEnabled()).toBe(false);
    expect(service.readOnlyReason).toMatch(message);
    expect(actions.getWorkflowContent().macroInterface!.id).toBe("parent");
    await service.save();
    expectNoSave();
    await service.back(0);
    expect(actions.getWorkflowContent()).toEqual(before);
  });

  it.each(["inputs", "outputs"] as const)(
    "saves unfinished %s and leaves nested drafts without changing other pins",
    async direction => {
      await openPath("selected", "child");
      setMacroGraph(actions, {
        ...actions.getMacroInterface()!,
        [direction]: [{ portID: "pending", displayName: "Pending", targets: [] }],
      });
      await service.back(0);
      const updated = actions.getWorkflowContent();
      expect(updated.operators[1].operatorType).toBe(macroType(parent));
      expect(saved.get(10)!.content.operators[1].operatorType).toBe(macroType(leaf));
      expect(readMacroInterface(saved.get(20)!.content)![direction][0].targets).toEqual([]);

      await openPath("selected", "child");
      expect(actions.getMacroInterface()![direction][0].displayName).toBe("Pending");
      await service.back(0);

      undo.undoAction();
      expect(actions.getWorkflowContent().operators[0].operatorType).toBe(macroType(parent));
    }
  );

  it.each(["parent", "child"])(
    "lets users edit a disabled %s path without inheriting sibling statistics",
    async disabled => {
      const body = disabled === "parent" ? root.content : parent.content;
      body.operators[0] = { ...body.operators[0], isDisabled: true };
      install();
      const run = cacheExpansion(
        root.content,
        scanLeaves().filter(([path]) => (disabled === "parent" ? path[0] !== "selected" : path[1] !== "child"))
      );
      publishStatistics(run);
      await openPath("other", "sibling");
      const active = service.compilationContext(run);
      expect(status.getCurrentStatistics()[mockScanPredicate.operatorID]?.aggregatedOutputRowCount).toBe(4);
      await service.back(0);
      await service.open("selected");
      if (disabled === "parent") expect(service.compilationContext(run).ids).toEqual({});
      await service.open("child");
      const context = service.compilationContext(run);
      expect(context.local.content.operators).toEqual([]);
      expect(context.inputLinks(mockScanPredicate.operatorID)).toEqual([]);
      expect(TestBed.inject(MacroExpansionService).get(context.content)).toBe(active.local);
      expect(status.getCurrentStatistics()).toEqual({});
      expect(service.resultAliases()[mockScanPredicate.operatorID]).toBeNull();
      expect(actions.checkWorkflowModificationEnabled()).toBe(true);
      editScan("edited while disabled");
      expect(actions.getWorkflowContent().operators[0].operatorProperties.tableName).toBe("edited while disabled");
    }
  );

  it("does not roll old worker counts onto a root usage after its input changes", () => {
    forwardedSettings(true);
    const run = cacheExpansion(actions.getWorkflowContent());
    publishStatistics(run);

    expect(status.getCurrentStatistics()["selected"]?.numWorkers).toBe(2);
    actions.setOperatorProperty("selected", { forward: "another table" });
    cacheExpansion(actions.getWorkflowContent(), scanLeaves("another table"));
    publishStatistics(run);
    expect(status.getCurrentStatistics()["selected"]).toBeUndefined();
  });

  it("hides macro results during an incomplete root graph edit", () => {
    status.setExternalStatus({}, root.content);
    vi.spyOn(actions, "getWorkflowContent").mockReturnValue({
      ...root.content,
      links: [testLink("removed", "selected")],
    });
    expect(service.resultAliases()).toEqual({ selected: null, other: null });
  });

  it("resolves each pinned output to its occurrence before and after reopening", async () => {
    const scan = mockScanPredicate;
    leaf = sourceMacro("leaf", [scan], { outputs: [testPort("rows", scan.operatorID, "output-0")] });
    parent = sourceMacro("parent", siblings(leaf, "child", "sibling"), {
      macroDependencies: [leaf],
      outputs: [testPort("left", "child", "rows"), testPort("right", "sibling", "rows")],
    });
    install(siblingRoot(leaf));
    const compiled = cacheExpansion(root.content);
    const targets = (instance: string) =>
      ["child", "sibling"].map(child => ({
        operatorID: macroRuntimeID([instance, child], scan.operatorID),
        portIndex: 0,
      }));
    compiled.resultPorts = { selected: targets("selected"), other: targets("other") };
    compiled.resultTargets = Object.fromEntries(
      Object.entries(compiled.resultPorts).map(([id, ports]) => [id, ports[0]!])
    );
    const check = () => {
      expect(service.resultAliases().selected).toEqual(targets("selected")[0]);
      expect(service.resultAliases(1)).toEqual({ selected: targets("selected")[1], other: targets("other")[1] });
      expect(service.resultAliases(2).selected).toBeNull();
    };
    status.setRuntimeContent(root.content);
    check();
    status.clearStatus();
    await recordedExecution({}, 42);
    check();
  });

  it.each([false, true])(
    "reopens the exact execution and ignores slower responses (invalid draft: %s)",
    async invalidDraft => {
      await openPath("selected", "child");
      const content = root.content;
      const pending = new Subject<WorkflowContent>();
      const api = vi.mocked(TestBed.inject(WorkflowExecutionsService).retrieveMacroContent);
      api.mockReturnValueOnce(pending).mockReturnValueOnce(of(content));
      cacheExpansion(content);
      if (invalidDraft) {
        const compiler = TestBed.inject(MacroExpansionService);
        const expand = compiler.expand.bind(compiler);
        vi.spyOn(compiler, "expand").mockImplementation(content =>
          content.macroInterface ? Promise.reject(new Error("Invalid draft")) : expand(content)
        );
      } else cacheExpansion(actions.getWorkflowContent());

      status.setExternalStatus({}, 18);
      status.setExternalStatus({}, 19);
      await vi.waitFor(() => expect(status.getRuntimeContent()).toEqual(content));
      pending.next({ ...content, operators: [] });
      pending.complete();
      await Promise.resolve();
      expect(status.getRuntimeContent()).toEqual(content);
      expect(api.mock.calls).toEqual([
        [1, 18],
        [1, 19],
      ]);
      expect(status.runtimeExecutionId).toBe(19);
    }
  );

  it("projects live statistics onto the selected nested instance and clears them as its draft changes", async () => {
    const expanded = cacheExpansion(root.content);
    publishStatistics(expanded, path => ({
      ...completedScan,
      operatorState: OperatorState.Running,
      aggregatedOutputRowCount: path[0] === "other" ? 100 : path[1] === "sibling" ? 7 : 4,
    }));
    await openPath("selected", "child");
    service.compilationContext(expanded);
    expect(status.getCurrentStatistics()[mockScanPredicate.operatorID].aggregatedOutputRowCount).toBe(4);
    expect(Object.keys(status.getCurrentStatistics())).toEqual([mockScanPredicate.operatorID]);
    editScan("draft");
    await vi.waitFor(() => expect(status.getCurrentStatistics()).toEqual({}));
    await service.back(0, true);
    service.compilationContext(expanded);
    expect(status.getCurrentStatistics()).toHaveProperty("selected");
    publishStatistics(expanded, () => ({ ...completedScan, aggregatedInputRowCount: 1 }));
    await editNestedScan("new revision");
    await service.save();
    await service.back(0);
    await openPath("selected", "child");
    expect(status.getCurrentStatistics()).toEqual({});
    await service.back(0);
    status.clearStatus();
    await service.open("selected");
    expect(status.getCurrentStatistics()).toEqual({});
  });

  it("keeps live statistics for an explicitly stored default but clears a real ancestor override", async () => {
    leaf = sourceMacro("leaf", [{ ...mockScanPredicate, operatorProperties: { tableName: "default" } }], {
      formBinding: testBinding(mockScanPredicate.operatorID, "tableName", "table", "Table"),
    });
    parent.content = {
      ...parent.content,
      macroDependencies: [leaf],
      formBinding: testBinding("child", "table", "table", "Table"),
    };
    install(siblingRoot(leaf));
    const expanded = cacheExpansion(root.content);
    publishStatistics(expanded);
    await openPath("selected", "child");
    service.compilationContext(expanded);
    const usage = service.parents[0].session.shared.operatorIDMap.get("selected")!;
    const override = (value: string) => {
      updateYTypeFromObject(usage, { ...usage.toJSON(), operatorProperties: { table: value } });
      service.compilationContext(cacheExpansion(service.compilationDraft()!.content, scanLeaves(value)));
      status.refresh();
    };
    override("default");
    expect(status.getCurrentStatistics()[mockScanPredicate.operatorID]?.aggregatedOutputRowCount).toBe(4);
    usage.set("isDisabled", true);
    status.refresh();
    expect(status.getCurrentStatistics()).toEqual({});
    usage.set("isDisabled", false);
    status.refresh();
    expect(status.getCurrentStatistics()[mockScanPredicate.operatorID]?.aggregatedOutputRowCount).toBe(4);
    override("changed");
    expect(status.getCurrentStatistics()).toEqual({});
  });

  it.each([
    [1, "A newer saved revision exists"],
    [9, "A newer saved revision exists"],
    [9, "The parent is read-only"],
  ] as const)("keeps the draft from revision %s after the server rejects it: %s", async (head, reason) => {
    revisions.set(20, head);
    const published = actions.getWorkflowContent();
    await expectRejectedSave(reason, head);
    await service.back(0, true);
    expect(actions.getWorkflowContent()).toEqual(published);
  });

  it("retains a failed propagation for retry without rewriting earlier snapshots", async () => {
    await expectRejectedSave("parent save failed", 1);
    expect(saved.get(1)).toEqual(root);
    expect(revisions.has(10)).toBe(false);
    expect(revisions.has(20)).toBe(false);
    await service.save();
    await service.back(0);
    expect(actions.getWorkflowContent().operators[0].operatorType).not.toBe(macroType(parent));
    expect(actions.getWorkflowContent().operators[1].operatorType).toBe(macroType(parent));
    const updated = actions.getWorkflowContent();
    expect(pinned(updated, updated.operators[1]).content).toEqual({ ...parent.content, macroDependencies: undefined });
  });

  it("waits for an in-flight save before discarding a draft and restoring its parent", async () => {
    await editNestedScan("saving");
    const { saving, release } = await startPendingSave();
    const leaving = service.back(0, true);
    await Promise.resolve();
    expect(service.parents).toHaveLength(2);
    release();
    await saving;
    await leaving;
    expect(service.parents).toHaveLength(0);
    expect(actions.getWorkflowMetadata().wid).toBe(1);
    expect(actions.getWorkflowContent()).toEqual(saved.get(1)!.content);
  });

  it("uses the committed root for a subsequent save and preserves a rename queued during it", async () => {
    await openPath("selected", "child");
    const commit = persist.persistMacroPath.getMockImplementation()!;
    // A server-side layout change is part of the saved baseline even while its canvas is suspended.
    persist.persistMacroPath.mockImplementationOnce((steps, before) => {
      const committed = structuredClone(steps);
      committed.at(-1)!.workflow.content.operatorPositions.selected = { x: 123, y: 456 };
      return commit(committed, before);
    });
    editScan("first");
    await service.save();
    const committed = saved.get(root.wid!)!;
    editScan("second");
    const { saving, release } = await startPendingSave();
    expect(persist.persistMacroPath.mock.lastCall![1]).toEqual(committed);
    actions.setWorkflowName("Renamed while saving");
    const queued = service.save();
    release();
    await saving;
    await queued;
    expect(saved.get(20)!.name).toBe("Renamed while saving");
  });

  it("keeps a collaborator's rename and deletion at different levels during a nested save", async () => {
    await editNestedScan("changed");
    const { saving, release } = await startPendingSave();
    const session = service.parents[0].session;
    const intermediate = service.parents[1].session;
    intermediate.shared.operatorIDMap.get("child")!.set("customDisplayName", new Text("Renamed while saving"));
    session.shared.operatorIDMap.delete("selected");
    release();
    await saving;
    expect(actions.getSessionWorkflow(session).content.operators).toEqual([root.content.operators[1]]);
    expect(actions.getSessionWorkflow(intermediate).content.operators[0]).toMatchObject({
      operatorType: "WorkflowMacro:leaf:2",
      customDisplayName: "Renamed while saving",
    });
  });

  it("compiles an edited body in its parent context and maps results and boundary inputs to that instance", async () => {
    const pins = actions.getMacroDependencies();
    await openPath("selected", "child");
    expect(service.compilationDraft()!.drafts).toEqual([]);
    const operator = { ...mockSentimentPredicate, inputPorts: [{ portID: "custom-input" }] };
    actions.addOperatorsAndLinks(
      [{ op: operator, pos: { x: 50, y: 30 } }],
      [testLink(mockScanPredicate.operatorID, operator.operatorID, "output-0", "custom-input", "inside")]
    );
    const leaves = [
      ...scanLeaves(),
      [["selected", "child", operator.operatorID], operator] as [string[], OperatorPredicate],
    ];
    const preview = service.compilationDraft()!;
    expect(preview.drafts).toEqual([{ path: ["selected", "child"], content: actions.getWorkflowContent() }]);
    expect(preview.content.macroDependencies).toEqual(pins);
    const compiled = cacheExpansion(preview.content, leaves, [
      testLink(
        macroRuntimeID(["selected", "child"], mockScanPredicate.operatorID),
        macroRuntimeID(["selected", "child"], operator.operatorID),
        "output-0",
        "custom-input",
        "inside"
      ),
    ]);
    compiled.inspectors[JSON.stringify(["selected", "child"])].inputs[operator.operatorID] = [
      { link: 0, input: 0, output: 0 },
    ];
    const context = service.compilationContext(compiled);
    expect(context.root.content.operators).toHaveLength(5);
    expect(context.inputLinks(operator.operatorID)).toMatchObject([
      {
        source: { operatorID: context.ids[mockScanPredicate.operatorID], portID: "output-0" },
        target: { operatorID: operator.operatorID, portID: "custom-input", portIndex: 0 },
      },
    ]);
    expect(service.resultAliases()[operator.operatorID]).toBeNull();

    expectNoSave();
    await service.save();
    TestBed.inject(MacroExpansionService).remember(service.rootWorkflow.content, compiled);
    status.setRuntimeContent(service.rootWorkflow.content);
    expect(service.resultAliases()[operator.operatorID]).toEqual({
      operatorID: context.ids[operator.operatorID],
      portIndex: 0,
    });
  });
  it("preserves visible native port IDs in standalone macro compilation contexts", () => {
    const source = { ...mockScanPredicate, outputPorts: [{ portID: "output-0" }, { portID: "output-2" }] };
    const target = { ...mockSentimentPredicate, inputPorts: [{ portID: "input-0" }, { portID: "input-2" }] };
    const content = {
      ...leaf.content,
      operators: [...leaf.content.operators.filter(isMacroTerminal), source, target],
      operatorPositions: { ...leaf.content.operatorPositions, [target.operatorID]: { x: 200, y: 30 } },
      links: [testLink(source.operatorID, target.operatorID, "output-2", "input-2", "native-gap")],
    };
    actions.reloadWorkflow({ ...root, content }, false);
    const before = JSON.stringify(actions.getWorkflowContent());
    const compiled = cacheExpansion(
      content,
      [
        [[source.operatorID], source],
        [[target.operatorID], target],
      ],
      content.links
    );
    compiled.inspectors["[]"].inputs[target.operatorID] = [{ link: 0, input: 1, output: 1 }];
    const context = service.compilationContext(compiled);
    expect(context.inputLinks(target.operatorID)).toMatchObject([
      {
        ...content.links[0],
        source: { ...content.links[0].source, portIndex: 1 },
        target: { ...content.links[0].target, portIndex: 1 },
      },
    ]);
    expect(JSON.stringify(actions.getWorkflowContent())).toBe(before);
  });
  it("keeps nested terminal regions and live statistics through layout, Undo/Redo, save and reopening", async () => {
    const sentiment = { ...mockSentimentPredicate, operatorVersion: "Nlp1" };
    leaf = sourceMacro("leaf", [sentiment], {
      inputs: [testPort("rows", sentiment.operatorID, "input-0")],
      outputs: [testPort("result", sentiment.operatorID, "output-0")],
    });
    parent = sourceMacro("parent", [macroInstance(leaf, "child")], {
      macroDependencies: [leaf],
      inputs: [testPort("rows", "child")],
      outputs: [testPort("result", "child")],
    });
    install(
      testWorkflow(
        [mockScanPredicate, macroInstance(parent, "selected"), mockResultPredicate],
        [
          testLink(mockScanPredicate.operatorID, "selected", "output-0", "rows", "in"),
          testLink("selected", mockResultPredicate.operatorID, "result", "input-0", "out"),
        ],
        { macroDependencies: [parent, leaf] }
      )
    );
    const scope = ["selected", "child"];
    const nativeID = macroRuntimeID(scope, sentiment.operatorID);
    const compiled = cacheExpansion(
      root.content,
      [
        [[mockScanPredicate.operatorID], mockScanPredicate],
        [["selected", "child", sentiment.operatorID], sentiment],
        [[mockResultPredicate.operatorID], mockResultPredicate],
      ],
      [testLink(mockScanPredicate.operatorID, nativeID), testLink(nativeID, mockResultPredicate.operatorID)]
    );
    const terminalLinks = [
      { direction: "inputs" as const, portID: "rows", links: [0] },
      { direction: "outputs" as const, portID: "result", links: [1] },
    ];
    const innerView = compiled.inspectors[JSON.stringify(scope)];
    const [input, output] = leaf.content.operators.filter(isMacroTerminal);
    for (const terminal of [input, output])
      innerView.ids[terminal.operatorID] = macroRuntimeID(scope, terminal.operatorID);
    innerView.terminals = terminalLinks;
    compiled.inspectors[JSON.stringify(["selected"])].terminals = terminalLinks;
    const inspect = async (id: string) => {
      await service.open(id);
      service.compilationContext(compiled);
      return actions.runtimeBoundaryLinks!();
    };
    status.setRuntimeContent(root.content);

    const outer = await inspect("selected");
    expect(outer).toEqual(
      terminalLinks.map(({ direction, portID, links }) => ({
        terminal: { direction, portID },
        links: links.map(index => compiled.content.links[index]),
      }))
    );
    expect(await inspect("child")).toEqual(outer);
    expect(actions.getWorkflowContent()).toEqual(leaf.content);
    const checkEditedRegion = () => {
      actions.setOperatorProperty(sentiment.operatorID, { attribute: "edited" });
      expect(actions.runtimeBoundaryLinks!()).toEqual([]);
      undo.undoAction();
      expect(actions.runtimeBoundaryLinks!()).toEqual(outer);
    };
    checkEditedRegion();
    const usage = { ...completedScan, aggregatedOutputRowCount: 3, outputPortMetrics: { "0": 3 } };
    const samples = {
      [nativeID]: usage,
      [innerView.ids[input.operatorID]]: { ...usage, numWorkers: 0 },
      [innerView.ids[output.operatorID]]: {
        ...usage,
        numWorkers: 0,
        inputPortMetrics: { "0": 3 },
      },
      [macroRuntimeID(["other", "child"], output.operatorID)]: { ...usage, inputPortMetrics: { "0": 99 } },
    };
    events.next({ type: "OperatorStatisticsUpdateEvent", operatorStatistics: samples });
    const statistics = status.getCurrentStatistics();
    expect(statistics[mockSentimentPredicate.operatorID].aggregatedOutputRowCount).toBe(3);
    expect(statistics[input.operatorID].outputPortMetrics).toEqual({ "0": 3 });
    expect(statistics[output.operatorID].inputPortMetrics).toEqual({ "0": 3 });
    const aliases = service.resultAliases();
    const expectRuntime = () => {
      expect(actions.runtimeBoundaryLinks!()).toEqual(outer);
      expect(status.getCurrentStatistics()).toEqual(statistics);
      expect(service.resultAliases()).toEqual(aliases);
    };
    actions.autoLayoutWorkflow();
    status.refresh();
    expectRuntime();

    undo.undoAction();
    expectRuntime();
    undo.redoAction();
    expectRuntime();
    await service.save();
    expectRuntime();
    await service.back(0);
    status.clearStatus();
    await recordedExecution(samples, 42);
    await inspect("selected");
    await inspect("child");
    expectRuntime();
    checkEditedRegion();
  });
});

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
import { testMacro } from "../../../../common/testing/macro-graph";
import { testBinding } from "../../../../common/testing/workflow-graph";
import { commonTestImports } from "../../../../common/testing/test-utils";
import { provideRouter } from "@angular/router";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { firstValueFrom, Subject } from "rxjs";
import { MacroVersionComponent } from "./macro-version.component";
import { MacroService } from "../../../service/macro/macro.service";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { MacroDefinition, macroInstance } from "../../../../common/util/workflow-macro";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { macroWorkspaceTestProviders } from "../../../service/macro/macro-workspace.spec-harness";

describe("MacroVersionComponent", () => {
  const definition = testMacro("m", [], {
    formBinding: testBinding("leaf", "text", "kept"),
    version: 3,
    name: "Macro",
    wid: 12,
  });
  const source = { metadata: { ...DEFAULT_WORKFLOW, name: definition.name, wid: 12 }, revisions: [3, 64] };
  let actions: WorkflowActionService, macros: MacroService;
  const error = vi.fn();
  let fixture: ComponentFixture<MacroVersionComponent>, component: MacroVersionComponent;
  const setValues = (properties: Record<string, unknown>) => {
    actions.setOperatorProperty("selected", properties);
    vi.mocked(actions.setOperatorProperty).mockClear();
  };
  const repairButton = () =>
    Array.from<HTMLButtonElement>(fixture.nativeElement.querySelectorAll("button")).find(button =>
      button.textContent?.includes("Remove unavailable values")
    );
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MacroVersionComponent, ...commonTestImports],
      providers: [
        provideRouter([]),
        ...macroWorkspaceTestProviders,
        { provide: NotificationService, useValue: { error } },
      ],
    }).compileComponents();
    error.mockClear();
    macros = TestBed.inject(MacroService);
    vi.spyOn(macros, "sourceFor").mockResolvedValue(source);
    vi.spyOn(macros, "pin").mockResolvedValue({ ...definition, version: 64, revision: 2 });
    vi.spyOn(macros, "adoptVersion");
    actions = TestBed.inject(WorkflowActionService);
    actions.suspendWorkflow().shared.wsProvider.disconnect();
    actions.setMacroDependencies([definition]);
    actions.addOperator(macroInstance(definition, "selected"), { x: 0, y: 0 });
    actions.addOperator(macroInstance(definition, "sibling"), { x: 100, y: 0 });
    vi.spyOn(actions, "setOperatorProperty");
    fixture = TestBed.createComponent(MacroVersionComponent);
    component = fixture.componentInstance;
    component.operatorID = "selected";
  });
  afterEach(() => actions.destroySharedModel());

  it("shows history ordinals and explicitly switches only the selected usage", async () => {
    await component.ngOnChanges();
    fixture.detectChanges();
    expect(component.versions).toEqual([
      { id: 3, label: "Revision 1" },
      { id: 64, label: "Revision 2" },
    ]);
    expect(fixture.nativeElement.textContent).toContain("A newer revision is available");
    expect(fixture.nativeElement.textContent).not.toContain("v64");
    component.disabled = true;
    fixture.detectChanges();
    await component.use(3);
    expect(macros.adoptVersion).not.toHaveBeenCalled();
    expect(fixture.nativeElement.querySelector("button:last-child").disabled).toBe(true);
    component.disabled = false;
    await component.use(64);
    expect(macros.adoptVersion).toHaveBeenCalledExactlyOnceWith("selected", expect.objectContaining({ version: 64 }));
    expect(actions.getTexeraGraph().getOperator("selected").operatorType).toBe("WorkflowMacro:m:64");
    expect(actions.getTexeraGraph().getOperator("sibling").operatorType).toBe("WorkflowMacro:m:3");
  });

  it("keeps portable snapshots viewable when a foreign source id belongs to another definition", async () => {
    vi.mocked(macros.sourceFor).mockRejectedValueOnce(new Error("Macro source identity differs"));
    await component.ngOnChanges();
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Source unavailable");
    expect(macros.pin).not.toHaveBeenCalled();
    await component.use(64);
    expect(macros.adoptVersion).not.toHaveBeenCalled();
  });

  it("does not apply an in-flight revision to another selected operator", async () => {
    await component.ngOnChanges();
    fixture.detectChanges();
    const pin = new Subject<MacroDefinition>();
    vi.mocked(macros.pin).mockReturnValue(firstValueFrom(pin));
    const using = component.use(64);
    component.operatorID = "another";
    pin.next({ ...definition, version: 64 });
    await using;
    expect(macros.adoptVersion).not.toHaveBeenCalled();
    expect(error).toHaveBeenCalledWith(expect.stringMatching(/changed/i));
  });

  it.each(["history response", "source failure"])("ignores a late %s after selection is cleared", async kind => {
    const response = new Subject<typeof source>();
    vi.mocked(macros.sourceFor).mockReturnValueOnce(firstValueFrom(response));
    const loading = component.ngOnChanges();
    component.operatorID = undefined;
    await component.ngOnChanges();
    if (kind === "history response") response.next(source);
    else response.error(new Error("source unavailable"));
    await loading;
    fixture.detectChanges();

    expect(component.versions).toEqual([]);
    expect(component.source).toBeUndefined();
    expect(component.latest).toBeUndefined();
    expect(component.busy).toBe(false);
    expect(component.unavailable).toBe(false);
  });

  it.each([
    { before: { kept: { text: "東京" }, removed: null }, after: { kept: { text: "東京" } } },
    { before: { removed: "東京" }, after: {} },
  ])("repairs unavailable values and restores the saved graph with Undo/Redo (%j)", ({ before, after }) => {
    const undo = TestBed.inject(UndoRedoService);
    setValues(before);
    undo.clearUndoStack();
    const original = structuredClone(actions.getWorkflowContent());
    fixture.detectChanges();
    repairButton()!.click();
    fixture.detectChanges();
    expect(actions.setOperatorProperty).toHaveBeenCalledWith("selected", after);
    expect(repairButton()).toBeUndefined();
    const repaired = actions.getWorkflowContent();
    expect(repaired.operators[0].operatorProperties).toEqual(after);
    expect(repaired.macroDependencies).toEqual(original.macroDependencies);
    undo.undoAction();
    expect(actions.getWorkflowContent()).toEqual(original);
    undo.redoAction();
    expect(actions.getWorkflowContent()).toEqual(repaired);
  });

  it.each(["disabled", "busy"] as const)("keeps unavailable values while %s", guard => {
    setValues({ removed: "preserve" });
    component[guard] = true;
    fixture.detectChanges();
    expect(repairButton()?.disabled).toBe(true);
    component.removeUnavailableValues();
    expect(actions.setOperatorProperty).not.toHaveBeenCalled();
  });

  it("does not offer or perform repair without unavailable values or a selected macro", () => {
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).not.toContain("Remove unavailable values");
    component.removeUnavailableValues();
    component.operatorID = "missing";
    fixture.detectChanges();
    component.removeUnavailableValues();
    expect(actions.setOperatorProperty).not.toHaveBeenCalled();
  });
});

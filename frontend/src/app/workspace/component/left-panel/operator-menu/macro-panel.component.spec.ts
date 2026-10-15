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
import { TestBed } from "@angular/core/testing";
import { BehaviorSubject, EMPTY } from "rxjs";
import { MacroPanelComponent } from "./macro-panel.component";
import { MacroService } from "../../../service/macro/macro.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { FormBindingService } from "../../../service/form-binding/form-binding.service";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";

describe("MacroPanelComponent", () => {
  const boundary = { id: "macro", inputs: [], outputs: [] };
  let actions: any, service: any, notification: any;
  beforeEach(() => {
    actions = {
      getMacroInterface: () => boundary,
      checkWorkflowModificationEnabled: () => true,
      getFormBinding: () => ({ fields: [] }),
      setFormBinding: vi.fn(),
      getTexeraGraph: () => ({
        hasOperator: (id: string) => id === "leaf",
        getOperatorAddStream: () => EMPTY,
        getOperatorDeleteStream: () => EMPTY,
        getOperator: () => ({ operatorType: "Filter" }),
        getAllOperators: () => [
          {
            operatorID: "leaf",
            customDisplayName: "Filter",
            inputPorts: [{ portID: "input-0" }],
            outputPorts: [{ portID: "output-0" }],
          },
        ],
      }),
      getJointGraphWrapper: () => ({ getCurrentHighlightedOperatorIDs: () => ["leaf"] }),
      setMacroInterface: vi.fn(),
    };
    service = {
      library$: new BehaviorSubject([]),
      inspect$: new BehaviorSubject(undefined),
      refresh: vi.fn(async () => []),
      createFromSelection: vi.fn(async () => ({})),
      adoptLatest: vi.fn(async () => {}),
    };
    notification = { error: vi.fn(), success: vi.fn() };
    TestBed.configureTestingModule({
      imports: [MacroPanelComponent],
      providers: [
        FormBindingService,
        DynamicSchemaService,
        { provide: WorkflowActionService, useValue: actions },
        { provide: MacroService, useValue: service },
        {
          provide: OperatorMetadataService,
          useValue: {
            getOperatorSchema: vi.fn(() => ({
              jsonSchema: { properties: { value: { type: "string", title: "Value" } } },
            })),
          },
        },
        { provide: NotificationService, useValue: notification },
        { provide: NzModalService, useValue: { create: vi.fn() } },
      ],
    });
  });
  it("shows the definition controls and maps chosen leaf ports explicitly", () => {
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    const select = fixture.nativeElement.querySelector('select[aria-label="Input target"]') as HTMLSelectElement;
    select.value = select.options[1].value;
    select.dispatchEvent(new Event("change"));
    fixture.detectChanges();
    (fixture.nativeElement.querySelector('[data-action="add-input"]') as HTMLButtonElement).click();
    expect(actions.setMacroInterface).toHaveBeenCalledWith(
      expect.objectContaining({
        inputs: [
          expect.objectContaining({
            targets: [{ operatorID: "leaf", portID: "input-0" }],
          }),
        ],
      })
    );
  });
  it("disables editing controls for a read-only macro and reports save failures", async () => {
    actions.checkWorkflowModificationEnabled = () => false;
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('[data-action="create"]').disabled).toBe(true);
    expect(fixture.nativeElement.querySelector('[data-action="add-input"]').disabled).toBe(true);
    actions.checkWorkflowModificationEnabled = () => true;
    service.createFromSelection.mockRejectedValue(new Error("save failed"));
    fixture.componentInstance.name = "Clean";
    await fixture.componentInstance.create();
    expect(notification.error).toHaveBeenCalledWith("save failed");
    expect(fixture.componentInstance.busy).toBe(false);
  });

  it("exposes a chosen leaf property without requiring Form View to be enabled", () => {
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    const select = fixture.nativeElement.querySelector('select[aria-label="Property target"]') as HTMLSelectElement;
    select.value = select.options[1].value;
    select.dispatchEvent(new Event("change"));
    fixture.detectChanges();
    fixture.nativeElement.querySelector('[data-action="add-property"]').click();
    expect(actions.setFormBinding).toHaveBeenCalledWith({
      fields: [
        expect.objectContaining({
          operatorID: "leaf",
          propertyKey: "value",
          displayName: "Value",
        }),
      ],
    });
  });

  it("explains empty selection and read-only state without hiding the library", () => {
    actions.getJointGraphWrapper = () => ({ getCurrentHighlightedOperatorIDs: () => [] });
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("No saved macros yet");
    expect(fixture.nativeElement.textContent).toContain("Select operators on the canvas");
    expect(fixture.nativeElement.querySelector('[data-action="create"]').disabled).toBe(true);
    actions.checkWorkflowModificationEnabled = () => false;
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Read-only workflow");
    expect(fixture.nativeElement.querySelector("summary").textContent).toContain("Macros");
  });

  it("submits a named selection from the keyboard and clears the name only after success", async () => {
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.componentInstance.name = "清理 · café";
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("1 operator selected");
    fixture.nativeElement.querySelector("form").dispatchEvent(new Event("submit", { cancelable: true }));
    await fixture.whenStable();
    fixture.detectChanges();
    expect(service.createFromSelection).toHaveBeenCalledWith("清理 · café");
    expect(fixture.componentInstance.name).toBe("");
    expect(notification.success).toHaveBeenCalledWith("Macro created");
    service.createFromSelection.mockRejectedValue(new Error("save failed"));
    fixture.componentInstance.name = "Try again";
    await fixture.componentInstance.create();
    expect(fixture.componentInstance.name).toBe("Try again");
  });

  it("keeps a port name focused when shared metadata is reread", () => {
    actions.getMacroInterface = () =>
      structuredClone({
        ...boundary,
        inputs: [{ portID: "input-0", displayName: "Rows", targets: [{ operatorID: "leaf", portID: "input-0" }] }],
      });
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    const input = fixture.nativeElement.querySelector('[aria-label="Port name input-0"]') as HTMLInputElement;
    input.focus();
    fixture.detectChanges();
    expect(document.activeElement).toBe(input);
  });

  it("labels imported targets independently of JSON property ordering", () => {
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    expect(fixture.componentInstance.targetLabel("inputs", { portID: "input-0", operatorID: "leaf" })).toBe(
      "Filter / input-0"
    );
  });

  it("ties inspector runtime context to the selected workflow instance and pinned version", async () => {
    let wid = 1;
    const definition = { id: "macro", version: 1, name: "Macro" };
    const op = { operatorID: "instance", operatorType: "WorkflowMacro:macro:1" };
    const content = { operators: [op], macroDependencies: [definition] };
    const expanded = { paths: {}, ports: {} };
    actions.getWorkflowContent = () => content;
    actions.getWorkflowMetadata = () => ({ wid });
    actions.getMacroExpansion = () => expanded;
    const fixture = TestBed.createComponent(MacroPanelComponent);
    fixture.detectChanges();
    const create = vi.spyOn((fixture.componentInstance as any).modal, "create").mockReturnValue({});
    await fixture.componentInstance.inspect("instance");
    const data = (create.mock.calls[0][0] as any).nzData;
    expect(data.runtime.operatorID).toBe("instance");
    expect(data.runtime.getExpansion()).toBe(expanded);
    wid = 2;
    expect(data.runtime.getExpansion()).toBeUndefined();
    wid = 1;
    op.operatorType = "WorkflowMacro:macro:2";
    expect(data.runtime.getExpansion()).toBeUndefined();
    content.operators = [];
    expect(data.runtime.getExpansion()).toBeUndefined();
    fixture.destroy();
  });
});

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

import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";

import { PortPropertyEditFrameComponent } from "./port-property-edit-frame.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { DynamicSchemaService } from "../../../service/dynamic-schema/dynamic-schema.service";
import { FormGroup } from "@angular/forms";
import { LogicalPort, PortDescription } from "../../../types/workflow-common.interface";
import { mockPortSchema } from "../../../service/operator-metadata/mock-operator-metadata.data";
import { FORM_DEBOUNCE_TIME_MS } from "../../../service/execute-workflow/execute-workflow.service";

describe("PortPropertyEditFrameComponent", () => {
  let component: PortPropertyEditFrameComponent;
  let fixture: ComponentFixture<PortPropertyEditFrameComponent>;
  let workflowActionService: WorkflowActionService;
  let dynamicSchemaService: DynamicSchemaService;
  // concrete texera graph instance (exposes the raw subjects the component subscribes to)
  let texeraGraph: any;

  const inputPort: LogicalPort = { operatorID: "op-1", portID: "input-0" };
  const outputPort: LogicalPort = { operatorID: "op-1", portID: "output-0" };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [WorkflowActionService, ...commonTestProviders],
      imports: [PortPropertyEditFrameComponent, HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PortPropertyEditFrameComponent);
    component = fixture.componentInstance;
    workflowActionService = TestBed.inject(WorkflowActionService);
    dynamicSchemaService = TestBed.inject(DynamicSchemaService);
    texeraGraph = workflowActionService.getTexeraGraph();
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    fixture.destroy();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("ngOnInit / handler registration", () => {
    it("should register all three change handlers", () => {
      const propertySpy = vi.spyOn(component as any, "registerPortPropertyChangeHandler");
      const displayNameSpy = vi.spyOn(component as any, "registerPortDisplayNameChangeHandler");
      const formChangeSpy = vi.spyOn(component as any, "registerOnFormChangeHandler");

      component.ngOnInit();

      expect(propertySpy).toHaveBeenCalledTimes(1);
      expect(displayNameSpy).toHaveBeenCalledTimes(1);
      expect(formChangeSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("onFormChanges", () => {
    it("should forward the raw form event onto the source form-change stream", () => {
      const received: Record<string, unknown>[] = [];
      (component as any).sourceFormChangeEventStream.subscribe((e: Record<string, unknown>) => received.push(e));

      const event = { type: "hash", hashAttributeNames: ["a"] };
      component.onFormChanges(event);

      expect(received).toEqual([event]);
    });
  });

  describe("ngOnChanges", () => {
    it("should adopt the new port id and open the editor when a port is provided", () => {
      const showSpy = vi.spyOn(component as any, "showPortPropertyEditor").mockImplementation(() => {});

      component.ngOnChanges({ currentPortID: { currentValue: inputPort } } as any);

      expect(component.currentPortID).toEqual(inputPort);
      expect(showSpy).toHaveBeenCalledWith(inputPort);
    });

    it("should not open the editor when the new port id is undefined", () => {
      const showSpy = vi.spyOn(component as any, "showPortPropertyEditor").mockImplementation(() => {});

      component.ngOnChanges({ currentPortID: { currentValue: undefined } } as any);

      expect(component.currentPortID).toBeUndefined();
      expect(showSpy).not.toHaveBeenCalled();
    });
  });

  describe("showPortPropertyEditor", () => {
    it("should throw when the target port does not exist in the graph", () => {
      expect(() => (component as any).showPortPropertyEditor({ operatorID: "ghost", portID: "input-0" })).toThrowError(
        /does not exist/
      );
    });

    it("should set the title but skip building the form when customization is not allowed", () => {
      const descriptor: PortDescription = {
        portID: "input-0",
        displayName: "Input A",
        partitionRequirement: { type: "none" },
        dependencies: [],
      };
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue(descriptor);
      vi.spyOn(dynamicSchemaService, "getDynamicSchema").mockReturnValue({
        additionalMetadata: { allowPortCustomization: false },
      } as any);

      (component as any).showPortPropertyEditor(inputPort);

      expect(component.currentPortID).toEqual(inputPort);
      expect(component.formTitle).toBe("Input A");
      expect(component.formlyFields).toBeUndefined();
      expect(component.formlyFormGroup).toBeUndefined();
    });

    it("should skip building the form for an output port even when customization is allowed", () => {
      const descriptor: PortDescription = { portID: "output-0", displayName: "Output A" };
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue(descriptor);
      vi.spyOn(dynamicSchemaService, "getDynamicSchema").mockReturnValue({
        additionalMetadata: { allowPortCustomization: true },
      } as any);

      (component as any).showPortPropertyEditor(outputPort);

      expect(component.formTitle).toBe("Output A");
      expect(component.formlyFields).toBeUndefined();
    });

    it("should build the formly form from the port descriptor when customization is allowed on an input port", () => {
      const descriptor: PortDescription = {
        portID: "input-0",
        displayName: "Input A",
        partitionRequirement: { type: "hash", hashAttributeNames: ["a"] },
        dependencies: [{ id: 1, internal: false }],
      };
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue(descriptor);
      vi.spyOn(dynamicSchemaService, "getDynamicSchema").mockReturnValue({
        additionalMetadata: { allowPortCustomization: true },
      } as any);

      (component as any).showPortPropertyEditor(inputPort);

      expect(component.formTitle).toBe("Input A");
      expect(component.formData).toEqual({
        partitionInfo: { type: "hash", hashAttributeNames: ["a"] },
        dependencies: [{ id: 1, internal: false }],
      });
      expect(component.formlyFormGroup).toBeInstanceOf(FormGroup);
      expect(Array.isArray(component.formlyFields)).toBe(true);
    });
  });

  describe("setFormlyFormBinding (interactive vs. display mode)", () => {
    it("should keep the form enabled when interactive, and disable it via the onInit hook when not", () => {
      const fakeField: any = { fieldGroup: [{ key: "partitionInfo" }] };
      vi.spyOn((component as any).formlyJsonschema, "toFieldConfig").mockReturnValue(fakeField);

      (component as any).setFormlyFormBinding(mockPortSchema.jsonSchema);

      expect(component.formlyFormGroup).toBeInstanceOf(FormGroup);
      expect(component.formlyFields).toBe(fakeField.fieldGroup);

      const onInit = fakeField.hooks.onInit;
      const fakeForm = { disable: vi.fn() };

      component.interactive = true;
      onInit({ form: fakeForm });
      expect(fakeForm.disable).not.toHaveBeenCalled();

      component.interactive = false;
      onInit({ form: fakeForm });
      expect(fakeForm.disable).toHaveBeenCalledTimes(1);

      // guard: a missing field config / form must not throw
      expect(() => onInit(undefined)).not.toThrow();
    });
  });

  describe("checkPort", () => {
    it("should return false when there is no current port", () => {
      component.currentPortID = undefined;
      expect((component as any).checkPort({ type: "none" })).toBe(false);
    });

    it("should return false when the current port is no longer in the graph", () => {
      component.currentPortID = inputPort;
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(false);
      expect((component as any).checkPort({ type: "none" })).toBe(false);
    });

    it("should compare form data against the port's partition requirement", () => {
      component.currentPortID = inputPort;
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue({
        portID: "input-0",
        partitionRequirement: { type: "none" },
      } as PortDescription);

      // identical to the current partition requirement -> no change
      expect((component as any).checkPort({ type: "none" })).toBe(false);
      // different -> change detected
      expect((component as any).checkPort({ type: "hash", hashAttributeNames: ["a"] })).toBe(true);
    });
  });

  describe("registerOnFormChangeHandler (edit propagation)", () => {
    it("should push a debounced, changed form value to WorkflowActionService.setPortProperty", fakeAsync(() => {
      component.currentPortID = inputPort;
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue({ portID: "input-0" } as PortDescription);
      const setSpy = vi.spyOn(workflowActionService, "setPortProperty").mockImplementation(() => {});

      const newValue = { type: "hash", hashAttributeNames: ["a"] };
      component.onFormChanges(newValue);
      tick(FORM_DEBOUNCE_TIME_MS + 10);

      expect(setSpy).toHaveBeenCalledTimes(1);
      expect(setSpy).toHaveBeenCalledWith(inputPort, newValue);
      // the setter passes a clone, not the same reference
      expect(setSpy.mock.calls[0][1]).not.toBe(newValue);
      expect(component.listeningToChange).toBe(true);
    }));

    it("should not propagate when the form value matches the current partition requirement", fakeAsync(() => {
      component.currentPortID = inputPort;
      vi.spyOn(texeraGraph, "hasPort").mockReturnValue(true);
      vi.spyOn(texeraGraph, "getPortDescription").mockReturnValue({
        portID: "input-0",
        partitionRequirement: { type: "none" },
      } as PortDescription);
      const setSpy = vi.spyOn(workflowActionService, "setPortProperty").mockImplementation(() => {});

      component.onFormChanges({ type: "none" });
      tick(FORM_DEBOUNCE_TIME_MS + 10);

      expect(setSpy).not.toHaveBeenCalled();
    }));
  });

  describe("registerPortPropertyChangeHandler (programmatic updates)", () => {
    it("should adopt the new property when the change targets the current port", () => {
      component.currentPortID = inputPort;
      component.formData = { type: "none" };

      const newProperty = { partitionInfo: { type: "single" }, dependencies: [] };
      texeraGraph.portPropertyChangedSubject.next({ operatorPortID: inputPort, newProperty });

      expect(component.formData).toEqual(newProperty);
      // stored as a clone
      expect(component.formData).not.toBe(newProperty);
    });

    it("should ignore programmatic changes while not listening", () => {
      component.currentPortID = inputPort;
      component.formData = { type: "none" };
      component.listeningToChange = false;

      texeraGraph.portPropertyChangedSubject.next({
        operatorPortID: inputPort,
        newProperty: { partitionInfo: { type: "single" } },
      });

      expect(component.formData).toEqual({ type: "none" });
    });

    it("should ignore changes targeting a different port", () => {
      component.currentPortID = inputPort;
      component.formData = { type: "none" };

      texeraGraph.portPropertyChangedSubject.next({
        operatorPortID: outputPort,
        newProperty: { partitionInfo: { type: "single" } },
      });

      expect(component.formData).toEqual({ type: "none" });
    });

    it("should ignore a change equal to the currently held form data", () => {
      component.currentPortID = inputPort;
      const current = { partitionInfo: { type: "single" }, dependencies: [] };
      component.formData = current;

      texeraGraph.portPropertyChangedSubject.next({
        operatorPortID: inputPort,
        newProperty: { partitionInfo: { type: "single" }, dependencies: [] },
      });

      // filtered out because it equals the current form data -> same reference kept
      expect(component.formData).toBe(current);
    });
  });

  describe("registerPortDisplayNameChangeHandler", () => {
    it("should update the form title when the display-name change matches the current port", () => {
      component.currentPortID = inputPort;
      component.formTitle = "old";

      texeraGraph.portDisplayNameChangedSubject.next({
        operatorID: inputPort.operatorID,
        portID: inputPort.portID,
        newDisplayName: "renamed",
      });

      expect(component.formTitle).toBe("renamed");
    });

    it("should leave the form title unchanged for a different port", () => {
      component.currentPortID = inputPort;
      component.formTitle = "old";

      texeraGraph.portDisplayNameChangedSubject.next({
        operatorID: "other-op",
        portID: inputPort.portID,
        newDisplayName: "renamed",
      });

      expect(component.formTitle).toBe("old");
    });
  });

  describe("quill title editing", () => {
    it("should not create a binding when there is no current port", () => {
      const bindingSpy = vi.spyOn(component as any, "registerQuillBinding").mockImplementation(() => {});
      component.currentPortID = undefined;

      component.connectQuillToText();

      expect(bindingSpy).toHaveBeenCalledTimes(1);
      expect(component.quillBinding).toBeUndefined();
    });

    it("should not create a binding when the shared port descriptor is unavailable", () => {
      vi.spyOn(component as any, "registerQuillBinding").mockImplementation(() => {});
      component.currentPortID = inputPort;
      vi.spyOn(texeraGraph, "getSharedPortDescriptionType").mockReturnValue(undefined);

      component.connectQuillToText();

      expect(component.quillBinding).toBeUndefined();
    });

    it("should blur, drop the binding and exit editing on disconnect", () => {
      const blur = vi.fn();
      component.quill = { blur } as any;
      component.quillBinding = {} as any;
      component.editingTitle = true;

      component.disconnectQuillFromText();

      expect(blur).toHaveBeenCalledTimes(1);
      expect(component.quillBinding).toBeUndefined();
      expect(component.editingTitle).toBe(false);
    });
  });
});

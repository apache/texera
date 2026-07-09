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

import { ApplicationRef, DebugElement, getDebugNode, SimpleChange } from "@angular/core";
import { NgModel } from "@angular/forms";
import { CdkVirtualScrollViewport } from "@angular/cdk/scrolling";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, throwError } from "rxjs";
import type { Mocked } from "vitest";
import { ComputingUnitCreateModalComponent } from "./computing-unit-create-modal.component";
import { WorkflowComputingUnitManagingService } from "../../service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { NotificationService } from "../../service/notification/notification.service";
import { DashboardWorkflowComputingUnit, WorkflowComputingUnitType } from "../../type/workflow-computing-unit";
import { commonTestProviders } from "../../testing/test-utils";
import { buildLocalComputingUnitUri, getJvmMemorySliderConfig } from "../../util/computing-unit.util";

describe("ComputingUnitCreateModalComponent", () => {
  let component: ComputingUnitCreateModalComponent;
  let fixture: ComponentFixture<ComputingUnitCreateModalComponent>;
  let mockComputingUnitService: Mocked<WorkflowComputingUnitManagingService>;
  let mockNotificationService: Mocked<NotificationService>;

  const createdUnit = { computingUnit: { cuid: 42 } } as unknown as DashboardWorkflowComputingUnit;

  beforeEach(async () => {
    mockComputingUnitService = {
      getComputingUnitTypes: vi.fn(),
      getComputingUnitLimitOptions: vi.fn(),
      createKubernetesBasedComputingUnit: vi.fn(),
      createLocalComputingUnit: vi.fn(),
    } as unknown as Mocked<WorkflowComputingUnitManagingService>;
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(of({ typeOptions: [] }));
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
      of({ cpuLimitOptions: [], memoryLimitOptions: [], gpuLimitOptions: [] })
    );
    mockComputingUnitService.createKubernetesBasedComputingUnit.mockReturnValue(of(createdUnit));
    mockComputingUnitService.createLocalComputingUnit.mockReturnValue(of(createdUnit));

    mockNotificationService = {
      success: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    } as unknown as Mocked<NotificationService>;

    await TestBed.configureTestingModule({
      providers: [
        // The real NzModalService is required here: the declarative <nz-modal>
        // in this component's template delegates opening to NzModalService.create(),
        // so a stub breaks every test that renders the modal open.
        NzModalService,
        { provide: WorkflowComputingUnitManagingService, useValue: mockComputingUnitService },
        { provide: NotificationService, useValue: mockNotificationService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
      imports: [ComputingUnitCreateModalComponent, HttpClientTestingModule, NoopAnimationsModule],
    }).compileComponents();

    fixture = TestBed.createComponent(ComputingUnitCreateModalComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("prefers the kubernetes type when available", () => {
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
      of({ typeOptions: ["local", "kubernetes"] as WorkflowComputingUnitType[] })
    );
    fixture.detectChanges();
    expect(component.selectedComputingUnitType).toBe("kubernetes");
  });

  it("falls back to the first available type when kubernetes is absent", () => {
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
      of({ typeOptions: ["local"] as WorkflowComputingUnitType[] })
    );
    fixture.detectChanges();
    expect(component.selectedComputingUnitType).toBe("local");
  });

  it("applies fetched limit options with first-option defaults", () => {
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
      of({ cpuLimitOptions: ["2", "4"], memoryLimitOptions: ["2Gi", "4Gi"], gpuLimitOptions: ["0", "1"] })
    );
    fixture.detectChanges();
    expect(component.cpuOptions).toEqual(["2", "4"]);
    expect(component.selectedCpu).toBe("2");
    expect(component.selectedMemory).toBe("2Gi");
    expect(component.selectedGpu).toBe("0");
    expect(component.showGpuSelection()).toBe(true);
  });

  it("falls back to hardcoded defaults when option lists are empty", () => {
    fixture.detectChanges();
    expect(component.selectedCpu).toBe("1");
    expect(component.selectedMemory).toBe("1Gi");
    expect(component.selectedGpu).toBe("0");
    expect(component.showGpuSelection()).toBe(false);
  });

  it("rejects a kubernetes create with an empty name but still closes the modal on Ok", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "kubernetes";
    component.newComputingUnitName = "   ";
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);

    component.handleAddComputeUnitModalOk();

    expect(mockNotificationService.error).toHaveBeenCalledWith("Name of the computing unit cannot be empty");
    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).not.toHaveBeenCalled();
    expect(visibleSpy).toHaveBeenCalledWith(false);
  });

  it("rejects a local create with a blank URI", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "local";
    component.newComputingUnitName = "My Local Unit";
    component.localComputingUnitUri = "   ";

    component.startComputingUnit();

    expect(mockNotificationService.error).toHaveBeenCalledWith("URI for local computing unit cannot be empty");
    expect(mockComputingUnitService.createLocalComputingUnit).not.toHaveBeenCalled();
  });

  it("rejects a create without a selected type", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = undefined;

    component.startComputingUnit();

    expect(mockNotificationService.error).toHaveBeenCalledWith("Please select a valid computing unit type");
    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).not.toHaveBeenCalled();
    expect(mockComputingUnitService.createLocalComputingUnit).not.toHaveBeenCalled();
  });

  it("emits unitCreated and a success toast on a successful kubernetes create", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "kubernetes";
    component.newComputingUnitName = "GPU Test Unit";
    component.selectedCpu = "2";
    component.selectedMemory = "4Gi";
    component.selectedGpu = "0";
    component.selectedJvmMemorySize = "2G";
    component.shmSizeValue = 128;
    component.shmSizeUnit = "Mi";
    const unitCreatedSpy = vi.fn();
    component.unitCreated.subscribe(unitCreatedSpy);

    component.startComputingUnit();

    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).toHaveBeenCalledWith(
      "GPU Test Unit",
      "2",
      "4Gi",
      "0",
      "2G",
      "128Mi"
    );
    expect(mockNotificationService.success).toHaveBeenCalledWith("Successfully created the new compute unit");
    expect(unitCreatedSpy).toHaveBeenCalledWith(createdUnit);
  });

  it("closes without creating on Cancel", () => {
    fixture.detectChanges();
    const visibleSpy = vi.fn();
    component.visibleChange.subscribe(visibleSpy);

    component.handleAddComputeUnitModalCancel();

    expect(visibleSpy).toHaveBeenCalledWith(false);
    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).not.toHaveBeenCalled();
    expect(mockComputingUnitService.createLocalComputingUnit).not.toHaveBeenCalled();
  });

  it("reconfigures the JVM memory slider when the memory selection changes", () => {
    fixture.detectChanges();
    component.selectedMemory = "4Gi";
    component.onMemorySelectionChange();
    const expected = getJvmMemorySliderConfig("4Gi");
    expect(component.jvmMemoryMax).toBe(expected.jvmMemoryMax);
    expect(component.showJvmMemorySlider).toBe(expected.showJvmMemorySlider);
    expect(component.jvmMemorySteps).toEqual(expected.jvmMemorySteps);
  });

  it("flags shared memory larger than total memory", () => {
    fixture.detectChanges();
    component.selectedMemory = "1Gi";
    component.shmSizeValue = 2;
    component.shmSizeUnit = "Gi";
    expect(component.isShmTooLarge()).toBe(true);

    component.shmSizeValue = 64;
    component.shmSizeUnit = "Mi";
    expect(component.isShmTooLarge()).toBe(false);
  });

  it("toggles the advanced-settings panel", () => {
    component.showAdvancedSettings = false;
    component.toggleAdvancedSettings();
    expect(component.showAdvancedSettings).toBe(true);
    component.toggleAdvancedSettings();
    expect(component.showAdvancedSettings).toBe(false);
  });

  it("re-collapses the advanced-settings panel each time the modal opens", () => {
    component.showAdvancedSettings = true;
    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });
    expect(component.showAdvancedSettings).toBe(false);

    // Closing the modal must not touch the panel state.
    component.showAdvancedSettings = true;
    component.ngOnChanges({ visible: new SimpleChange(true, false, false) });
    expect(component.showAdvancedSettings).toBe(true);
  });

  it("resets the advanced values each time the modal opens", () => {
    fixture.detectChanges();
    component.selectedMemory = "8Gi";
    component.onMemorySelectionChange();
    component.shmSizeValue = 4;
    component.shmSizeUnit = "Gi";
    component.onJvmMemorySliderChange(component.jvmMemoryMax);

    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });

    const expected = getJvmMemorySliderConfig("8Gi");
    expect(component.shmSizeValue).toBe(64);
    expect(component.shmSizeUnit).toBe("Mi");
    expect(component.jvmMemorySliderValue).toBe(expected.jvmMemorySliderValue);
    expect(component.selectedJvmMemorySize).toBe(expected.selectedJvmMemorySize);

    // Closing the modal must not touch the values.
    component.shmSizeValue = 2;
    component.shmSizeUnit = "Gi";
    component.ngOnChanges({ visible: new SimpleChange(true, false, false) });
    expect(component.shmSizeValue).toBe(2);
    expect(component.shmSizeUnit).toBe("Gi");
  });

  it("surfaces the max-JVM warning on the collapsed header", () => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(false);
    vi.spyOn(component, "isMaxJvmMemorySelected").mockReturnValue(true);

    component.showAdvancedSettings = false;
    expect(component.hasCollapsedWarning()).toBe(true);
    expect(component.collapsedWarningText()).toBe("JVM memory at maximum");

    // Expanded: the inline nz-alert is visible, so the header stays quiet.
    component.showAdvancedSettings = true;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("prefers the shm warning over the JVM warning when both apply", () => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(true);
    vi.spyOn(component, "isMaxJvmMemorySelected").mockReturnValue(true);

    component.selectedMemory = "4Gi";
    component.showAdvancedSettings = false;
    expect(component.collapsedWarningText()).toBe("Shared memory exceeds total");
  });

  it("flags a collapsed warning only when collapsed and shm exceeds memory", () => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(true);
    component.selectedMemory = "4Gi";
    component.showAdvancedSettings = false;
    expect(component.hasCollapsedWarning()).toBe(true);
    // Expanded: the inline warning is visible, so the header stays quiet.
    component.showAdvancedSettings = true;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("suppresses the shm warning until the memory options have loaded", () => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(true);
    component.selectedMemory = "";
    component.showAdvancedSettings = false;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("does not flag a warning when shm fits within memory", () => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(false);
    component.showAdvancedSettings = false;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("initializes the local computing unit URI from the window location", () => {
    fixture.detectChanges();
    expect(component.localComputingUnitUri).toBe(buildLocalComputingUnitUri(window.location));
  });

  it("builds the local URI with and without a port", () => {
    expect(buildLocalComputingUnitUri({ protocol: "http:", hostname: "localhost", port: "8080" })).toBe(
      "http://localhost:8080/wsapi"
    );
    expect(buildLocalComputingUnitUri({ protocol: "https:", hostname: "texera.io", port: "" })).toBe(
      "https://texera.io/wsapi"
    );
  });

  it("toasts when fetching computing unit types fails", () => {
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(throwError(() => new Error("boom")));
    fixture.detectChanges();
    expect(mockNotificationService.error).toHaveBeenCalledWith(
      expect.stringContaining("Failed to fetch computing unit types")
    );
  });

  it("toasts when fetching resource limit options fails", () => {
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(throwError(() => new Error("boom")));
    fixture.detectChanges();
    expect(mockNotificationService.error).toHaveBeenCalledWith(
      expect.stringContaining("Failed to fetch resource options")
    );
  });

  it("toasts when the create request fails", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "kubernetes";
    component.newComputingUnitName = "Unit";
    mockComputingUnitService.createKubernetesBasedComputingUnit.mockReturnValue(throwError(() => new Error("boom")));
    const unitCreatedSpy = vi.fn();
    component.unitCreated.subscribe(unitCreatedSpy);

    component.startComputingUnit();

    expect(mockNotificationService.error).toHaveBeenCalledWith(
      expect.stringContaining("Failed to start computing unit")
    );
    expect(unitCreatedSpy).not.toHaveBeenCalled();
  });

  it("shows GPU selection for a single non-zero option and hides it for a single zero option", () => {
    fixture.detectChanges();
    component.gpuOptions = ["1"];
    expect(component.showGpuSelection()).toBe(true);
    component.gpuOptions = ["0"];
    expect(component.showGpuSelection()).toBe(false);
  });

  it("snaps the JVM memory slider to the nearest valid step", () => {
    fixture.detectChanges();
    component.selectedMemory = "8Gi";
    component.onMemorySelectionChange();

    component.onJvmMemorySliderChange(component.jvmMemoryMax - 1);

    expect(component.jvmMemorySteps).toContain(component.jvmMemorySteps[component.jvmMemorySliderValue]);
    expect(component.selectedJvmMemorySize).toBe(`${component.jvmMemorySteps[component.jvmMemorySliderValue]}G`);
  });

  it("flags max JVM memory only when the slider is shown at its maximum", () => {
    fixture.detectChanges();
    component.selectedMemory = "8Gi";
    component.onMemorySelectionChange();

    component.onJvmMemorySliderChange(component.jvmMemoryMax);
    expect(component.isMaxJvmMemorySelected()).toBe(true);

    component.onJvmMemorySliderChange(0);
    expect(component.isMaxJvmMemorySelected()).toBe(false);
  });

  it("preserves a valid previous JVM selection when switching between large memory options", () => {
    fixture.detectChanges();
    component.selectedMemory = "8Gi";
    component.onMemorySelectionChange();
    component.onJvmMemorySliderChange(1); // Index 1 for 4G in [2, 4, 6]

    component.selectedMemory = "4096Mi";
    component.onMemorySelectionChange();

    expect(component.jvmMemorySliderValue).toBe(1);
    expect(component.selectedJvmMemorySize).toBe("4G");
  });

  it("resets the JVM selection when switching to an unrecognized memory unit", () => {
    fixture.detectChanges();
    component.selectedMemory = "8Gi";
    component.onMemorySelectionChange();
    component.onJvmMemorySliderChange(1);

    component.selectedMemory = "2";
    component.onMemorySelectionChange();

    const expected = getJvmMemorySliderConfig("2");
    expect(component.showJvmMemorySlider).toBe(expected.showJvmMemorySlider);
    expect(component.jvmMemorySliderValue).toBe(expected.jvmMemorySliderValue);
  });

  describe("template rendering", () => {
    afterEach(() => {
      document.querySelectorAll(".cdk-overlay-container").forEach(el => el.remove());
    });

    it("renders the full kubernetes form with GPU select, shm warning, JVM slider and max-memory alert", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes", "local"] as WorkflowComputingUnitType[] })
      );
      mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
        of({ cpuLimitOptions: ["1", "2"], memoryLimitOptions: ["8Gi", "16Gi"], gpuLimitOptions: ["0", "1"] })
      );
      fixture.detectChanges();
      component.onJvmMemorySliderChange(component.jvmMemoryMax);
      component.shmSizeValue = 16;
      component.shmSizeUnit = "Gi";
      component.visible = true;
      fixture.detectChanges();

      expect(document.querySelector(".create-compute-unit-container")).toBeTruthy();
      expect(document.querySelector(".unit-name-input")).toBeTruthy();
      expect(document.querySelector(".gpu-selection")).toBeTruthy();

      // Collapsed: the invalid shm value is announced on the panel header.
      expect(document.querySelector(".advanced-settings-hint--warning")?.textContent).toContain(
        "Shared memory exceeds total"
      );

      // Expand the panel so the advanced controls are actually user-visible
      // before asserting on them.
      (document.querySelector(".advanced-settings-toggle") as HTMLButtonElement).click();
      fixture.detectChanges();

      expect(document.querySelector(".shm-warning")?.textContent).toContain(
        "Shared memory cannot be greater than total memory."
      );
      expect(document.querySelector(".jvm-memory-slider")).toBeTruthy();
      expect(document.querySelector("nz-alert")).toBeTruthy();
      expect(document.querySelector(".unit-uri-input")).toBeNull();
    });

    it("expands and collapses the advanced panel when the toggle button is clicked", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes"] as WorkflowComputingUnitType[] })
      );
      mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
        of({ cpuLimitOptions: ["1"], memoryLimitOptions: ["1Gi"], gpuLimitOptions: ["0"] })
      );
      fixture.detectChanges();
      component.visible = true;
      fixture.detectChanges();

      const toggle = document.querySelector(".advanced-settings-toggle") as HTMLButtonElement;
      const body = document.querySelector(".advanced-settings-body") as HTMLElement;
      expect(toggle.getAttribute("aria-expanded")).toBe("false");
      expect(body.classList.contains("expanded")).toBe(false);

      toggle.click();
      fixture.detectChanges();
      expect(component.showAdvancedSettings).toBe(true);
      expect(toggle.getAttribute("aria-expanded")).toBe("true");
      expect(body.classList.contains("expanded")).toBe(true);

      toggle.click();
      fixture.detectChanges();
      expect(component.showAdvancedSettings).toBe(false);
      expect(toggle.getAttribute("aria-expanded")).toBe("false");
      expect(body.classList.contains("expanded")).toBe(false);
    });

    it("renders the minimal kubernetes form without GPU select, warnings, or slider", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes"] as WorkflowComputingUnitType[] })
      );
      mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
        of({ cpuLimitOptions: ["1"], memoryLimitOptions: ["1Gi"], gpuLimitOptions: ["0"] })
      );
      fixture.detectChanges();
      component.visible = true;
      fixture.detectChanges();

      expect(document.querySelector(".create-compute-unit-container")).toBeTruthy();
      expect(document.querySelector(".gpu-selection")).toBeNull();
      expect(document.querySelector(".shm-warning")).toBeNull();
      expect(document.querySelector(".jvm-memory-slider")).toBeNull();
      expect(document.querySelector("nz-alert")).toBeNull();
    });

    it("renders the local form with name and URI inputs", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["local"] as WorkflowComputingUnitType[] })
      );
      fixture.detectChanges();
      component.visible = true;
      fixture.detectChanges();

      expect(document.querySelector(".unit-uri-input")).toBeTruthy();
      expect(document.querySelector(".memory-selection")).toBeNull();
    });

    // The modal body renders inside the CDK overlay, which is a view attached to
    // ApplicationRef rather than to this fixture — tick() is what re-renders it.
    const tick = (): void => {
      fixture.detectChanges();
      TestBed.inject(ApplicationRef).tick();
    };

    const setInputValue = (selector: string, value: string): void => {
      const input = document.querySelector<HTMLInputElement>(selector)!;
      input.value = value;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      tick();
    };

    const clickSelectOption = (selectClass: string, optionText: string): void => {
      const select = document.querySelector<HTMLElement>(`nz-select.${selectClass}`)!;
      select.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      tick();
      // jsdom performs no layout, so the dropdown's virtual-scroll viewport
      // measures 0px and renders no options; force the range to materialize them.
      const viewportEl = document.querySelector("cdk-virtual-scroll-viewport")!;
      const viewport = (getDebugNode(viewportEl) as DebugElement).componentInstance as CdkVirtualScrollViewport;
      viewport.setRenderedRange({ start: 0, end: 20 });
      tick();
      const option = Array.from(document.querySelectorAll<HTMLElement>(".ant-select-item-option")).find(
        o => o.textContent?.trim() === optionText
      )!;
      option.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      tick();
    };

    it("updates the form state through the rendered controls", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes", "local"] as WorkflowComputingUnitType[] })
      );
      mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
        of({ cpuLimitOptions: ["1", "2"], memoryLimitOptions: ["8Gi", "16Gi"], gpuLimitOptions: ["0", "1"] })
      );
      fixture.detectChanges();
      component.visible = true;
      fixture.detectChanges();

      setInputValue(".unit-name-input", "Typed Name");
      expect(component.newComputingUnitName).toBe("Typed Name");

      // Expand the advanced panel first: the shm input and JVM slider sit in
      // the collapsed (inert) body, which a real user cannot interact with.
      (document.querySelector(".advanced-settings-toggle") as HTMLButtonElement).click();
      tick();

      setInputValue(".shm-size-input", "128");
      expect(component.shmSizeValue).toBe(128);

      clickSelectOption("memory-selection", "16Gi");
      expect(component.selectedMemory).toBe("16Gi");

      clickSelectOption("cpu-selection", "2");
      expect(component.selectedCpu).toBe("2");

      clickSelectOption("gpu-selection", "1");
      expect(component.selectedGpu).toBe("1");

      clickSelectOption("shm-unit-select", "Gi");
      expect(component.shmSizeUnit).toBe("Gi");

      // jsdom cannot produce meaningful slider drag geometry; emit the view-model
      // update at the NgModel seam, which is what a real drag/keypress triggers.
      const sliderEl = document.querySelector("nz-slider")!;
      const sliderNgModel = (getDebugNode(sliderEl) as DebugElement).injector.get(NgModel);
      sliderNgModel.viewToModelUpdate(component.jvmMemoryMax);
      tick();
      expect(component.jvmMemorySliderValue).toBe(component.jvmMemoryMax);
      expect(component.selectedJvmMemorySize).toBe(`${component.jvmMemorySteps[component.jvmMemoryMax]}G`);

      clickSelectOption("type-selection", "Local");
      expect(component.selectedComputingUnitType).toBe("local");
      fixture.detectChanges();

      setInputValue(".unit-name-input", "Local Name");
      expect(component.newComputingUnitName).toBe("Local Name");

      setInputValue(".unit-uri-input", "http://localhost:8085");
      expect(component.localComputingUnitUri).toBe("http://localhost:8085");
    });

    it("falls back to a slider minimum of 0 when no JVM steps are configured", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes"] as WorkflowComputingUnitType[] })
      );
      fixture.detectChanges();
      component.showJvmMemorySlider = true;
      component.jvmMemorySteps = [];
      component.visible = true;
      fixture.detectChanges();

      const sliderEl = document.querySelector("nz-slider")!;
      const slider = (getDebugNode(sliderEl) as DebugElement).componentInstance as { nzMin: number };
      expect(slider.nzMin).toBe(0);
    });

    it("wires the footer buttons to cancel and create", () => {
      mockComputingUnitService.getComputingUnitTypes.mockReturnValue(
        of({ typeOptions: ["kubernetes"] as WorkflowComputingUnitType[] })
      );
      fixture.detectChanges();
      component.visible = true;
      fixture.detectChanges();
      const visibleSpy = vi.fn();
      const unitCreatedSpy = vi.fn();
      component.visibleChange.subscribe(visibleSpy);
      component.unitCreated.subscribe(unitCreatedSpy);
      const buttons = document.querySelectorAll<HTMLButtonElement>(".ant-modal-footer button");
      expect(buttons.length).toBe(2);

      buttons[0].click();
      expect(visibleSpy).toHaveBeenCalledWith(false);
      expect(unitCreatedSpy).not.toHaveBeenCalled();

      component.visible = true;
      fixture.detectChanges();
      buttons[1].click();
      expect(unitCreatedSpy).toHaveBeenCalledWith(createdUnit);
      expect(visibleSpy).toHaveBeenCalledTimes(2);
    });
  });
});

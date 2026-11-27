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
import { CuImage, CuImageService } from "../../../dashboard/service/admin/cu-image/cu-image.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, throwError } from "rxjs";
import { HttpErrorResponse } from "@angular/common/http";
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
  const curatedImage = (over: Partial<CuImage>): CuImage => ({
    iid: 1,
    name: "Python ML",
    sourceRef: "owner/name:1",
    sourceDigest: "sha256:abc",
    status: "READY",
    imageTag: "owner/name@sha256:abc",
    attempt: 1,
    creationTime: 0,
    updateTime: 0,
    ...over,
  });
  const mockCuImageService = { list: vi.fn() };
  let mockComputingUnitService: Mocked<WorkflowComputingUnitManagingService>;
  let mockNotificationService: Mocked<NotificationService>;

  const createdUnit = { computingUnit: { cuid: 42 } } as unknown as DashboardWorkflowComputingUnit;

  const stubTypes = (...typeOptions: WorkflowComputingUnitType[]) =>
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(of({ typeOptions }));

  const stubLimits = (cpu: string[], memory: string[], gpu: string[]) =>
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(
      of({ cpuLimitOptions: cpu, memoryLimitOptions: memory, gpuLimitOptions: gpu })
    );

  const selectMemory = (memory: string): void => {
    component.selectedMemory = memory;
    component.onMemorySelectionChange();
  };

  const setShm = (value: number, unit: "Mi" | "Gi"): void => {
    component.shmSizeValue = value;
    component.shmSizeUnit = unit;
  };

  const setVisible = (): void => {
    component.visible = true;
    fixture.detectChanges();
  };

  const triggerVisible = (): void => {
    component.visible = true;
    component.ngOnChanges({ visible: { currentValue: true } as never });
  };

  /** Renders the component, optionally fires ngOnChanges(visible=true) (for host dialogs), then settles. */
  const openModal = (emitChanges = false): void => {
    fixture.detectChanges();
    if (emitChanges) triggerVisible();
    setVisible();
  };

  beforeEach(async () => {
    // No curated images by default, so the existing tests see today's behaviour. Cleared
    // as well as stubbed: the mock is shared, so call counts would carry between tests.
    mockCuImageService.list.mockClear();
    mockCuImageService.list.mockReturnValue(of([]));
    mockComputingUnitService = {
      getComputingUnitTypes: vi.fn(),
      getComputingUnitLimitOptions: vi.fn(),
      createKubernetesBasedComputingUnit: vi.fn(),
      createLocalComputingUnit: vi.fn(),
    } as unknown as Mocked<WorkflowComputingUnitManagingService>;
    stubTypes();
    stubLimits([], [], []);
    mockComputingUnitService.createKubernetesBasedComputingUnit.mockReturnValue(of(createdUnit));
    mockComputingUnitService.createLocalComputingUnit.mockReturnValue(of(createdUnit));

    mockNotificationService = {
      success: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
    } as unknown as Mocked<NotificationService>;

    await TestBed.configureTestingModule({
      providers: [
        { provide: CuImageService, useValue: mockCuImageService },
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

  const stubImageError = (status: number, statusText: string) =>
    mockCuImageService.list.mockReturnValue(throwError(() => new HttpErrorResponse({ status, statusText })));

  it("offers only ready images, and none when there are none", () => {
    mockCuImageService.list.mockReturnValue(
      of([curatedImage({ iid: 1 }), curatedImage({ iid: 2, name: "Still checking", status: "VALIDATING" })])
    );
    openModal(true);
    // A unit cannot start from an image that has not passed its check.
    expect(component.curatedImages.map(i => i.iid)).toEqual([1]);
    // Nothing is preselected: the default is the deployment's own image.
    expect(component.selectedImageId).toBe(component.DEPLOYMENT_IMAGE);
  });

  // A 503 means the feature is off and is expected; anything else means the picker
  // vanished for a reason the user should hear about.
  it("says so when the images cannot be loaded for a real reason", () => {
    stubImageError(500, "Server Error");
    openModal(true);
    expect(component.curatedImages).toEqual([]);
    expect(mockNotificationService.error).toHaveBeenCalled();
  });

  it("shows no images when the deployment has the feature switched off", () => {
    // The API answers 503 there. The dropdown is hidden and nothing else changes.
    stubImageError(503, "Service Unavailable");
    openModal(true);
    expect(component.curatedImages).toEqual([]);
    // Expected on such a deployment, so it is not reported.
    expect(mockNotificationService.error).not.toHaveBeenCalled();
  });

  it("sends the chosen image, and forgets it when the modal reopens", () => {
    mockCuImageService.list.mockReturnValue(of([curatedImage({ iid: 7 })]));
    stubTypes("kubernetes");
    mockComputingUnitService.createKubernetesBasedComputingUnit.mockReturnValue(of(createdUnit));
    openModal(true);

    component.newComputingUnitName = "On a curated image";
    component.selectedImageId = 7;
    component.startComputingUnit();
    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).toHaveBeenCalledWith(
      "On a curated image",
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      expect.anything(),
      7
    );

    // The image is chosen per unit, so the next one must not inherit it.
    triggerVisible();
    expect(component.selectedImageId).toBe(component.DEPLOYMENT_IMAGE);
  });

  // The regression this guards: read in ngOnInit, which runs at page load because neither
  // host wraps this modal in an *ngIf, an image that became ready since then never showed.
  it("re-reads the images each time the dialog opens", () => {
    mockCuImageService.list.mockReturnValue(of([]));
    fixture.detectChanges();
    expect(mockCuImageService.list).not.toHaveBeenCalled();

    mockCuImageService.list.mockReturnValue(of([curatedImage({ iid: 9, name: "Ready later" })]));
    triggerVisible();
    expect(component.curatedImages.map(i => i.iid)).toEqual([9]);
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("prefers the kubernetes type when available", () => {
    stubTypes("local", "kubernetes");
    fixture.detectChanges();
    expect(component.selectedComputingUnitType).toBe("kubernetes");
  });

  it("falls back to the first available type when kubernetes is absent", () => {
    stubTypes("local");
    fixture.detectChanges();
    expect(component.selectedComputingUnitType).toBe("local");
  });

  it("applies fetched limit options with first-option defaults", () => {
    stubLimits(["2", "4"], ["2Gi", "4Gi"], ["0", "1"]);
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

  const expectNoCreateCalls = (): void => {
    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).not.toHaveBeenCalled();
    expect(mockComputingUnitService.createLocalComputingUnit).not.toHaveBeenCalled();
  };

  it("rejects a create without a selected type", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = undefined;

    component.startComputingUnit();

    expect(mockNotificationService.error).toHaveBeenCalledWith("Please select a valid computing unit type");
    expectNoCreateCalls();
  });

  it("emits unitCreated and a success toast on a successful kubernetes create", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "kubernetes";
    component.newComputingUnitName = "GPU Test Unit";
    component.selectedCpu = "2";
    component.selectedMemory = "4Gi";
    component.selectedGpu = "0";
    component.selectedJvmMemorySize = "2G";
    setShm(128, "Mi");
    const unitCreatedSpy = vi.fn();
    component.unitCreated.subscribe(unitCreatedSpy);

    component.startComputingUnit();

    expect(mockComputingUnitService.createKubernetesBasedComputingUnit).toHaveBeenCalledWith(
      "GPU Test Unit",
      "2",
      "4Gi",
      "0",
      "2G",
      "128Mi",
      // No curated image chosen, so the unit runs the deployment's own.
      undefined
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
    expectNoCreateCalls();
  });

  it("reconfigures the JVM memory slider when the memory selection changes", () => {
    fixture.detectChanges();
    selectMemory("4Gi");
    const expected = getJvmMemorySliderConfig("4Gi");
    expect(component.jvmMemoryMax).toBe(expected.jvmMemoryMax);
    expect(component.showJvmMemorySlider).toBe(expected.showJvmMemorySlider);
    expect(component.jvmMemorySteps).toEqual(expected.jvmMemorySteps);
  });

  it("flags shared memory larger than total memory", () => {
    fixture.detectChanges();
    component.selectedMemory = "1Gi";
    setShm(2, "Gi");
    expect(component.isShmTooLarge()).toBe(true);

    setShm(64, "Mi");
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
    selectMemory("8Gi");
    setShm(4, "Gi");
    component.onJvmMemorySliderChange(component.jvmMemoryMax);

    component.ngOnChanges({ visible: new SimpleChange(false, true, false) });

    const expected = getJvmMemorySliderConfig("8Gi");
    expect(component.shmSizeValue).toBe(64);
    expect(component.shmSizeUnit).toBe("Mi");
    expect(component.jvmMemorySliderValue).toBe(expected.jvmMemorySliderValue);
    expect(component.selectedJvmMemorySize).toBe(expected.selectedJvmMemorySize);

    // Closing the modal must not touch the values.
    setShm(2, "Gi");
    component.ngOnChanges({ visible: new SimpleChange(true, false, false) });
    expect(component.shmSizeValue).toBe(2);
    expect(component.shmSizeUnit).toBe("Gi");
  });

  const setupCollapsedWarning = (shmTooLarge: boolean, maxJvm?: boolean, memory?: string): void => {
    vi.spyOn(component, "isShmTooLarge").mockReturnValue(shmTooLarge);
    if (maxJvm !== undefined) vi.spyOn(component, "isMaxJvmMemorySelected").mockReturnValue(maxJvm);
    if (memory !== undefined) component.selectedMemory = memory;
    component.showAdvancedSettings = false;
  };

  it("surfaces the max-JVM warning on the collapsed header", () => {
    setupCollapsedWarning(false, true);
    expect(component.hasCollapsedWarning()).toBe(true);
    expect(component.collapsedWarningText()).toBe("JVM memory at maximum");

    // Expanded: the inline nz-alert is visible, so the header stays quiet.
    component.showAdvancedSettings = true;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("prefers the shm warning over the JVM warning when both apply", () => {
    setupCollapsedWarning(true, true, "4Gi");
    expect(component.collapsedWarningText()).toBe("Shared memory exceeds total");
  });

  it("flags a collapsed warning only when collapsed and shm exceeds memory", () => {
    setupCollapsedWarning(true, undefined, "4Gi");
    expect(component.hasCollapsedWarning()).toBe(true);
    // Expanded: the inline warning is visible, so the header stays quiet.
    component.showAdvancedSettings = true;
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("suppresses the shm warning until the memory options have loaded", () => {
    setupCollapsedWarning(true, undefined, "");
    expect(component.hasCollapsedWarning()).toBe(false);
  });

  it("does not flag a warning when shm fits within memory", () => {
    setupCollapsedWarning(false);
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

  const expectErrorContaining = (fragment: string): void =>
    expect(mockNotificationService.error).toHaveBeenCalledWith(expect.stringContaining(fragment));

  it("toasts when fetching computing unit types fails", () => {
    mockComputingUnitService.getComputingUnitTypes.mockReturnValue(throwError(() => new Error("boom")));
    fixture.detectChanges();
    expectErrorContaining("Failed to fetch computing unit types");
  });

  it("toasts when fetching resource limit options fails", () => {
    mockComputingUnitService.getComputingUnitLimitOptions.mockReturnValue(throwError(() => new Error("boom")));
    fixture.detectChanges();
    expectErrorContaining("Failed to fetch resource options");
  });

  it("toasts when the create request fails", () => {
    fixture.detectChanges();
    component.selectedComputingUnitType = "kubernetes";
    component.newComputingUnitName = "Unit";
    mockComputingUnitService.createKubernetesBasedComputingUnit.mockReturnValue(throwError(() => new Error("boom")));
    const unitCreatedSpy = vi.fn();
    component.unitCreated.subscribe(unitCreatedSpy);

    component.startComputingUnit();

    expectErrorContaining("Failed to start computing unit");
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
    selectMemory("8Gi");

    component.onJvmMemorySliderChange(component.jvmMemoryMax - 1);

    expect(component.jvmMemorySteps).toContain(component.jvmMemorySteps[component.jvmMemorySliderValue]);
    expect(component.selectedJvmMemorySize).toBe(`${component.jvmMemorySteps[component.jvmMemorySliderValue]}G`);
  });

  it("flags max JVM memory only when the slider is shown at its maximum", () => {
    fixture.detectChanges();
    selectMemory("8Gi");

    component.onJvmMemorySliderChange(component.jvmMemoryMax);
    expect(component.isMaxJvmMemorySelected()).toBe(true);

    component.onJvmMemorySliderChange(0);
    expect(component.isMaxJvmMemorySelected()).toBe(false);
  });

  it("preserves a valid previous JVM selection when switching between large memory options", () => {
    fixture.detectChanges();
    selectMemory("8Gi");
    component.onJvmMemorySliderChange(1); // Index 1 for 4G in [2, 4, 6]

    selectMemory("4096Mi");

    expect(component.jvmMemorySliderValue).toBe(1);
    expect(component.selectedJvmMemorySize).toBe("4G");
  });

  it("resets the JVM selection when switching to an unrecognized memory unit", () => {
    fixture.detectChanges();
    selectMemory("8Gi");
    component.onJvmMemorySliderChange(1);

    selectMemory("2");

    const expected = getJvmMemorySliderConfig("2");
    expect(component.showJvmMemorySlider).toBe(expected.showJvmMemorySlider);
    expect(component.jvmMemorySliderValue).toBe(expected.jvmMemorySliderValue);
  });

  describe("template rendering", () => {
    const stubFullForm = (): void => {
      stubTypes("kubernetes", "local");
      stubLimits(["1", "2"], ["8Gi", "16Gi"], ["0", "1"]);
    };
    const openMinimalK8s = (): void => {
      stubTypes("kubernetes");
      stubLimits(["1"], ["1Gi"], ["0"]);
      openModal();
    };
    const expectPresent = (...selectors: string[]) =>
      selectors.forEach(s => expect(document.querySelector(s)).toBeTruthy());
    const expectAbsent = (...selectors: string[]) =>
      selectors.forEach(s => expect(document.querySelector(s)).toBeNull());

    afterEach(() => {
      document.querySelectorAll(".cdk-overlay-container").forEach(el => el.remove());
    });

    it("renders the full kubernetes form with GPU select, shm warning, JVM slider and max-memory alert", () => {
      stubFullForm();
      fixture.detectChanges();
      component.onJvmMemorySliderChange(component.jvmMemoryMax);
      setShm(16, "Gi");
      setVisible();

      expectPresent(".create-compute-unit-container", ".unit-name-input", ".gpu-selection");

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
      expectPresent(".jvm-memory-slider", "nz-alert");
      expectAbsent(".unit-uri-input");
    });

    it("expands and collapses the advanced panel when the toggle button is clicked", () => {
      openMinimalK8s();

      const toggle = document.querySelector(".advanced-settings-toggle") as HTMLButtonElement;
      const body = document.querySelector(".advanced-settings-body") as HTMLElement;
      const expectExpanded = (expanded: boolean): void => {
        expect(toggle.getAttribute("aria-expanded")).toBe(String(expanded));
        expect(body.classList.contains("expanded")).toBe(expanded);
      };
      expectExpanded(false);

      toggle.click();
      fixture.detectChanges();
      expect(component.showAdvancedSettings).toBe(true);
      expectExpanded(true);

      toggle.click();
      fixture.detectChanges();
      expect(component.showAdvancedSettings).toBe(false);
      expectExpanded(false);
    });

    it("renders the minimal kubernetes form without GPU select, warnings, or slider", () => {
      openMinimalK8s();

      expectPresent(".create-compute-unit-container");
      expectAbsent(".gpu-selection", ".shm-warning", ".jvm-memory-slider", "nz-alert");
    });

    it("renders the local form with name and URI inputs", () => {
      stubTypes("local");
      openModal();

      expectPresent(".unit-uri-input");
      expectAbsent(".memory-selection");
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
      stubFullForm();
      openModal();

      setInputValue(".unit-name-input", "Typed Name");
      expect(component.newComputingUnitName).toBe("Typed Name");

      // Expand the advanced panel first: the shm input and JVM slider sit in
      // the collapsed (inert) body, which a real user cannot interact with.
      (document.querySelector(".advanced-settings-toggle") as HTMLButtonElement).click();
      tick();

      setInputValue(".shm-size-input", "128");
      expect(component.shmSizeValue).toBe(128);

      for (const [selectClass, optionText, readActual] of [
        ["memory-selection", "16Gi", () => component.selectedMemory],
        ["cpu-selection", "2", () => component.selectedCpu],
        ["gpu-selection", "1", () => component.selectedGpu],
        ["shm-unit-select", "Gi", () => component.shmSizeUnit],
      ] as const) {
        clickSelectOption(selectClass, optionText);
        expect(readActual()).toBe(optionText);
      }

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
      stubTypes("kubernetes");
      fixture.detectChanges();
      component.showJvmMemorySlider = true;
      component.jvmMemorySteps = [];
      setVisible();

      const sliderEl = document.querySelector("nz-slider")!;
      const slider = (getDebugNode(sliderEl) as DebugElement).componentInstance as { nzMin: number };
      expect(slider.nzMin).toBe(0);
    });

    it("wires the footer buttons to cancel and create", () => {
      stubTypes("kubernetes");
      openModal();
      const visibleSpy = vi.fn();
      const unitCreatedSpy = vi.fn();
      component.visibleChange.subscribe(visibleSpy);
      component.unitCreated.subscribe(unitCreatedSpy);
      const buttons = document.querySelectorAll<HTMLButtonElement>(".ant-modal-footer button");
      expect(buttons.length).toBe(2);

      buttons[0].click();
      expect(visibleSpy).toHaveBeenCalledWith(false);
      expect(unitCreatedSpy).not.toHaveBeenCalled();

      setVisible();
      buttons[1].click();
      expect(unitCreatedSpy).toHaveBeenCalledWith(createdUnit);
      expect(visibleSpy).toHaveBeenCalledTimes(2);
    });
  });
});

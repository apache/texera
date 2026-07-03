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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { of } from "rxjs";
import type { Mocked } from "vitest";
import { ComputingUnitCreateModalComponent } from "./computing-unit-create-modal.component";
import { WorkflowComputingUnitManagingService } from "../../service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { NotificationService } from "../../service/notification/notification.service";
import { DashboardWorkflowComputingUnit, WorkflowComputingUnitType } from "../../type/workflow-computing-unit";
import { commonTestProviders } from "../../testing/test-utils";
import { getJvmMemorySliderConfig } from "../../util/computing-unit.util";

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
        NzModalService,
        { provide: WorkflowComputingUnitManagingService, useValue: mockComputingUnitService },
        { provide: NotificationService, useValue: mockNotificationService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
      imports: [ComputingUnitCreateModalComponent, HttpClientTestingModule],
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
});

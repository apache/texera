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

import { Component, Input, OnInit } from "@angular/core";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { DashboardEntry } from "../../../type/dashboard-entry";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitType,
} from "../../../../common/type/workflow-computing-unit";
import { extractErrorMessage } from "../../../../common/util/error";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import {
  parseResourceUnit,
  parseResourceNumber,
  findNearestValidStep,
  unitTypeMessageTemplate,
  isComputingUnitShmTooLarge,
  getJvmMemorySliderConfig,
} from "../../../../common/util/computing-unit.util";
import { ComputingUnitActionsService } from "../../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-section",
  templateUrl: "user-computing-unit.component.html",
  styleUrls: ["user-computing-unit.component.scss"],
})
export class UserComputingUnitComponent implements OnInit {
  public entries: DashboardEntry[] = [];
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;

  allComputingUnits: DashboardWorkflowComputingUnit[] = [];

  // variables for creating a computing unit
  addComputeUnitModalVisible = false;
  newComputingUnitName: string = "";
  selectedMemory: string = "";
  selectedCpu: string = "";
  selectedGpu: string = "0"; // Default to no GPU
  selectedJvmMemorySize: string = "1G"; // Initial JVM memory size
  selectedComputingUnitType?: WorkflowComputingUnitType; // Selected computing unit type
  selectedShmSize: string = "64Mi"; // Shared memory size
  shmSizeValue: number = 64; // default to 64
  shmSizeUnit: "Mi" | "Gi" = "Mi"; // default unit
  availableComputingUnitTypes: WorkflowComputingUnitType[] = [];
  localComputingUnitUri: string = ""; // URI for local computing unit

  // JVM memory slider configuration
  jvmMemorySliderValue: number = 1; // Initial value in GB
  jvmMemoryMarks: { [key: number]: string } = { 1: "1G" };
  jvmMemoryMax: number = 1;
  jvmMemorySteps: number[] = [1]; // Available steps in binary progression (1,2,4,8...)
  showJvmMemorySlider: boolean = false; // Whether to show the slider

  // cpu&memory limit options from backend
  cpuOptions: string[] = [];
  memoryOptions: string[] = [];
  gpuOptions: string[] = []; // Add GPU options array

  constructor(
    private notificationService: NotificationService,
    private modalService: NzModalService,
    private userService: UserService,
    private computingUnitService: WorkflowComputingUnitManagingService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private computingUnitActionsService: ComputingUnitActionsService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngOnInit() {
    this.localComputingUnitUri = `${window.location.protocol}//${window.location.hostname}${window.location.port ? `:${window.location.port}` : ""}/wsapi`;
    this.newComputingUnitName = "My Computing Unit";
    this.computingUnitService
      .getComputingUnitTypes()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ typeOptions }) => {
          this.availableComputingUnitTypes = typeOptions;
          // Set default selected type if available
          if (typeOptions.includes("kubernetes")) {
            this.selectedComputingUnitType = "kubernetes";
          } else if (typeOptions.length > 0) {
            this.selectedComputingUnitType = typeOptions[0];
          }
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to fetch computing unit types: ${extractErrorMessage(err)}`),
      });

    this.computingUnitService
      .getComputingUnitLimitOptions()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ cpuLimitOptions, memoryLimitOptions, gpuLimitOptions }) => {
          this.cpuOptions = cpuLimitOptions;
          this.memoryOptions = memoryLimitOptions;
          this.gpuOptions = gpuLimitOptions;

          // fallback defaults
          this.selectedCpu = this.cpuOptions[0] ?? "1";
          this.selectedMemory = this.memoryOptions[0] ?? "1Gi";
          this.selectedGpu = this.gpuOptions[0] ?? "0";

          // Initialize JVM memory slider based on selected memory
          this.updateJvmMemorySlider();
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to fetch resource options: ${extractErrorMessage(err)}`),
      });

    this.computingUnitStatusService
      .getAllComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.allComputingUnits = units;
        this.entries = units.map(u => new DashboardEntry(u));
      });
  }

  terminateComputingUnit(cuid: number): void {
    const unit = this.allComputingUnits.find(u => u.computingUnit.cuid === cuid);

    if (!unit) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    this.computingUnitActionsService.confirmAndTerminate(cuid, unit);
  }

  startComputingUnit(): void {
    if (this.selectedComputingUnitType === "kubernetes" && this.newComputingUnitName.trim() === "") {
      this.notificationService.error("Name of the computing unit cannot be empty");
      return;
    }

    if (this.selectedComputingUnitType === "local" && this.localComputingUnitUri.trim() === "") {
      this.notificationService.error("URI for local computing unit cannot be empty");
      return;
    }

    if (!this.selectedComputingUnitType) {
      this.notificationService.error("Please select a valid computing unit type");
      return;
    }

    const request = {
      type: this.selectedComputingUnitType,
      name: this.newComputingUnitName,
      cpu: this.selectedCpu,
      memory: this.selectedMemory,
      gpu: this.selectedGpu,
      jvmMemorySize: this.selectedJvmMemorySize,
      shmSize: `${this.shmSizeValue}${this.shmSizeUnit}`,
      localUri: this.localComputingUnitUri,
    };

    this.computingUnitActionsService
      .create(request)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success("Successfully created the new compute unit");
          this.computingUnitStatusService.refreshComputingUnitList();
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to start computing unit: ${extractErrorMessage(err)}`),
      });
  }

  showGpuSelection(): boolean {
    // Don't show GPU selection if there are no options or only "0" option
    return this.gpuOptions.length > 1 || (this.gpuOptions.length === 1 && this.gpuOptions[0] !== "0");
  }

  showAddComputeUnitModalVisible(): void {
    this.addComputeUnitModalVisible = true;
  }

  handleAddComputeUnitModalOk(): void {
    this.startComputingUnit();
    this.addComputeUnitModalVisible = false;
  }

  handleAddComputeUnitModalCancel(): void {
    this.addComputeUnitModalVisible = false;
  }

  isShmTooLarge(): boolean {
    return isComputingUnitShmTooLarge(this.selectedMemory, this.shmSizeValue, this.shmSizeUnit);
  }

  updateJvmMemorySlider(): void {
    this.resetJvmMemorySlider();
  }

  onJvmMemorySliderChange(value: number): void {
    // Ensure the value is one of the valid steps
    const validStep = findNearestValidStep(value, this.jvmMemorySteps);
    this.jvmMemorySliderValue = validStep;
    this.selectedJvmMemorySize = `${validStep}G`;
  }

  isMaxJvmMemorySelected(): boolean {
    // Only show warning for larger memory sizes (>=4GB) where the slider is shown
    // AND when the maximum value is selected
    return this.showJvmMemorySlider && this.jvmMemorySliderValue === this.jvmMemoryMax && this.jvmMemoryMax >= 4;
  }

  // Completely reset the JVM memory slider based on the selected CU memory
  resetJvmMemorySlider(): void {
    const config = getJvmMemorySliderConfig(this.selectedMemory);

    this.jvmMemoryMax = config.jvmMemoryMax;
    this.showJvmMemorySlider = config.showJvmMemorySlider;
    this.jvmMemorySteps = config.jvmMemorySteps;
    this.jvmMemoryMarks = config.jvmMemoryMarks;
    this.jvmMemorySliderValue = config.jvmMemorySliderValue;
    this.selectedJvmMemorySize = config.selectedJvmMemorySize;
  }

  onMemorySelectionChange(): void {
    // Store current JVM memory value for potential reuse
    const previousJvmMemory = this.jvmMemorySliderValue;

    // Reset slider configuration based on the new memory selection
    this.resetJvmMemorySlider();

    // For CU memory > 3GB, preserve previous value if valid and >= 2GB
    // Get the current memory in GB
    const memoryValue = parseResourceNumber(this.selectedMemory);
    const memoryUnit = parseResourceUnit(this.selectedMemory);
    let cuMemoryInGb = memoryUnit === "Gi" ? memoryValue : memoryUnit === "Mi" ? Math.floor(memoryValue / 1024) : 1;

    // Only try to preserve previous value for larger memory sizes where slider is shown
    if (
      cuMemoryInGb > 3 &&
      previousJvmMemory >= 2 &&
      previousJvmMemory <= this.jvmMemoryMax &&
      this.jvmMemorySteps.includes(previousJvmMemory)
    ) {
      this.jvmMemorySliderValue = previousJvmMemory;
      this.selectedJvmMemorySize = `${previousJvmMemory}G`;
    }
  }

  getCreateModalTitle(): string {
    if (!this.selectedComputingUnitType) return "Create Computing Unit";
    return unitTypeMessageTemplate[this.selectedComputingUnitType].createTitle;
  }
}

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

import { Component, OnInit, OnChanges, SimpleChanges } from "@angular/core";
import { interval } from "rxjs";
import { switchMap } from "rxjs/operators";
import { WorkflowComputingUnitManagingService } from "../../service/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { environment } from "../../../../environments/environment";
import { extractErrorMessage } from "../../../common/util/error";
import { ComputingUnitStatusService } from "../../service/computing-unit-status/computing-unit-status.service";
import { NzModalService } from "ng-zorro-antd/modal";

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-selection",
  templateUrl: "./computing-unit-selection.component.html",
  styleUrls: ["./computing-unit-selection.component.scss"],
})
export class ComputingUnitSelectionComponent implements OnInit, OnChanges {
  // current workflow's Id, will change with wid in the workflowActionService.metadata
  workflowId: number | undefined;

  selectedComputingUnit: DashboardWorkflowComputingUnit | null = null;
  computingUnits: DashboardWorkflowComputingUnit[] = [];
  private readonly REFRESH_INTERVAL_MS = 2000;

  // variables for creating a computing unit
  addComputeUnitModalVisible = false;
  newComputingUnitName: string = "";
  selectedMemory: string = "";
  selectedCpu: string = "";
  selectedGpu: string = "0"; // Default to no GPU
  selectedJvmMemorySize: string = "1G"; // Initial JVM memory size

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

  // Add property to track user-initiated termination
  private isUserTerminatingUnit = false;

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private modalService: NzModalService
  ) {}

  ngOnInit(): void {
    if (environment.computingUnitManagerEnabled) {
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
    }

    // Track if user is intentionally terminating a unit
    this.isUserTerminatingUnit = false;

    // Subscribe to the current selected unit from the status service
    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
        // Check if the status changed from Running to something else
        if (
          this.selectedComputingUnit?.status === "Running" &&
          unit?.status &&
          unit.status !== "Running" &&
          !this.isUserTerminatingUnit
        ) {
          // Only show notification for unexpected status changes
          if (unit.status === "Disconnected") {
            this.notificationService.info(`Connecting to computing unit "${unit.computingUnit.name}"...`);
          } else if (unit.status === "Terminating") {
            this.notificationService.error(
              `Computing unit "${unit.computingUnit.name}" is being terminated. Please select another unit to continue.`
            );
          }
        }

        this.selectedComputingUnit = unit;
      });

    // Subscribe to all available units from the status service
    this.computingUnitStatusService
      .getAllComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.computingUnits = units;
      });

    this.registerWorkflowMetadataSubscription();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["workflowId"] && isDefined(this.workflowId)) {
      // Connecting to the workflowWebsocketService with the workflowId will trigger the computing unit service
      if (this.selectedComputingUnit) {
        this.connectToComputingUnit(this.selectedComputingUnit);
      }
    }
  }

  /**
   * Registers a subscription to listen for workflow metadata changes;
   * Calls `onComputingUnitChange` when the `wid` changes;
   * The wid can change by time because of the workspace rendering;
   */
  private registerWorkflowMetadataSubscription(): void {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
        if (wid !== this.workflowId) {
          this.workflowId = wid;
          this.connectToComputingUnit(this.selectedComputingUnit);
        }
      });
  }

  /**
   * Called whenever the selected computing unit changes.
   */
  connectToComputingUnit(computingUnit: DashboardWorkflowComputingUnit | null): void {
    if (computingUnit && isDefined(this.workflowId)) {
      // First update the selection in the status service
      this.computingUnitStatusService.selectComputingUnit(computingUnit);

      // Then open the websocket connection
      this.workflowWebsocketService.closeWebsocket();
      this.workflowWebsocketService.openWebsocket(this.workflowId, undefined, computingUnit.computingUnit.cuid);
    }
  }

  isComputingUnitRunning(): boolean {
    return this.selectedComputingUnit != null && this.selectedComputingUnit.status === "Running";
  }

  getButtonText(): string {
    if (!this.selectedComputingUnit) {
      return "Connect";
    } else {
      return this.selectedComputingUnit.computingUnit.name;
    }
  }

  computeStatus(): string {
    if (!this.selectedComputingUnit) {
      return "processing";
    }

    const status = this.selectedComputingUnit.status;
    if (status === "Running") {
      return "success";
    } else if (status === "Pending" || status === "Terminating") {
      return "warning";
    } else {
      return "error";
    }
  }

  /**
   * Determines if a unit cannot be selected (disabled in the dropdown)
   */
  cannotSelectUnit(unit: DashboardWorkflowComputingUnit): boolean {
    // Only allow selecting units that are in the Running state
    return unit.status !== "Running";
  }

  isSelectedUnit(unit: DashboardWorkflowComputingUnit): boolean {
    return unit.uri === this.selectedComputingUnit?.uri;
  }

  // Determines if the GPU selection dropdown should be shown
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

  /**
   * Gets the computing unit name from the units URI
   * @param unitURI (i.e. "computing-unit-85.workflow-computing-unit-svc.workflow-computing-unit-pool.svc.cluster.local")
   * @return "Computing unit 85"
   */
  getComputingUnitId(unitURI: string): string {
    if (unitURI.includes("localhost")) return "Local Computing Unit";
    const re = /computing-unit-(\d+)/;
    const match = unitURI.match(re);
    if (match) {
      return `Computing unit ${match[1]}`;
    }
    return "Unknown Computing Unit";
  }

  /**
   * Start a new computing unit.
   */
  startComputingUnit(): void {
    if (this.newComputingUnitName.trim() == "") {
      this.notificationService.error("Name of the computing unit cannot be empty");
      return;
    }
    const computeUnitName = this.newComputingUnitName;
    const computeCPU = this.selectedCpu;
    const computeMemory = this.selectedMemory;
    const computeGPU = this.selectedGpu;
    const computeJvmMemory = this.selectedJvmMemorySize;

    this.computingUnitService
      .createComputingUnit(computeUnitName, computeCPU, computeMemory, computeGPU, computeJvmMemory)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (unit: DashboardWorkflowComputingUnit) => {
          this.notificationService.success("Successfully created the new compute unit");
          // Select the newly created unit
          this.connectToComputingUnit(unit);
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to start computing unit: ${extractErrorMessage(err)}`),
      });
  }

  /**
   * Terminate a computing unit.
   * @param cuid The CUID of the unit to terminate.
   */
  terminateComputingUnit(cuid: number): void {
    const unit = this.computingUnits.find(unit => unit.computingUnit.cuid === cuid);

    if (!unit || !unit.uri) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    const unitName = unit.computingUnit.name;
    const unitId = this.getComputingUnitId(unit.uri);
    const isTerminatingSelectedUnit = this.selectedComputingUnit?.computingUnit.cuid === cuid;

    // Show confirmation modal
    this.modalService.confirm({
      nzTitle: "Terminate Computing Unit",
      nzContent: `
        <p>Are you sure you want to terminate <strong>${unitName}</strong>?</p>
        <p style="color: #ff4d4f;"><strong>Warning:</strong> All execution results in this computing unit will be lost.</p>
      `,
      nzOkText: "Terminate",
      nzOkType: "primary",
      nzOnOk: () => {
        // Set flag to avoid showing disconnection errors during intentional termination
        if (isTerminatingSelectedUnit) {
          this.isUserTerminatingUnit = true;
        }

        // Use the ComputingUnitStatusService to handle termination
        // This will properly close the websocket before terminating the unit
        this.computingUnitStatusService
          .terminateComputingUnit(cuid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: (success: boolean) => {
              // Reset the termination flag regardless of result
              if (isTerminatingSelectedUnit) {
                this.isUserTerminatingUnit = false;
              }

              if (success) {
                this.notificationService.success(`Terminated ${unitId}`);

                // Find another running unit to select if needed
                if (this.selectedComputingUnit === null || isTerminatingSelectedUnit) {
                  const runningUnit = this.computingUnits.find(
                    unit => unit.computingUnit.cuid !== cuid && unit.status === "Running"
                  );

                  if (runningUnit) {
                    this.connectToComputingUnit(runningUnit);
                  } else {
                    this.selectedComputingUnit = null;
                  }
                }
              } else {
                this.notificationService.error("Failed to terminate computing unit");
              }
            },
            error: (err: unknown) => {
              // Reset the termination flag on error
              if (isTerminatingSelectedUnit) {
                this.isUserTerminatingUnit = false;
              }
              this.notificationService.error(`Failed to terminate computing unit: ${extractErrorMessage(err)}`);
            },
          });
      },
      nzCancelText: "Cancel",
    });
  }

  parseResourceUnit(resource: string): string {
    // check if has a capacity (is a number followed by a unit)
    if (!resource || resource === "NaN") return "N/A";
    const re = /^(\d+(\.\d+)?)([a-zA-Z]*)$/;
    const match = resource.match(re);
    if (match) {
      return match[3] || "";
    }
    return "";
  }

  parseResourceNumber(resource: string): number {
    // check if has a capacity (is a number followed by a unit)
    if (!resource || resource === "NaN") return 0;
    const re = /^(\d+(\.\d+)?)([a-zA-Z]*)$/;
    const match = resource.match(re);
    if (match) {
      return parseFloat(match[1]);
    }
    return 0;
  }

  cpuResourceConversion(from: string, toUnit: string): string {
    // cpu conversions
    type CpuUnit = "n" | "u" | "m" | "";
    const cpuScales: { [key in CpuUnit]: number } = {
      n: 1,
      u: 1_000,
      m: 1_000_000,
      "": 1_000_000_000,
    };
    const fromUnit = this.parseResourceUnit(from) as CpuUnit;
    const fromNumber = this.parseResourceNumber(from);

    // Handle empty unit in input (means cores)
    const effectiveFromUnit = (fromUnit || "") as CpuUnit;
    const effectiveToUnit = (toUnit || "") as CpuUnit;

    // Convert to base units (nanocores) then to target unit
    const fromScaled = fromNumber * (cpuScales[effectiveFromUnit] || cpuScales["m"]);
    const toScaled = fromScaled / (cpuScales[effectiveToUnit] || cpuScales[""]);

    // For display purposes, use appropriate precision
    if (effectiveToUnit === "") {
      return toScaled.toFixed(4); // 4 decimal places for cores
    } else if (effectiveToUnit === "m") {
      return toScaled.toFixed(2); // 2 decimal places for millicores
    } else {
      return Math.round(toScaled).toString(); // Whole numbers for smaller units
    }
  }

  memoryResourceConversion(from: string, toUnit: string): string {
    // memory conversion
    type MemoryUnit = "Ki" | "Mi" | "Gi" | "";
    const memoryScales: { [key in MemoryUnit]: number } = {
      "": 1,
      Ki: 1024,
      Mi: 1024 * 1024,
      Gi: 1024 * 1024 * 1024,
    };
    const fromUnit = this.parseResourceUnit(from) as MemoryUnit;
    const fromNumber = this.parseResourceNumber(from);

    // Handle empty unit in input (means bytes)
    const effectiveFromUnit = (fromUnit || "") as MemoryUnit;
    const effectiveToUnit = (toUnit || "") as MemoryUnit;

    // Convert to base units (bytes) then to target unit
    const fromScaled = fromNumber * (memoryScales[effectiveFromUnit] || 1);
    const toScaled = fromScaled / (memoryScales[effectiveToUnit] || 1);

    // For memory, we want to show in the same format as the limit (typically X.XXX Gi)
    return toScaled.toFixed(4);
  }

  getCurrentComputingUnitCpuUsage(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.metrics.cpuUsage : "N/A";
  }

  getCurrentComputingUnitMemoryUsage(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.metrics.memoryUsage : "N/A";
  }

  getCurrentComputingUnitCpuLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.resourceLimits.cpuLimit : "N/A";
  }

  getCurrentComputingUnitMemoryLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.resourceLimits.memoryLimit : "N/A";
  }

  getCurrentComputingUnitGpuLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.resourceLimits.gpuLimit : "0";
  }

  /**
   * Returns the badge color based on computing unit status
   */
  getBadgeColor(status: string): string {
    switch (status) {
      case "Running":
        return "green";
      case "Pending":
        return "gold";
      default:
        return "red";
    }
  }

  getCpuLimit(): number {
    return this.parseResourceNumber(this.getCurrentComputingUnitCpuLimit());
  }

  getGpuLimit(): string {
    return this.getCurrentComputingUnitGpuLimit();
  }

  getCpuLimitUnit(): string {
    const unit = this.parseResourceUnit(this.getCurrentComputingUnitCpuLimit());
    if (unit === "") {
      return "CPU";
    }
    return unit;
  }

  getMemoryLimit(): number {
    return this.parseResourceNumber(this.getCurrentComputingUnitMemoryLimit());
  }

  getMemoryLimitUnit(): string {
    return this.parseResourceUnit(this.getCurrentComputingUnitMemoryLimit());
  }

  getCpuValue(): number {
    const usage = this.getCurrentComputingUnitCpuUsage();
    const limit = this.getCurrentComputingUnitCpuLimit();
    if (usage === "N/A" || limit === "N/A") return 0;
    const displayUnit = this.getCpuLimitUnit() === "CPU" ? "" : this.getCpuLimitUnit();
    const usageValue = this.cpuResourceConversion(usage, displayUnit);
    return parseFloat(usageValue);
  }

  getMemoryValue(): number {
    const usage = this.getCurrentComputingUnitMemoryUsage();
    const limit = this.getCurrentComputingUnitMemoryLimit();
    if (usage === "N/A" || limit === "N/A") return 0;
    const displayUnit = this.getMemoryLimitUnit();
    const usageValue = this.memoryResourceConversion(usage, displayUnit);
    return parseFloat(usageValue);
  }

  getCpuPercentage(): number {
    const usage = this.getCurrentComputingUnitCpuUsage();
    const limit = this.getCurrentComputingUnitCpuLimit();
    if (usage === "N/A" || limit === "N/A") return 0;

    // Convert to the same unit for comparison
    const displayUnit = ""; // Convert to cores for percentage calculation

    // Use our existing conversion method to get values in the same unit
    const usageValue = parseFloat(this.cpuResourceConversion(usage, displayUnit));
    const limitValue = parseFloat(this.cpuResourceConversion(limit, displayUnit));

    if (limitValue <= 0) return 0;

    // Calculate percentage and ensure it doesn't exceed 100%
    const percentage = (usageValue / limitValue) * 100;

    return Math.min(percentage, 100);
  }

  getMemoryPercentage(): number {
    const usage = this.getCurrentComputingUnitMemoryUsage();
    const limit = this.getCurrentComputingUnitMemoryLimit();
    if (usage === "N/A" || limit === "N/A") return 0;

    // Convert to the same unit for comparison
    const displayUnit = "Gi"; // Convert to GiB for percentage calculation

    // Use our existing conversion method to get values in the same unit
    const usageValue = parseFloat(this.memoryResourceConversion(usage, displayUnit));
    const limitValue = parseFloat(this.memoryResourceConversion(limit, displayUnit));

    if (limitValue <= 0) return 0;

    // Calculate percentage and ensure it doesn't exceed 100%
    const percentage = (usageValue / limitValue) * 100;

    return Math.min(percentage, 100);
  }

  getCpuStatus(): "success" | "exception" | "active" | "normal" {
    const percentage = this.getCpuPercentage();
    if (percentage > 90) return "exception";
    if (percentage > 50) return "normal";
    return "success";
  }

  getMemoryStatus(): "success" | "exception" | "active" | "normal" {
    const percentage = this.getMemoryPercentage();
    if (percentage > 90) return "exception";
    if (percentage > 50) return "normal";
    return "success";
  }

  getCpuUnit(): string {
    return this.getCpuLimitUnit() === "CPU" ? "Cores" : this.getCpuLimitUnit();
  }

  getMemoryUnit(): string {
    return this.getMemoryLimitUnit() === "" ? "B" : this.getMemoryLimitUnit();
  }

  /**
   * Returns a descriptive tooltip for a specific unit's status
   */
  getUnitStatusTooltip(unit: DashboardWorkflowComputingUnit): string {
    switch (unit.status) {
      case "Running":
        return "Ready to use";
      case "Pending":
        return "Computing unit is starting up";
      case "Disconnected":
        return "Computing unit is not connected";
      case "Terminating":
        return "Computing unit is being terminated";
      default:
        return unit.status;
    }
  }

  // Called when the component initializes
  updateJvmMemorySlider(): void {
    this.resetJvmMemorySlider();
  }

  // Find the nearest valid step value
  findNearestValidStep(value: number): number {
    if (this.jvmMemorySteps.length === 0) return 1;
    if (this.jvmMemorySteps.includes(value)) return value;

    // Find the closest step value
    return this.jvmMemorySteps.reduce((prev, curr) => {
      return Math.abs(curr - value) < Math.abs(prev - value) ? curr : prev;
    });
  }

  onJvmMemorySliderChange(value: number): void {
    // Ensure the value is one of the valid steps
    const validStep = this.findNearestValidStep(value);
    this.jvmMemorySliderValue = validStep;
    this.selectedJvmMemorySize = `${validStep}G`;
  }

  // Check if the maximum JVM memory value is selected
  isMaxJvmMemorySelected(): boolean {
    // Only show warning for larger memory sizes (>=4GB) where the slider is shown
    // AND when the maximum value is selected
    return this.showJvmMemorySlider && this.jvmMemorySliderValue === this.jvmMemoryMax && this.jvmMemoryMax >= 4;
  }

  // Completely reset the JVM memory slider based on the selected CU memory
  resetJvmMemorySlider(): void {
    // Parse memory limit to determine max JVM memory
    const memoryValue = this.parseResourceNumber(this.selectedMemory);
    const memoryUnit = this.parseResourceUnit(this.selectedMemory);

    // Set max JVM memory to the total memory selected (in GB)
    let cuMemoryInGb = 1; // Default to 1GB
    if (memoryUnit === "Gi") {
      cuMemoryInGb = memoryValue;
    } else if (memoryUnit === "Mi") {
      cuMemoryInGb = Math.max(1, Math.floor(memoryValue / 1024));
    }

    this.jvmMemoryMax = cuMemoryInGb;

    // Special cases for smaller memory sizes (1-3GB)
    if (cuMemoryInGb <= 3) {
      // Don't show slider for small memory sizes
      this.showJvmMemorySlider = false;

      // Set JVM memory size to 1GB when CU memory is 1GB, otherwise set to 2GB
      if (cuMemoryInGb === 1) {
        this.jvmMemorySliderValue = 1;
        this.selectedJvmMemorySize = "1G";
      } else {
        // For 2-3GB instances, use 2GB for JVM
        this.jvmMemorySliderValue = 2;
        this.selectedJvmMemorySize = "2G";
      }

      // Still calculate steps for completeness
      this.jvmMemorySteps = [];
      let value = 1;
      while (value <= this.jvmMemoryMax) {
        this.jvmMemorySteps.push(value);
        value = value * 2;
      }

      // Update marks
      this.jvmMemoryMarks = {};
      this.jvmMemorySteps.forEach(step => {
        this.jvmMemoryMarks[step] = `${step}G`;
      });

      return;
    }

    // For larger memory sizes (4GB+), show the slider
    this.showJvmMemorySlider = true;

    // Calculate binary steps (2,4,8,...) starting from 2GB
    this.jvmMemorySteps = [];
    let value = 2; // Start from 2GB for larger instances
    while (value <= this.jvmMemoryMax) {
      this.jvmMemorySteps.push(value);
      value = value * 2;
    }

    // Update slider marks
    this.jvmMemoryMarks = {};
    this.jvmMemorySteps.forEach(step => {
      this.jvmMemoryMarks[step] = `${step}G`;
    });

    // Always default to 2GB for larger memory sizes
    this.jvmMemorySliderValue = 2;
    this.selectedJvmMemorySize = "2G";
  }

  // Listen for memory selection changes
  onMemorySelectionChange(): void {
    // Store current JVM memory value for potential reuse
    const previousJvmMemory = this.jvmMemorySliderValue;

    // Reset slider configuration based on the new memory selection
    this.resetJvmMemorySlider();

    // For CU memory > 3GB, preserve previous value if valid and >= 2GB
    // Get the current memory in GB
    const memoryValue = this.parseResourceNumber(this.selectedMemory);
    const memoryUnit = this.parseResourceUnit(this.selectedMemory);
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

  protected readonly environment = environment;
}

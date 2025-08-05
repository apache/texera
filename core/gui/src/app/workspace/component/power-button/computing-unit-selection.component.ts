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

import { Component, OnInit, ChangeDetectorRef } from "@angular/core";
import { take } from "rxjs/operators";
import { WorkflowComputingUnitManagingService } from "../../service/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit, WorkflowComputingUnitType } from "../../types/workflow-computing-unit";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { extractErrorMessage } from "../../../common/util/error";
import { ComputingUnitStatusService } from "../../service/computing-unit-status/computing-unit-status.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ShareAccessComponent } from "../../../dashboard/component/user/share-access/share-access.component";
import { combineLatest } from "rxjs";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-selection",
  templateUrl: "./computing-unit-selection.component.html",
  styleUrls: ["./computing-unit-selection.component.scss"],
})
export class ComputingUnitSelectionComponent implements OnInit {
  // current workflow's Id, will change with wid in the workflowActionService.metadata
  workflowId: number | undefined;

  lastSelectedCuid?: number;
  selectedComputingUnit: DashboardWorkflowComputingUnit | null = null;
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

  // variables for renaming a computing unit
  editingNameOfUnit: number | null = null;
  editingUnitName: string = "";

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
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    protected config: GuiConfigService,
    private workflowActionService: WorkflowActionService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowExecutionsService: WorkflowExecutionsService,
    private modalService: NzModalService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    // Fetch available computing unit types
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

    // Subscribe to the current selected unit from the status service
    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
        const wid = this.workflowActionService.getWorkflowMetadata()?.wid;

        // ── compare with the *previous* cuid, not the one we are just about to store ──
        if (isDefined(wid) && unit?.computingUnit.cuid !== this.lastSelectedCuid) {
          this.updateWorkflowModificationStatus(wid);
        }

        // update local caches **after** the comparison
        this.lastSelectedCuid = unit?.computingUnit.cuid;
        this.selectedComputingUnit = unit;
      });

    this.computingUnitStatusService
      .getAllComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.allComputingUnits = units;
      });

    this.registerWorkflowMetadataSubscription();
  }

  /**
   * Helper to query backend and (de)activate modification status.
   */
  private updateWorkflowModificationStatus(wid: number): void {
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(wid, [ExecutionState.Running, ExecutionState.Initializing])
      .pipe(take(1), untilDestroyed(this))
      .subscribe(execList => {
        if (execList.length > 0) {
          this.notificationService.info(
            "There are onging executions on this workflow. Modification of the workflow is currently disabled."
          );
          this.workflowActionService.disableWorkflowModification();
        } else {
          this.workflowActionService.enableWorkflowModification();
        }
      });
  }

  /**
   * utility function used for displaying the computing unit
   */
  public trackByCuid(_idx: number, unit: DashboardWorkflowComputingUnit): number {
    return unit.computingUnit.cuid;
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
          if (isDefined(this.workflowId) && this.workflowId !== DEFAULT_WORKFLOW.wid) {
            this.workflowExecutionsService
              .retrieveLatestWorkflowExecution(this.workflowId)
              .pipe(untilDestroyed(this))
              .subscribe({
                next: (latestWorkflowExecution: WorkflowExecutionsEntry) => {
                  this.selectComputingUnit(this.workflowId, latestWorkflowExecution.cuId);
                },
                error: (err: unknown) => {
                  const runningUnit = this.allComputingUnits.find(unit => unit.status === "Running");
                  if (runningUnit) {
                    this.selectComputingUnit(this.workflowId, runningUnit.computingUnit.cuid);
                  }
                },
              });
          }
        }
      });
  }

  /**
   * Called whenever the selected computing unit changes.
   */
  selectComputingUnit(wid: number | undefined, cuid: number | undefined): void {
    if (isDefined(cuid) && wid !== DEFAULT_WORKFLOW.wid) {
      this.computingUnitStatusService.selectComputingUnit(wid, cuid);
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
    return unit.computingUnit.uri === this.selectedComputingUnit?.computingUnit.uri;
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

  isShmTooLarge(): boolean {
    const total = this.parseResourceNumber(this.selectedMemory);
    const unit = this.parseResourceUnit(this.selectedMemory);
    const memoryInMi = unit === "Gi" ? total * 1024 : total;
    const shmInMi = this.shmSizeUnit === "Gi" ? this.shmSizeValue * 1024 : this.shmSizeValue;

    return shmInMi > memoryInMi;
  }

  /**
   * Start a new computing unit.
   */
  startComputingUnit(): void {
    // Validate based on computing unit type
    if (this.selectedComputingUnitType === "kubernetes") {
      if (this.newComputingUnitName.trim() == "") {
        this.notificationService.error("Name of the computing unit cannot be empty");
        return;
      }

      this.selectedShmSize = `${this.shmSizeValue}${this.shmSizeUnit}`;

      this.computingUnitService
        .createKubernetesBasedComputingUnit(
          this.newComputingUnitName,
          this.selectedCpu,
          this.selectedMemory,
          this.selectedGpu,
          this.selectedJvmMemorySize,
          this.selectedShmSize
        )
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (unit: DashboardWorkflowComputingUnit) => {
            this.notificationService.success("Successfully created the new Kubernetes compute unit");
            // Select the newly created unit
            this.selectComputingUnit(this.workflowId, unit.computingUnit.cuid);
          },
          error: (err: unknown) =>
            this.notificationService.error(`Failed to start Kubernetes computing unit: ${extractErrorMessage(err)}`),
        });
    } else if (this.selectedComputingUnitType === "local") {
      // For local computing units, validate the URI
      if (!this.localComputingUnitUri || this.localComputingUnitUri.trim() === "") {
        this.notificationService.error("URI for local computing unit cannot be empty");
        return;
      }

      this.computingUnitService
        .createLocalComputingUnit(this.newComputingUnitName, this.localComputingUnitUri)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (unit: DashboardWorkflowComputingUnit) => {
            this.notificationService.success("Successfully created the new local compute unit");
            // Select the newly created unit
            this.selectComputingUnit(this.workflowId, unit.computingUnit.cuid);
          },
          error: (err: unknown) =>
            this.notificationService.error(`Failed to start local computing unit: ${extractErrorMessage(err)}`),
        });
    } else {
      this.notificationService.error("Please select a valid computing unit type");
    }
  }

  openComputingUnitMetadataModal(unit: DashboardWorkflowComputingUnit) {
    this.modalService.create({
      nzTitle: "Computing Unit Information",
      nzContent: `
        <table class="ant-table">
          <tbody>
            <tr><th style="width: 150px;">Name</th><td>${unit.computingUnit.name}</td></tr>
            <tr><th>Status</th><td>${unit.status}</td></tr>
            <tr><th>Type</th><td>${unit.computingUnit.type}</td></tr>
            <tr><th>CPU Limit</th><td>${unit.computingUnit.resource.cpuLimit}</td></tr>
            <tr><th>Memory Limit</th><td>${unit.computingUnit.resource.memoryLimit}</td></tr>
            <tr><th>GPU Limit</th><td>${unit.computingUnit.resource.gpuLimit || "None"}</td></tr>
            <tr><th>JVM Memory</th><td>${unit.computingUnit.resource.jvmMemorySize}</td></tr>
            <tr><th>Shared Memory</th><td>${unit.computingUnit.resource.shmSize}</td></tr>
            <tr><th>Created</th><td>${new Date(unit.computingUnit.creationTime).toLocaleString()}</td></tr>
            <tr><th>Access</th><td>${unit.isOwner ? "Owner" : unit.accessPrivilege}</td></tr>
          </tbody>
        </table>
      `,
      nzFooter: null,
      nzMaskClosable: true,
      nzWidth: "600px",
    });
  }

  /**
   * Terminate a computing unit.
   * @param cuid The CUID of the unit to terminate.
   */
  terminateComputingUnit(cuid: number): void {
    const unit = this.allComputingUnits.find(u => u.computingUnit.cuid === cuid);

    if (!unit || !unit.computingUnit.uri) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    const unitName = unit.computingUnit.name;
    const unitType = unit?.computingUnit.type || "kubernetes"; // fallback
    const templates = this.unitTypeMessageTemplate[unitType];

    // Show confirmation modal
    this.modalService.confirm({
      nzTitle: templates.terminateTitle,
      nzContent: templates.terminateWarning
        ? `
      <p>Are you sure you want to terminate <strong>${unitName}</strong>?</p>
      ${templates.terminateWarning}
    `
        : `
      <p>Are you sure you want to disconnect from <strong>${unitName}</strong>?</p>
    `,
      nzOkText: unitType === "local" ? "Disconnect" : "Terminate",
      nzOkType: "primary",
      nzOnOk: () => {
        // Use the ComputingUnitStatusService to handle termination
        // This will properly close the websocket before terminating the unit
        this.computingUnitStatusService
          .terminateComputingUnit(cuid)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: (success: boolean) => {
              if (success) {
                this.notificationService.success(`Terminated Computing Unit: ${unitName}`);
              } else {
                this.notificationService.error("Failed to terminate computing unit");
              }
            },
            error: (err: unknown) => {
              this.notificationService.error(`Failed to terminate computing unit: ${extractErrorMessage(err)}`);
            },
          });
      },
      nzCancelText: "Cancel",
    });
  }

  /**
   * Start editing the name of a computing unit.
   */
  startEditingUnitName(unit: DashboardWorkflowComputingUnit): void {
    if (!unit.isOwner) {
      this.notificationService.error("Only owners can rename computing units");
      return;
    }

    this.editingNameOfUnit = unit.computingUnit.cuid;
    this.editingUnitName = unit.computingUnit.name;

    // Force change detection and focus the input
    this.cdr.detectChanges();
    setTimeout(() => {
      const input = document.querySelector(".unit-name-edit-input") as HTMLInputElement;
      if (input) {
        input.focus();
        input.select();
      }
    }, 0);
  }

  /**
   * Confirm the new name and update the computing unit.
   */
  confirmUpdateUnitName(cuid: number, newName: string): void {
    const trimmedName = newName.trim();

    if (!trimmedName) {
      this.notificationService.error("Computing unit name cannot be empty");
      this.editingNameOfUnit = null;
      return;
    }

    if (trimmedName.length > 128) {
      this.notificationService.error("Computing unit name cannot exceed 128 characters");
      this.editingNameOfUnit = null;
      return;
    }

    this.computingUnitService
      .renameComputingUnit(cuid, trimmedName)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success("Successfully renamed computing unit");
          // Update the local unit name immediately for better UX
          const unit = this.allComputingUnits.find(u => u.computingUnit.cuid === cuid);
          if (unit) {
            unit.computingUnit.name = trimmedName;
          }
          // Also update the selected unit if it's the one being renamed
          if (this.selectedComputingUnit?.computingUnit.cuid === cuid) {
            this.selectedComputingUnit.computingUnit.name = trimmedName;
          }
          // Refresh the computing units list
          this.computingUnitStatusService.refreshComputingUnitList();
        },
        error: (err: unknown) => {
          this.notificationService.error(`Failed to rename computing unit: ${extractErrorMessage(err)}`);
        },
      })
      .add(() => {
        this.editingNameOfUnit = null;
        this.editingUnitName = "";
      });
  }

  /**
   * Cancel editing the computing unit name.
   */
  cancelEditingUnitName(): void {
    this.editingNameOfUnit = null;
    this.editingUnitName = "";
  }

  parseResourceUnit(resource: string): string {
    // check if has a capacity (is a number followed by a unit)
    if (!resource || resource === "NaN") return "NaN";
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
    return this.selectedComputingUnit ? this.selectedComputingUnit.metrics.cpuUsage : "NaN";
  }

  getCurrentComputingUnitMemoryUsage(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.metrics.memoryUsage : "NaN";
  }

  getCurrentComputingUnitCpuLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.computingUnit.resource.cpuLimit : "NaN";
  }

  getCurrentComputingUnitMemoryLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.computingUnit.resource.memoryLimit : "NaN";
  }

  getCurrentComputingUnitGpuLimit(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.computingUnit.resource.gpuLimit : "NaN";
  }

  getCurrentComputingUnitJvmMemorySize(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.computingUnit.resource.jvmMemorySize : "NaN";
  }

  getCurrentSharedMemorySize(): string {
    return this.selectedComputingUnit ? this.selectedComputingUnit.computingUnit.resource.shmSize : "NaN";
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

  getJvmMemorySize(): string {
    return this.getCurrentComputingUnitJvmMemorySize();
  }

  getSharedMemorySize(): string {
    return this.getCurrentSharedMemorySize();
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

  getCreateModalTitle(): string {
    if (!this.selectedComputingUnitType) return "Create Computing Unit";
    return this.unitTypeMessageTemplate[this.selectedComputingUnitType].createTitle;
  }

  public async onClickOpenShareAccess(cuid: number): Promise<void> {
    this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: true,
        type: "computing-unit",
        id: cuid,
        inWorkspace: true,
      },
      nzFooter: null,
      nzTitle: "Share this computing unit with others",
      nzCentered: true,
      nzWidth: "800px",
    });
  }

  onDropdownVisibilityChange(visible: boolean): void {
    if (visible) {
      this.computingUnitStatusService.refreshComputingUnitList();
    }
  }

  unitTypeMessageTemplate = {
    local: {
      createTitle: "Connect to a Local Computing Unit",
      terminateTitle: "Disconnect from Local Computing Unit",
      terminateWarning: "", // no red warning
      createSuccess: "Successfully connected to the local computing unit",
      createFailure: "Failed to connect to the local computing unit",
      terminateSuccess: "Disconnected from the local computing unit",
      terminateFailure: "Failed to disconnect from the local computing unit",
      terminateTooltip: "Disconnect from this computing unit",
    },
    kubernetes: {
      createTitle: "Create Computing Unit",
      terminateTitle: "Terminate Computing Unit",
      terminateWarning:
        "<p style='color: #ff4d4f;'><strong>Warning:</strong> All execution results in this computing unit will be lost.</p>",
      createSuccess: "Successfully created the Kubernetes computing unit",
      createFailure: "Failed to create the Kubernetes computing unit",
      terminateSuccess: "Terminated Kubernetes computing unit",
      terminateFailure: "Failed to terminate Kubernetes computing unit",
      terminateTooltip: "Terminate this computing unit",
    },
  } as const;
}

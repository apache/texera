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

import { ChangeDetectorRef, Component, OnInit, NgZone } from "@angular/core";
import { take } from "rxjs/operators";
import { WorkflowComputingUnitManagingService } from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitType,
} from "../../../common/type/workflow-computing-unit";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { DEFAULT_WORKFLOW, WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { extractErrorMessage } from "../../../common/util/error";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { NzModalService, NzModalComponent, NzModalContentDirective } from "ng-zorro-antd/modal";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ShareAccessComponent } from "../../../dashboard/component/user/share-access/share-access.component";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { ComputingUnitActionsService } from "../../../common/service/computing-unit/computing-unit-actions/computing-unit-actions.service";
import {
  ComputingUnitMetadataComponent,
  parseResourceUnit,
  parseResourceNumber,
  findNearestValidStep,
  unitTypeMessageTemplate,
  cpuResourceConversion,
  memoryResourceConversion,
  cpuPercentage,
  memoryPercentage,
  validateName,
  getComputingUnitBadgeColor,
  getComputingUnitStatusTooltip,
  getComputingUnitCpuStatus,
  getComputingUnitMemoryStatus,
  getComputingUnitCpuLimitUnit,
  isComputingUnitShmTooLarge,
  getJvmMemorySliderConfig,
} from "../../../common/util/computing-unit.util";
import { PvePackageResponse, WorkflowPveService } from "../../service/virtual-environment/virtual-environment.service";
import { NgClass, NgIf, NgFor, DecimalPipe, TitleCasePipe } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzPopoverDirective } from "ng-zorro-antd/popover";
import { NzProgressComponent } from "ng-zorro-antd/progress";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { UserAvatarComponent } from "../../../dashboard/component/user/user-avatar/user-avatar.component";
import { NzBadgeComponent } from "ng-zorro-antd/badge";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzMenuDirective, NzMenuItemComponent, NzMenuDividerDirective } from "ng-zorro-antd/menu";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { FormsModule } from "@angular/forms";
import { NzSliderComponent } from "ng-zorro-antd/slider";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";

type PveDraft = {
  name: string;
  pipOutput: string;
  prettyPipOutput: string;
  expanded: boolean;
  socket?: WebSocket;
  isInstalling: boolean;
  isLocked: boolean;
};

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-selection",
  templateUrl: "./computing-unit-selection.component.html",
  styleUrls: ["./computing-unit-selection.component.scss"],
  imports: [
    NgClass,
    NgIf,
    ɵNzTransitionPatchDirective,
    NzPopoverDirective,
    NzProgressComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    NzDropdownDirective,
    UserAvatarComponent,
    NzBadgeComponent,
    NzTooltipDirective,
    NzIconDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NgFor,
    NzMenuItemComponent,
    NzInputDirective,
    NzMenuDividerDirective,
    NzModalComponent,
    NzSelectComponent,
    FormsModule,
    NzOptionComponent,
    NzSliderComponent,
    NzAlertComponent,
    NzModalContentDirective,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    DecimalPipe,
    TitleCasePipe,
  ],
})
export class ComputingUnitSelectionComponent implements OnInit {
  // variables for creating a virtual environment
  pves: PveDraft[] = [];
  systemPackages: { name: string; version: string }[] = [];
  pveModalVisible = false;

  // current workflow's Id, will change with wid in the workflowActionService.metadata
  protected readonly unitTypeMessageTemplate = unitTypeMessageTemplate;
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
    private cdr: ChangeDetectorRef,
    private computingUnitActionsService: ComputingUnitActionsService,
    private workflowPveService: WorkflowPveService,
    private ngZone: NgZone
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
            "There are ongoing executions on this workflow. Modification of the workflow is currently disabled."
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
   * Calls `selectComputingUnit` when the `wid` changes;
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
    return isComputingUnitShmTooLarge(this.selectedMemory, this.shmSizeValue, this.shmSizeUnit);
  }

  /**
   * Start a new computing unit.
   */
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
        next: (unit: DashboardWorkflowComputingUnit) => {
          this.notificationService.success("Successfully created the new compute unit");
          this.selectComputingUnit(this.workflowId, unit.computingUnit.cuid);
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to start computing unit: ${extractErrorMessage(err)}`),
      });
  }

  openComputingUnitMetadataModal(unit: DashboardWorkflowComputingUnit) {
    this.modalService.create({
      nzTitle: "Computing Unit Information",
      nzContent: ComputingUnitMetadataComponent,
      nzData: unit,
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

    if (!unit) {
      this.notificationService.error("Invalid computing unit.");
      return;
    }

    this.computingUnitActionsService.confirmAndTerminate(cuid, unit);

    if (this.selectedComputingUnit?.computingUnit.type === "local") {
      this.workflowPveService
        .deleteEnvironments(cuid)
        .pipe(untilDestroyed(this))
        .subscribe({
          error: (err: unknown) => {
            console.error("Failed to delete PVE environments", err);
          },
        });
    }
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

    const validationError = validateName(trimmedName);
    if (validationError) {
      this.notificationService.error(validationError);
      this.cancelEditingUnitName();
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
    return getComputingUnitBadgeColor(status);
  }

  getCpuLimit(): number {
    return parseResourceNumber(this.getCurrentComputingUnitCpuLimit());
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
    return getComputingUnitCpuLimitUnit(parseResourceUnit(this.getCurrentComputingUnitCpuLimit()));
  }

  getMemoryLimit(): number {
    return parseResourceNumber(this.getCurrentComputingUnitMemoryLimit());
  }

  getMemoryLimitUnit(): string {
    return parseResourceUnit(this.getCurrentComputingUnitMemoryLimit());
  }

  getCpuValue(): number {
    const usage = this.getCurrentComputingUnitCpuUsage();
    const limit = this.getCurrentComputingUnitCpuLimit();
    if (usage === "N/A" || limit === "N/A") return 0;
    const displayUnit = this.getCpuLimitUnit() === "CPU" ? "" : this.getCpuLimitUnit();
    const usageValue = cpuResourceConversion(usage, displayUnit);
    return parseFloat(usageValue);
  }

  getMemoryValue(): number {
    const usage = this.getCurrentComputingUnitMemoryUsage();
    const limit = this.getCurrentComputingUnitMemoryLimit();
    if (usage === "N/A" || limit === "N/A") return 0;
    const displayUnit = this.getMemoryLimitUnit();
    const usageValue = memoryResourceConversion(usage, displayUnit);
    return parseFloat(usageValue);
  }

  getCpuPercentage(): number {
    return cpuPercentage(this.getCurrentComputingUnitCpuUsage(), this.getCurrentComputingUnitCpuLimit());
  }

  getMemoryPercentage(): number {
    return memoryPercentage(this.getCurrentComputingUnitMemoryUsage(), this.getCurrentComputingUnitMemoryLimit());
  }

  getCpuStatus(): "success" | "exception" | "active" | "normal" {
    return getComputingUnitCpuStatus(this.getCpuPercentage());
  }

  getMemoryStatus(): "success" | "exception" | "active" | "normal" {
    return getComputingUnitMemoryStatus(this.getMemoryPercentage());
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
    return getComputingUnitStatusTooltip(unit);
  }

  // Called when the component initializes
  updateJvmMemorySlider(): void {
    this.resetJvmMemorySlider();
  }

  onJvmMemorySliderChange(value: number): void {
    // Ensure the value is one of the valid steps
    const validStep = findNearestValidStep(value, this.jvmMemorySteps);
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
    const config = getJvmMemorySliderConfig(this.selectedMemory);

    this.jvmMemoryMax = config.jvmMemoryMax;
    this.showJvmMemorySlider = config.showJvmMemorySlider;
    this.jvmMemorySteps = config.jvmMemorySteps;
    this.jvmMemoryMarks = config.jvmMemoryMarks;
    this.jvmMemorySliderValue = config.jvmMemorySliderValue;
    this.selectedJvmMemorySize = config.selectedJvmMemorySize;
  }

  // Listen for memory selection changes
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

  public async onClickOpenShareAccess(cuid: number): Promise<void> {
    this.computingUnitActionsService.openShareAccessModal(cuid, true);
  }

  onDropdownVisibilityChange(visible: boolean): void {
    if (visible) {
      this.computingUnitStatusService.refreshComputingUnitList();
    }
  }

  trackByIndex(index: number): number {
    return index;
  }

  addEnvironment(): void {
    this.pves.push({
      name: "",
      pipOutput: "",
      prettyPipOutput: "",
      expanded: true,
      isInstalling: false,
      isLocked: false,
    });
  }

  showPVEmodalVisible(): void {
    this.pveModalVisible = true;
    this.getPVEs();
  }

  closePveModal(): void {
    this.pves.forEach(pve => {
      pve.socket?.close();
      pve.socket = undefined;
      pve.isInstalling = false;
    });

    this.pveModalVisible = false;
  }

  getPVEs(): void {
    const cuId = this.selectedComputingUnit!.computingUnit.cuid;

    this.workflowPveService
      .fetchPVEs(cuId)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (resp: PvePackageResponse[]) => {
          this.pves = resp.map(pve => ({
            name: pve.pveName,
            expanded: false,
            isInstalling: false,
            pipOutput: "",
            prettyPipOutput: "",
            isLocked: true,
          }));

          this.workflowPveService
            .getSystemPackages()
            .pipe(untilDestroyed(this))
            .subscribe({
              next: installedResp => {
                this.systemPackages = installedResp.system.map(pkgStr => {
                  const [name, version] = pkgStr.split("==");
                  return {
                    name: name.trim(),
                    version: (version ?? "").trim(),
                  };
                });
              },
              error: (err: unknown) => {
                console.error("Failed to fetch system packages:", err);
                this.systemPackages = [];
              },
            });
        },
        error: (err: unknown) => {
          console.error("Failed to fetch PVEs:", err);
          this.pves = [];
          this.systemPackages = [];
        },
      });
  }

  scrollToBottomOfPipModal(index: number) {
    setTimeout(() => {
      const pre = document.getElementById(`pip-log-${index}`) as HTMLElement | null;
      if (pre) {
        pre.scrollTop = pre.scrollHeight;
      }
    }, 50);
  }

  // Converts raw pip output for UI rendering by escaping unsafe characters and
  // applying styling to exit codes, errors, warnings, and common success messages.
  updatePrettyPipOutput(index: number) {
    const env = this.pves[index];

    const escapeHtml = (s: string) =>
      s
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");

    const raw = env.pipOutput ?? "";
    const safe = escapeHtml(raw);

    env.prettyPipOutput = safe
      .replace(/^(\[pip\] Successfully installed.*)$/gm, '<span class="pip-exit ok"><strong>$1</strong></span>')

      .replace(
        /^(\[(?:PVE|pip|pve)\].*finished with exit code\s+0.*)$/gm,
        '<span class="pip-exit ok"><strong>$1</strong></span>'
      )

      .replace(/^(\[PVE\] Running pip freeze.*)$/gm, '<span class="pip-exit ok"><strong>$1</strong></span>')

      .replace(/^(\[(?:PVE|pip|pve)\]\[ERR\].*)$/gm, '<span class="pip-exit err"><strong>$1</strong></span>')

      .replace(/\n/g, "<br/>");
  }

  createVirtualEnvironment(index: number): void {
    const cuId = this.selectedComputingUnit!.computingUnit.cuid;

    const env = this.pves[index];

    const trimmedName = env.name.trim();

    if (!/^[a-zA-Z0-9]+$/.test(trimmedName)) {
      this.notificationService.error("Environment name must contain only letters and numbers.");
      return;
    }

    const duplicateExists = this.pves.some((pve, i) => i !== index && (pve.name ?? "").trim() === trimmedName);

    if (duplicateExists) {
      this.notificationService.error("An environment with this name already exists.");
      return;
    }

    const packageArray: string[] = [];

    env.socket?.close();

    const isLocal = this.selectedComputingUnit?.computingUnit.type === "local";

    const websocketUrl = this.workflowPveService.createPveWebSocketUrl(cuId, trimmedName, isLocal, packageArray);
    console.log("PVE websocketUrl", websocketUrl);
    const socket = new WebSocket(websocketUrl);

    this.pves[index] = {
      ...env,
      name: trimmedName,
      socket,
      pipOutput: "Starting ...\n",
      isInstalling: true,
      isLocked: true,
    };

    this.updatePrettyPipOutput(index);
    this.scrollToBottomOfPipModal(index);

    socket.onmessage = event => {
      console.log("PVE WS received:", event.data);

      this.ngZone.run(() => {
        const currentEnv = this.pves[index];

        if (event.data === "__DONE__") {
          this.pves[index] = {
            ...currentEnv,
            socket: undefined,
            isInstalling: false,
            isLocked: true,
          };

          socket.close();
          this.workflowPveService
            .getSystemPackages()
            .pipe(untilDestroyed(this))
            .subscribe({
              next: resp => {
                this.systemPackages = resp.system.map(pkg => {
                  const [name, version] = pkg.split("==");
                  return { name: name.trim(), version: (version ?? "").trim() };
                });
                this.cdr.detectChanges();
              },
              error: (e: unknown) => console.error("Failed to refresh packages", e),
            });

          this.cdr.detectChanges();
          return;
        }

        this.pves[index] = {
          ...currentEnv,
          pipOutput: `${currentEnv.pipOutput ?? ""}${event.data}\n`,
        };

        this.updatePrettyPipOutput(index);
        this.scrollToBottomOfPipModal(index);
        this.cdr.detectChanges();
      });
    };

    socket.onerror = err => {
      console.log("PVE WS error", err);

      this.ngZone.run(() => {
        const currentEnv = this.pves[index];

        this.pves[index] = {
          ...currentEnv,
          pipOutput: `${currentEnv.pipOutput ?? ""}\n[WebSocket error]\n`,
          socket: undefined,
          isInstalling: false,
          isLocked: true,
        };

        socket.close();
        this.updatePrettyPipOutput(index);
        this.cdr.detectChanges();
      });
    };

    socket.onclose = event => {
      console.log("PVE WS closed", {
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      });
    };
  }
}

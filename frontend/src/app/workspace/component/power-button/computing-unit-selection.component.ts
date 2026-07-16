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

import { ChangeDetectorRef, Component, OnInit, NgZone, ViewChild } from "@angular/core";
import { take } from "rxjs/operators";
import { WorkflowComputingUnitManagingService } from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
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
} from "../../../common/util/computing-unit.util";
import {
  PvePackageResponse,
  UserPveRecord,
  WorkflowPveService,
} from "../../service/virtual-environment/virtual-environment.service";
import { NgClass, NgIf, NgFor, DecimalPipe } from "@angular/common";
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
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { ComputingUnitCreateModalComponent } from "../../../common/component/computing-unit-create-modal/computing-unit-create-modal.component";

type PveUserPackageRow = {
  name: string;
  versionOp?: "==" | ">=" | "<=";
  version?: string;
  deleteToggle?: boolean;
};

type PveDraft = {
  name: string;
  userPackages: PveUserPackageRow[];
  newPackages: PveUserPackageRow[];
  deletingPackages: { name: string; version: string }[];
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
    NzModalContentDirective,
    NzCollapseComponent,
    NzCollapsePanelComponent,
    DecimalPipe,
    ComputingUnitCreateModalComponent,
  ],
})
export class ComputingUnitSelectionComponent implements OnInit {
  // variables for creating a virtual environment
  pves: PveDraft[] = [];
  systemPackages: { name: string; version: string }[] = [];
  // True while an /api/pve/system response is in flight. The server resolves
  // the full pinned set with a `pip freeze` against a throwaway venv,
  // which can take 30–60s on the first request after a server restart.
  systemPackagesLoading = false;
  pveModalVisible = false;

  // Saved PVE specs (name + packages) the user defined in the Python Venv
  // dashboard. Fetched whenever the CU PVE modal opens so the user can pick
  // one and have its packages installed into the active CU.
  availableDbPves: UserPveRecord[] = [];

  // current workflow's Id, will change with wid in the workflowActionService.metadata
  protected readonly unitTypeMessageTemplate = unitTypeMessageTemplate;
  workflowId: number | undefined;

  lastSelectedCuid?: number;
  selectedComputingUnit: DashboardWorkflowComputingUnit | null = null;
  allComputingUnits: DashboardWorkflowComputingUnit[] = [];

  // visibility of the shared create-computing-unit modal
  addComputeUnitModalVisible = false;

  @ViewChild(ComputingUnitCreateModalComponent)
  private computingUnitCreateModal?: ComputingUnitCreateModalComponent;

  // variables for renaming a computing unit
  editingNameOfUnit: number | null = null;
  editingUnitName: string = "";

  // GPU limit options, used by the metrics popover's GPU row via showGpuSelection()
  gpuOptions: string[] = [];
  // True when the limit-options fetch failed; showGpuSelection() then falls back
  // to permissive so the metrics popover doesn't silently hide the GPU row.
  private gpuOptionsFetchFailed = false;

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
    // GPU options drive the GPU row in the metrics popover. The shared
    // create modal fetches these options itself and owns the user-facing
    // error toast for this endpoint, so on failure this only logs and falls
    // back to showing GPU metrics based on the unit's own allocation.
    this.computingUnitService
      .getComputingUnitLimitOptions()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ gpuLimitOptions }) => {
          this.gpuOptions = gpuLimitOptions;
          this.gpuOptionsFetchFailed = false;
        },
        error: (err: unknown) => {
          this.gpuOptionsFetchFailed = true;
          console.error("Failed to fetch computing unit limit options for the GPU metric row", err);
        },
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
    // If the options fetch failed, err on the side of showing the GPU row —
    // the metrics template additionally requires the unit's own GPU limit to
    // be non-zero, so this cannot show a GPU row for a GPU-less unit.
    if (this.gpuOptionsFetchFailed) {
      return true;
    }
    // Don't show GPU selection if there are no options or only "0" option
    return this.gpuOptions.length > 1 || (this.gpuOptions.length === 1 && this.gpuOptions[0] !== "0");
  }

  showAddComputeUnitModalVisible(defaultName?: string): void {
    if (defaultName !== undefined && this.computingUnitCreateModal) {
      this.computingUnitCreateModal.newComputingUnitName = defaultName;
    }
    this.addComputeUnitModalVisible = true;
  }

  onComputingUnitCreated(unit: DashboardWorkflowComputingUnit): void {
    this.selectComputingUnit(this.workflowId, unit.computingUnit.cuid);
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

  addPackage(index: number): void {
    const env = this.pves[index];
    env.newPackages.push({ name: "", version: "", versionOp: undefined, deleteToggle: false });
  }

  togglePackageDelete(index: number, pkg: PveUserPackageRow): void {
    const env = this.pves[index];

    pkg.deleteToggle = !pkg.deleteToggle;

    const version = pkg.version ?? "";

    env.deletingPackages = env.deletingPackages.filter(p => !(p.name === pkg.name && p.version === version));

    if (pkg.deleteToggle) {
      env.deletingPackages.push({ name: pkg.name, version });
    }
  }

  showPVEmodalVisible(): void {
    this.pveModalVisible = true;
    this.getPVEs();
    this.refreshAvailableDbPves();
  }

  isSavedPveInstalledInCu(name: string): boolean {
    const trimmed = name.trim();
    return this.pves.some(p => p.isLocked && p.name.trim() === trimmed);
  }

  /**
   * Whether the per-environment "OK" (create/install) button should be
   * disabled: true until the environment name has non-whitespace content.
   */
  isCreateDisabled(pve: PveDraft): boolean {
    return !pve.name.trim();
  }

  private refreshAvailableDbPves(): void {
    this.workflowPveService
      .listUserPves()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: records => {
          this.availableDbPves = records;
        },
        error: (err: unknown) => {
          console.error("Failed to fetch saved Python environments", err);
          this.availableDbPves = [];
        },
      });
  }

  // Triggered when the user picks a saved PVE in the picker. Builds a new
  // env card from its name + packages and starts CU install flow
  // (createVirtualEnvironment), so pip output streams into the same panel.
  installFromSavedPve(veid: number): void {
    const saved = this.availableDbPves.find(p => p.veid === veid);
    if (!saved) return;

    const trimmedName = saved.name.trim();
    const dbRows = this.parseDbPackages(saved.packages);

    const existingIndex = this.pves.findIndex(p => p.isLocked && p.name.trim() === trimmedName);
    if (existingIndex !== -1) {
      this.applySavedPveAsUpdate(existingIndex, saved.name, dbRows);
      return;
    }

    this.pves.push({
      name: saved.name,
      userPackages: [],
      newPackages: dbRows,
      deletingPackages: [],
      pipOutput: "",
      prettyPipOutput: "",
      expanded: true,
      isInstalling: false,
      isLocked: false,
    });

    const newIndex = this.pves.length - 1;

    setTimeout(() => this.createVirtualEnvironment(newIndex), 0);
  }

  private parseDbPackages(packages: Record<string, string> | null | undefined): PveUserPackageRow[] {
    return Object.entries(packages ?? {}).map(([name, raw]) => {
      const match = raw?.match?.(/^(==|>=|<=)(.*)$/);
      return {
        name,
        versionOp: (match ? match[1] : "==") as "==" | ">=" | "<=",
        version: match ? match[2] : raw ?? "",
      };
    });
  }

  // Computes the diff between the saved DB record and the locked card's
  // current user packages, then triggers the existing update path
  private applySavedPveAsUpdate(index: number, displayName: string, dbRows: PveUserPackageRow[]): void {
    const existing = this.pves[index];

    const dbByName = new Map(dbRows.map(p => [p.name.trim().toLowerCase(), p]));
    const existingByName = new Map(existing.userPackages.map(p => [p.name.trim().toLowerCase(), p]));

    const toInstall: PveUserPackageRow[] = [];
    const toDelete: { name: string; version: string }[] = [];

    dbByName.forEach((db, key) => {
      const cur = existingByName.get(key);
      if (!cur) {
        toInstall.push({ name: db.name, versionOp: db.versionOp, version: db.version });
      } else if ((cur.version ?? "").trim() !== (db.version ?? "").trim()) {
        toDelete.push({ name: cur.name, version: (cur.version ?? "").trim() });
        toInstall.push({ name: db.name, versionOp: db.versionOp, version: db.version });
      }
    });

    existingByName.forEach((cur, key) => {
      if (!dbByName.has(key)) {
        toDelete.push({ name: cur.name, version: (cur.version ?? "").trim() });
      }
    });

    if (toInstall.length === 0 && toDelete.length === 0) {
      this.notificationService.success(`"${displayName}" is already up to date in this computing unit.`);
      return;
    }

    const deletingKeys = new Set(toDelete.map(p => p.name.trim().toLowerCase()));
    existing.userPackages = existing.userPackages.filter(p => !deletingKeys.has(p.name.trim().toLowerCase()));
    existing.newPackages = toInstall;
    existing.deletingPackages = toDelete;
    existing.expanded = true;

    setTimeout(() => this.createVirtualEnvironment(index), 0);
  }

  closePveModal(): void {
    this.pves.forEach(pve => {
      pve.socket?.close();
      pve.socket = undefined;
      pve.isInstalling = false;
    });

    this.availableDbPves = [];
    this.pveModalVisible = false;
  }

  getPVEs(): void {
    const cuId = this.selectedComputingUnit!.computingUnit.cuid;
    this.systemPackagesLoading = true;

    this.workflowPveService
      .fetchPVEs(cuId)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (resp: PvePackageResponse[]) => {
          this.pves = resp.map(pve => ({
            name: pve.pveName,
            userPackages: this.parsePackageRows(pve.userPackages),
            newPackages: [],
            deletingPackages: [],
            expanded: false,
            isInstalling: false,
            pipOutput: "",
            prettyPipOutput: "",
            isLocked: true,
          }));

          this.workflowPveService
            .getSystemPackages(cuId)
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
                this.systemPackagesLoading = false;
              },
              error: (err: unknown) => {
                console.error("Failed to fetch system packages:", err);
                this.systemPackages = [];
                this.systemPackagesLoading = false;
              },
            });
        },
        error: (err: unknown) => {
          console.error("Failed to fetch PVEs:", err);
          this.pves = [];
          this.systemPackages = [];
          this.systemPackagesLoading = false;
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

      .replace(/^(\[PVE\] Skipped.*)$/gm, '<span class="pip-exit err"><strong>$1</strong></span>')

      .replace(/\n/g, "<br/>");
  }

  private runPveWebSocket(
    index: number,
    action: "create" | "install",
    initialMessage: string,
    packages: string[] = [],
    onDone?: () => void
  ): void {
    const cuId = this.selectedComputingUnit!.computingUnit.cuid;
    const env = this.pves[index];
    const trimmedName = env.name.trim();

    env.socket?.close();

    const websocketUrl = this.workflowPveService.getPveWebSocketUrl(cuId, trimmedName, action, packages);

    const socket = new WebSocket(websocketUrl);

    this.pves[index] = {
      ...env,
      name: trimmedName,
      socket,
      pipOutput: initialMessage,
      isInstalling: true,
      isLocked: true,
    };

    this.updatePrettyPipOutput(index);
    this.scrollToBottomOfPipModal(index);

    socket.onmessage = event => {
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
          onDone?.();

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

    socket.onerror = () => {
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
  }

  private refreshUserPackages(index: number): void {
    const env = this.pves[index];

    this.workflowPveService
      .getUserPackages(this.selectedComputingUnit!.computingUnit.cuid, env.name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: pkgs => {
          env.userPackages = env.userPackages = this.parsePackageRows(pkgs);
          this.cdr.detectChanges();
        },
        error: (e: unknown) => console.error("Failed to refresh user packages", e),
      });
  }

  createVirtualEnvironment(index: number): void {
    const env = this.pves[index];
    const trimmedName = env.name.trim();

    if (!/^[a-zA-Z0-9]+$/.test(trimmedName)) {
      this.notificationService.error("Environment name must contain only letters and numbers.");
      return;
    }

    if (env.isLocked) {
      this.deleteUserPackages(index, () => {
        this.installUserPackages(index);
      });
      return;
    }

    const duplicateExists = this.pves.some((pve, i) => i !== index && (pve.name ?? "").trim() === trimmedName);

    if (duplicateExists) {
      this.notificationService.error("An environment with this name already exists.");
      return;
    }

    this.runPveWebSocket(index, "create", "Creating virtual environment...\n", [], () => {
      this.deleteUserPackages(index, () => {
        this.installUserPackages(index);
      });
    });
  }

  private installUserPackages(index: number): void {
    const env = this.pves[index];

    const missingVersionPackage = env.newPackages?.find(
      pkg => pkg.name?.trim() && (!pkg.versionOp?.trim() || !pkg.version?.trim())
    );

    if (missingVersionPackage) {
      this.notificationService.error("Please specify an operator and version for each package.");
      return;
    }

    const systemPackageNames = new Set(this.systemPackages.map(pkg => pkg.name.trim().toLowerCase()));

    const userPackageNames = new Set(env.userPackages.map(pkg => pkg.name.trim().toLowerCase()));

    const skippedMessages: string[] = [];

    const packageArray =
      env.newPackages
        ?.filter(pkg => pkg.name?.trim())
        .filter(pkg => {
          const packageName = pkg.name.trim().toLowerCase();

          if (systemPackageNames.has(packageName)) {
            this.notificationService.error(`Skipped ${pkg.name}: already installed as a system package.`);
            return false;
          }

          if (userPackageNames.has(packageName)) {
            this.notificationService.error(`Skipped ${pkg.name}: already installed in this environment.`);
            return false;
          }

          return true;
        })
        .map(pkg => `${pkg.name.trim()}${pkg.versionOp}${(pkg.version ?? "").trim()}`) ?? [];

    if (skippedMessages.length > 0) {
      this.pves[index].pipOutput = `${this.pves[index].pipOutput ?? ""}` + skippedMessages.join("\n") + "\n";

      this.updatePrettyPipOutput(index);
      this.scrollToBottomOfPipModal(index);
    }

    if (packageArray.length === 0) {
      this.pves[index].newPackages = [];
      this.pves[index].isInstalling = false;
      this.refreshUserPackages(index);
      return;
    }

    this.runPveWebSocket(index, "install", "Installing user packages...\n", packageArray, () => {
      this.pves[index].newPackages = [];
      this.refreshUserPackages(index);
    });
  }

  private parsePackageRows(packages: string[]): PveUserPackageRow[] {
    return packages.map(pkgStr => {
      const [name, version] = pkgStr.split("==");
      return {
        name: name.trim(),
        versionOp: "==" as const,
        version: (version ?? "").trim(),
      };
    });
  }

  private deleteUserPackages(index: number, onDone?: () => void): void {
    const cuId = this.selectedComputingUnit!.computingUnit.cuid;
    const pveName = this.pves[index].name.trim();
    const packagesToDelete = [...this.pves[index].deletingPackages];

    if (packagesToDelete.length === 0) {
      onDone?.();
      return;
    }

    this.pves[index] = {
      ...this.pves[index],
      pipOutput: `${this.pves[index].pipOutput ?? ""}Deleting user packages...\n`,
      isInstalling: true,
    };

    let deleteIndex = 0;

    const deleteNext = (): void => {
      if (deleteIndex >= packagesToDelete.length) {
        this.pves[index].deletingPackages = [];
        this.refreshUserPackages(index);
        onDone?.();
        return;
      }

      const pkg = packagesToDelete[deleteIndex];

      this.workflowPveService
        .deletePackage(cuId, pveName, pkg.name)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: messages => {
            this.pves[index].pipOutput = `${this.pves[index].pipOutput ?? ""}${messages.join("\n")}\n`;

            this.updatePrettyPipOutput(index);
            this.scrollToBottomOfPipModal(index);

            deleteIndex++;
            deleteNext();
          },
          error: () => {
            this.pves[index].pipOutput =
              `${this.pves[index].pipOutput ?? ""}[PVE][ERR] Failed to delete package: ${pkg.name}\n`;

            this.updatePrettyPipOutput(index);
            this.scrollToBottomOfPipModal(index);

            deleteIndex++;
            deleteNext();
          },
        });
    };

    deleteNext();
  }
}

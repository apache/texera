import { Component, OnInit } from "@angular/core";
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

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-selection",
  templateUrl: "./computing-unit-selection.component.html",
  styleUrls: ["./computing-unit-selection.component.scss"],
})
export class ComputingUnitSelectionComponent implements OnInit {
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

  // cpu&memory limit options from backend
  cpuOptions: string[] = [];
  memoryOptions: string[] = [];

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {}

  ngOnInit(): void {
    if (environment.computingUnitManagerEnabled) {
      this.computingUnitService
        .getComputingUnitLimitOptions()
        .pipe(untilDestroyed(this))
        .subscribe({
          next: ({ cpuLimitOptions, memoryLimitOptions }) => {
            this.cpuOptions = cpuLimitOptions;
            this.memoryOptions = memoryLimitOptions;

            // fallback defaults
            this.selectedCpu = this.cpuOptions[0] ?? "1";
            this.selectedMemory = this.memoryOptions[0] ?? "1Gi";
          },
          error: (err: unknown) =>
            this.notificationService.error(`Failed to fetch CPU/memory options: ${extractErrorMessage(err)}`),
        });
    }
    
    // Subscribe to the current selected unit from the status service
    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
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
    console.log("Selected computing unit changed to:", computingUnit);
    if (computingUnit && isDefined(this.workflowId)) {
      console.log(`Selected Unit URI: ${computingUnit.uri}`);
      
      // First update the selection in the status service
      this.computingUnitStatusService.selectComputingUnit(computingUnit);
      
      // Then open the websocket connection
      this.workflowWebsocketService.closeWebsocket();
      this.workflowWebsocketService.openWebsocket(this.workflowId, undefined, computingUnit.computingUnit.cuid);
    } else {
      console.log("Selection cleared.");
    }
  }

  isComputingUnitRunning(): boolean {
    return this.selectedComputingUnit != null && this.selectedComputingUnit.status === "Running";
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

  cannotSelectUnit(unit: DashboardWorkflowComputingUnit): boolean {
    return unit.status !== "Running";
  }

  isSelectedUnit(unit: DashboardWorkflowComputingUnit): boolean {
    return unit.uri === this.selectedComputingUnit?.uri;
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
    this.computingUnitService
      .createComputingUnit(computeUnitName, computeCPU, computeMemory)
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
    const uri = this.computingUnits.find(unit => unit.computingUnit.cuid === cuid)?.uri;

    if (!uri) {
      this.notificationService.error("Invalid computing unit URI.");
      return;
    }

    this.computingUnitService
      .terminateComputingUnit(cuid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (res: Response) => {
          this.notificationService.success(`Terminated ${this.getComputingUnitId(uri)}`);
          
          // Clear the selection if we terminated the selected unit
          if (this.selectedComputingUnit?.computingUnit.cuid === cuid) {
            this.selectedComputingUnit = null;
            
            // Find another running unit to select
            const runningUnit = this.computingUnits.find(unit => 
              unit.computingUnit.cuid !== cuid && unit.status === "Running"
            );
            
            if (runningUnit) {
              this.connectToComputingUnit(runningUnit);
            }
          }
        },
        error: (err: unknown) =>
          this.notificationService.error(`Failed to terminate computing unit: ${extractErrorMessage(err)}`),
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
    const fromScaled = fromNumber * cpuScales[fromUnit];
    const toScaled = fromScaled / cpuScales[toUnit as CpuUnit];

    // For display purposes, round to 2 decimal places for normal units, whole numbers for smaller units
    if (toUnit === "") {
      return toScaled.toFixed(2);
    } else {
      return Math.round(toScaled).toString();
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
    const fromScaled = fromNumber * memoryScales[fromUnit];
    const toScaled = fromScaled / memoryScales[toUnit as MemoryUnit];
    return Math.round(toScaled).toString();
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
    return Math.min(
      (this.parseResourceNumber(usage) / this.parseResourceNumber(limit)) * 100,
      100
    );
  }

  getCpuStatus(): "success" | "exception" | "active" | "normal" {
    const percentage = this.getCpuPercentage();
    if (percentage > 90) return "exception";
    if (percentage > 50) return "normal";
    return "success";
  }

  getMemoryPercentage(): number {
    const usage = this.getCurrentComputingUnitMemoryUsage();
    const limit = this.getCurrentComputingUnitMemoryLimit();
    if (usage === "N/A" || limit === "N/A") return 0;
    return Math.min(
      (this.parseResourceNumber(usage) / this.parseResourceNumber(limit)) * 100,
      100
    );
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

  protected readonly environment = environment;
}

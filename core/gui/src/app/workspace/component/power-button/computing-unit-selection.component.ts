import { Component, Input, OnInit } from "@angular/core";
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

  addComputeUnitModalVisible = false;
  selectedMemory: string = "2Gi";
  selectedCpu: string = "2";

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    this.computingUnitService
      .listComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (units: DashboardWorkflowComputingUnit[]) => {
          let firstRunningUnit = units.find(unit => unit.status === "Running");
          if (firstRunningUnit) {
            this.selectedComputingUnit = firstRunningUnit;
            this.connectToComputingUnit(firstRunningUnit);
          }
          this.updateComputingUnits(units);
          this.refreshComputingUnits();
        },
        error: (err: unknown) => console.error("Failed to fetch computing units:", err),
      });

    this.registerWorkflowMetadataSubscription();
  }

  /**
   * Periodically refresh the list of computing units.
   */
  private refreshComputingUnits(): void {
    interval(this.REFRESH_INTERVAL_MS)
      .pipe(
        switchMap(() => this.computingUnitService.listComputingUnits()),
        untilDestroyed(this)
      )
      .subscribe({
        next: (units: DashboardWorkflowComputingUnit[]) => this.updateComputingUnits(units),
        error: (err: unknown) => console.error("Failed to fetch computing units:", err),
      });
  }

  /**
   * Update the computing units list, maintaining object references for the same CUID.
   */
  private updateComputingUnits(newUnits: DashboardWorkflowComputingUnit[]): void {
    const unitMap = new Map(this.computingUnits.map(unit => [unit.computingUnit.cuid, unit]));

    this.computingUnits = newUnits.map(newUnit =>
      unitMap.has(newUnit.computingUnit.cuid)
        ? Object.assign(unitMap.get(newUnit.computingUnit.cuid)!, newUnit)
        : newUnit
    );

    // If selected computing unit is removed, deselect it
    if (
      this.selectedComputingUnit &&
      !this.computingUnits.some(unit => unit.computingUnit.cuid === this.selectedComputingUnit!.computingUnit.cuid)
    ) {
      this.selectedComputingUnit = null;
    }
  }

  /**
   * Start a new computing unit.
   */
  startComputingUnit(): void {
    const computeUnitName = `Compute for Workflow ${this.workflowId}`;
    const computeCPU = this.selectedCpu;
    const computeMemory = this.selectedMemory;
    this.computingUnitService
      .createComputingUnit(computeUnitName, computeCPU, computeMemory)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (unit: DashboardWorkflowComputingUnit) => {
          this.notificationService.success("Successfully created the new compute unit");
          this.refreshComputingUnits();
        },
        error: (err: unknown) => this.notificationService.error("Failed to start computing unit"),
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
      .terminateComputingUnit(uri)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (res: Response) => {
          this.notificationService.success(`Terminated ${this.getComputingUnitName(uri)}`);
          this.refreshComputingUnits();
        },
        error: (err: unknown) => this.notificationService.error("Failed to terminate computing unit"),
      });
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
    switch (this.selectedComputingUnit.status) {
      case "Running":
        return "success";
      case "Pending" || "Terminating":
        return "warning";
      default:
        return "error";
    }
  }

  cannotSelectUnit(unit: DashboardWorkflowComputingUnit): boolean {
    return !unit || unit.status !== "Running";
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
  getComputingUnitName(unitURI: string): string {
    const computingUnit = unitURI.split(".")[0];
    return computingUnit
      .split("-")
      .map((word, index) => (index < 2 ? word.charAt(0).toUpperCase() + word.slice(1) : word))
      .join(" ");
  }

  /**
   * Parses computing units resource unit
   * @param resource (i.e. "12412512n")
   * @return associated unit with resource (i.e. "n", "Mi", "Gi", ...)
   */
  parseResourceUnit(resource: string): string {
    const match = resource.match(/[a-z].*/i);
    return match ? match[0] : "";
  }

  /**
   * Parses computing units numerical value
   * @param resource (i.e. "12412512n")
   * @return associated number with resource (i.e. 12412512)
   */
  parseResourceNumber(resource: string): number {
    const match = resource.match(/[0-9.]*/);
    return match ? Number(match[0]) : 0;
  }

  /**
   * Convert computing cpu unit resource number to a specific unit
   * @param from (i.e. "12412512n")
   * @param toUnit (i.e. cores)
   * @return i.e. 1.2412512 Cores
   */
  cpuResourceConversion(from: string, toUnit: string): string {
    // CPU conversion constants (base unit: nanocores)
    type CpuUnit = "n" | "u" | "m" | "";
    const cpuUnits: Record<CpuUnit, number> = {
      n: 1, // nanocores
      u: 10 ** 3, // microcores
      m: 10 ** 6, // millicores
      "": 10 ** 9, // cores
    };

    const fromNumber: number = this.parseResourceNumber(from);
    const fromUnit: string = this.parseResourceUnit(from);

    if (!(fromUnit in cpuUnits) || !(toUnit in cpuUnits)) {
      return "";
    }
    return `${fromNumber * (cpuUnits[fromUnit as CpuUnit] / cpuUnits[toUnit as CpuUnit])} ${toUnit}`;
  }

  /**
   * Convert computing unit memory resource number to a specific unit
   * @param from (i.e. "523Mi")
   * @param toUnit (i.e. "Gi")
   * @return i.e. 0.524 Gi
   */
  memoryResourceConversion(from: string, toUnit: string): string {
    // Memory conversion constants (base unit: bytes)
    type MemoryUnit = "Ki" | "Mi" | "Gi" | "";
    const memoryUnits = {
      "": 1, // bytes
      Ki: 1024, // KiB
      Mi: 1024 ** 2, // MiB
      Gi: 1024 ** 3, // GiB
    };

    const fromNumber: number = this.parseResourceNumber(from);
    const fromUnit: string = this.parseResourceUnit(from);

    if (!(fromUnit in memoryUnits) || !(toUnit in memoryUnits)) {
      return "";
    }
    return `${fromNumber * (memoryUnits[fromUnit as MemoryUnit] / memoryUnits[toUnit as MemoryUnit])} ${toUnit}`;
  }

  getCurrentComputingUnitCpuUsage(): string {
    return this.selectedComputingUnit?.metrics.cpuUsage || "";
  }

  getCurrentComputingUnitMemoryUsage(): string {
    return this.selectedComputingUnit?.metrics.memoryUsage || "";
  }

  getCurrentComputingUnitCpuLimit(): string {
    return this.selectedComputingUnit?.resourceLimits.cpuLimit || "";
  }

  getCurrentComputingUnitMemoryLimit(): string {
    return this.selectedComputingUnit?.resourceLimits.memoryLimit || "";
  }

  /**
   * Get badge color based on the unit's status.
   */
  getBadgeColor(status: string): string {
    return status === "Running" ? "green" : "yellow";
  }

  getCpuLimit(): number {
    return this.parseResourceNumber(this.getCurrentComputingUnitCpuLimit());
  }

  getCpuLimitUnit(): string {
    let unit = this.parseResourceUnit(this.getCurrentComputingUnitCpuLimit());
    if (!unit) {
      return this.getCpuLimit() == 1 ? "Core" : "Cores";
    }
    return this.parseResourceUnit(this.getCurrentComputingUnitCpuLimit());
  }

  getMemoryLimit(): number {
    return this.parseResourceNumber(this.getCurrentComputingUnitMemoryLimit());
  }

  getMemoryLimitUnit(): string {
    return this.parseResourceUnit(this.getCurrentComputingUnitMemoryLimit());
  }

  getCpuValue(): number {
    // convert to appropriate unit based on the limit unit
    const cpuLimitUnit: string = this.getCpuLimitUnit();
    const convertedValue: string = this.cpuResourceConversion(this.getCurrentComputingUnitCpuUsage(), cpuLimitUnit);
    return this.parseResourceNumber(convertedValue);
  }

  getMemoryValue(): number {
    // convert to appropriate unit based on the limit
    const memoryLimitUnit: string = this.getMemoryLimitUnit();
    const convertedValue: string = this.memoryResourceConversion(
      this.getCurrentComputingUnitMemoryUsage(),
      memoryLimitUnit
    );
    return this.parseResourceNumber(convertedValue);
  }

  getCpuPercentage(): number {
    // handle divison by zero
    const cpuLimit = this.getCpuLimit();
    if (cpuLimit <= 0) {
      return 0;
    }
    return (this.getCpuValue() / cpuLimit) * 100;
  }

  getCpuStatus(): "success" | "exception" | "active" | "normal" {
    const usage = this.getCpuValue();
    const limit = this.getCpuLimit();
    return usage >= limit ? "exception" : "active";
  }

  getMemoryPercentage(): number {
    // handle divison by zero
    const memoryLimit = this.getMemoryLimit();
    if (memoryLimit <= 0) {
      return 0;
    }
    return (this.getMemoryValue() / memoryLimit) * 100;
  }

  getMemoryStatus(): "success" | "exception" | "active" | "normal" {
    const usage = this.getMemoryValue();
    const limit = this.getMemoryLimit();
    return usage >= limit ? "exception" : "active";
  }

  getCpuUnit(): string {
    return this.parseResourceUnit(this.getCurrentComputingUnitCpuUsage());
  }

  getMemoryUnit(): string {
    return this.parseResourceUnit(this.getCurrentComputingUnitMemoryUsage());
  }

  protected readonly environment = environment;
}

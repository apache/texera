import { Component, Input, OnInit } from "@angular/core";
import { interval } from "rxjs";
import {map, switchMap} from "rxjs/operators";
import { WorkflowComputingUnitManagingService } from "../../service/workflow-computing-unit/workflow-computing-unit-managing.service";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-computing-unit-selection",
  templateUrl: "./computing-unit-selection.component.html",
  styleUrls: ["./computing-unit-selection.component.scss"],
})
export class ComputingUnitSelectionComponent implements OnInit {
  @Input()
  workflowId: number | undefined;

  selectedComputingUnit: DashboardWorkflowComputingUnit | null = null;
  computingUnits: DashboardWorkflowComputingUnit[] = [];
  private readonly REFRESH_INTERVAL_MS = 2000;

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    this.refreshComputingUnits();
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
    this.updateComputingUnitMetrics();
  }

  /**
   * Update the currently selected computing units metrics
   */
  updateComputingUnitMetrics(): void {
    const cuid = this.selectedComputingUnit?.computingUnit.cuid;
    if (!cuid) {
      return // for now
    }
    this.computingUnitService.getComputingUnitMetrics(cuid)
      .pipe(
        map(metrics => ({
          cpu: metrics["cpu"],
          memory: metrics["memory"]
        }))
      )
      .subscribe(({ cpu, memory }) => {
        console.log(`CPU: ${cpu}`);
        console.log(`Memory: ${memory}`);
      });
  }

  /**
   * Start a new computing unit.
   */
  startComputingUnit(): void {
    const computeUnitName = `Compute for Workflow ${this.workflowId}`;
    this.computingUnitService
      .createComputingUnit(computeUnitName)
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
          this.notificationService.success(`Terminated computing unit with URI: ${uri}`);
          this.refreshComputingUnits();
        },
        error: (err: unknown) => this.notificationService.error("Failed to terminate computing unit"),
      });
  }

  /**
   * Called whenever the selected computing unit changes.
   */
  onComputingUnitChange(newSelection: DashboardWorkflowComputingUnit | null): void {
    console.log("Selected computing unit changed to:", newSelection);
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (newSelection && isDefined(wid)) {
      console.log(`Selected Unit URI: ${newSelection.uri}`);
      this.workflowWebsocketService.reopenWebsocket(wid, newSelection.computingUnit.cuid);
    } else {
      console.log("Selection cleared.");
      this.workflowWebsocketService.closeWebsocket();
    }
  }

  /**
   * Get badge color based on the unit's status.
   */
  getBadgeColor(status: string): string {
    return status === "Running" ? "green" : "yellow";
  }
}

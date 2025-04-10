import { Injectable, OnDestroy } from "@angular/core";
import { BehaviorSubject, Observable, interval, Subscription, Subject, timer, of } from "rxjs";
import { filter, map, switchMap, tap, take, mergeMap, catchError } from "rxjs/operators";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ComputingUnitConnectionState } from "../../types/computing-unit-connection.interface";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecutionState } from "../../types/execute-workflow.interface";

/**
 * Service that manages and provides access to computing unit status information
 * across the application.
 *
 * This service is agnostic to whether the computing unit manager is enabled or not.
 * In local mode, it will provide a default local computing unit with status based on websocket connection.
 */
@UntilDestroy()
@Injectable({
  providedIn: "root",
})
export class ComputingUnitStatusService implements OnDestroy {
  // Behavior subjects to track and broadcast state changes
  private selectedUnitSubject = new BehaviorSubject<DashboardWorkflowComputingUnit | null>(null);
  private allUnitsSubject = new BehaviorSubject<DashboardWorkflowComputingUnit[]>([]);
  private connectedSubject = new BehaviorSubject<boolean>(false);

  // New behavior subjects for tracking connection process
  private isCreatingUnitSubject = new BehaviorSubject<boolean>(false);
  private isConnectingUnitSubject = new BehaviorSubject<boolean>(false);
  private autoRunAfterConnectSubject = new BehaviorSubject<boolean>(false);
  private workflowIdSubject = new BehaviorSubject<number | undefined>(undefined);

  // Connection timeout values
  private readonly CONNECTION_TIMEOUT_MS = 20000;

  // Refresh interval in milliseconds
  private readonly REFRESH_INTERVAL_MS = 2000;
  private readonly CONNECTION_CHECK_INTERVAL_MS = 1000;
  private refreshSubscription: Subscription | null = null;
  private connectionCheckSubscription: Subscription | null = null;
  private connectionTimeoutTimer: Subscription | null = null;

  // Flag to track if we're using a local computing unit
  private isUsingLocalComputingUnit: boolean = false;

  // Variables to store auto-run parameters
  private autoRunExecutionName = "";
  private autoRunEnableEmailNotification = false;

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService, // Added for auto-run
    private notificationService: NotificationService // Added for error notifications
  ) {
    // Initialize the service by loading computing units
    this.initializeService();

    // Monitor websocket connection status
    this.monitorConnectionStatus();

    // Monitor auto-run execution state transitions
    this.monitorExecutionStateForAutoRun();
  }

  // Initialize the service with available computing units
  private initializeService(): void {
    // Initial load of computing units
    this.computingUnitService
      .listComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: units => {
          // Check if the units include a local computing unit (when cuManager is disabled)
          this.isUsingLocalComputingUnit = !environment.computingUnitManagerEnabled;

          // Update the computing units
          this.updateComputingUnits(units);
        },
        error: (err: unknown) => console.error("Failed to fetch computing units:", err),
      });

    // Set up periodic refresh
    this.startRefreshInterval();
  }

  // Update computing units list and the selected unit
  private updateComputingUnits(units: DashboardWorkflowComputingUnit[]): void {
    // Update the all units list
    this.allUnitsSubject.next(units);

    // If we don't have a selected unit yet, select one
    if (!this.selectedUnitSubject.value) {
      const runningUnit = units.find(unit => unit.status === "Running");
      if (runningUnit) {
        this.selectComputingUnit(runningUnit);
      } else if (units.length > 0) {
        // Otherwise select the first available unit
        this.selectComputingUnit(units[0]);
      }
    } else {
      // If we already have a selected unit, update its status from the fresh data
      const updatedUnit = units.find(
        unit => unit.computingUnit.cuid === this.selectedUnitSubject.value?.computingUnit.cuid
      );

      if (updatedUnit) {
        this.selectedUnitSubject.next(updatedUnit);
      } else if (units.length > 0) {
        // Our selected unit is no longer available, select another one
        this.selectComputingUnit(units[0]);
      } else {
        // No units available
        this.selectedUnitSubject.next(null);
      }
    }
  }

  // Monitor the connection status of the websocket service
  private monitorConnectionStatus(): void {
    // Initial state
    this.connectedSubject.next(this.workflowWebsocketService.isConnected);

    // Set up periodic check of connection status
    this.connectionCheckSubscription = interval(this.CONNECTION_CHECK_INTERVAL_MS)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        const isConnected = this.workflowWebsocketService.isConnected;

        // Always update connection status and computing unit status together
        this.connectedSubject.next(isConnected);

        // Always update the status of the computing unit based on connection
        if (this.selectedUnitSubject.value) {
          // If we have a selected unit but connection is lost, update its status
          const currentStatus = this.selectedUnitSubject.value.status;
          const newStatus = isConnected
            ? // If the unit was previously in disconnected state but we're now connected,
              // set it to Running, otherwise keep the current status
              currentStatus === "Disconnected"
              ? "Running"
              : currentStatus
            : // If we lost connection, always set to Disconnected
              "Disconnected";

          if (currentStatus !== newStatus) {
            const updatedUnit = {
              ...this.selectedUnitSubject.value,
              status: newStatus,
            };
            this.selectedUnitSubject.next(updatedUnit);

            // Update the units list as well
            const updatedUnitsList = this.allUnitsSubject.value.map(unit =>
              unit.computingUnit.cuid === updatedUnit.computingUnit.cuid ? updatedUnit : unit
            );
            this.allUnitsSubject.next(updatedUnitsList);
          }
        }
      });
  }

  // Start the interval to refresh computing unit data
  private startRefreshInterval(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
    }

    this.refreshSubscription = interval(this.REFRESH_INTERVAL_MS)
      .pipe(
        switchMap(() => this.computingUnitService.listComputingUnits()),
        untilDestroyed(this)
      )
      .subscribe({
        next: units => {
          // Check the connection status
          const isConnected = this.workflowWebsocketService.isConnected;

          // If we're not connected, ensure all units show as disconnected
          if (!isConnected) {
            const disconnectedUnits = units.map(unit => ({
              ...unit,
              status: "Disconnected",
            }));
            this.updateComputingUnits(disconnectedUnits);
          } else {
            // In local mode with connection, ensure unit shows as Running
            if (this.isUsingLocalComputingUnit) {
              const modifiedUnits = units.map(unit => ({
                ...unit,
                // Local units should always be Running when connected
                status: "Running",
              }));
              this.updateComputingUnits(modifiedUnits);
            } else {
              // In cuManager mode with connection, use the units as is
              this.updateComputingUnits(units);
            }
          }
        },
        error: (err: unknown) => console.error("Failed to refresh computing units:", err),
      });
  }

  // Select a computing unit and emit the updated selection
  public selectComputingUnit(unit: DashboardWorkflowComputingUnit): void {
    // First, update the selected unit
    this.selectedUnitSubject.next(unit);

    // If the status is running and we're connected, make sure we emit correct connection status
    if (unit.status === "Running" && this.workflowWebsocketService.isConnected) {
      this.connectedSubject.next(true);
    } else {
      this.connectedSubject.next(false);
    }

    // If we're in local mode, make sure connection status and unit status match
    if (this.isUsingLocalComputingUnit) {
      // Update the status based on connection status
      const isConnected = this.workflowWebsocketService.isConnected;
      const updatedUnit = {
        ...unit,
        status: isConnected ? "Running" : "Disconnected",
      };
      this.selectedUnitSubject.next(updatedUnit);

      // Also update in the all units list
      const updatedUnitsList = this.allUnitsSubject.value.map(u =>
        u.computingUnit.cuid === updatedUnit.computingUnit.cuid ? updatedUnit : u
      );
      this.allUnitsSubject.next(updatedUnitsList);
    }
  }

  // Observable for the currently selected computing unit
  public getSelectedComputingUnit(): Observable<DashboardWorkflowComputingUnit | null> {
    return this.selectedUnitSubject.asObservable();
  }

  // Observable for all available computing units
  public getAllComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return this.allUnitsSubject.asObservable();
  }

  // Observable for the connection status to the selected computing unit
  public getConnectionStatus(): Observable<boolean> {
    return this.connectedSubject.asObservable();
  }

  // Get the current status of the selected computing unit as string
  public getStatus(): Observable<ComputingUnitConnectionState> {
    return this.selectedUnitSubject.pipe(
      map((unit: DashboardWorkflowComputingUnit | null) => {
        if (!unit) {
          return ComputingUnitConnectionState.NoComputingUnit;
        }

        // Convert string status to enum
        switch (unit.status) {
          case "Running":
            return ComputingUnitConnectionState.Running;
          case "Pending":
            return ComputingUnitConnectionState.Pending;
          case "Disconnected":
            return ComputingUnitConnectionState.Disconnected;
          case "Terminating":
            return ComputingUnitConnectionState.Terminating;
          default:
            return ComputingUnitConnectionState.Disconnected;
        }
      })
    );
  }

  // Get whether the selected computing unit is in a ready state
  public isReady(): Observable<boolean> {
    return this.selectedUnitSubject.pipe(
      map((unit: DashboardWorkflowComputingUnit | null) => {
        // Must have a unit, the unit status must be Running, and the websocket must be connected
        return !!unit && unit.status === "Running" && this.workflowWebsocketService.isConnected;
      })
    );
  }

  // Get the current CPU usage percentage of the selected computing unit
  public getCpuUsagePercentage(): Observable<number> {
    return this.selectedUnitSubject.pipe(
      filter((unit): unit is DashboardWorkflowComputingUnit => !!unit),
      map((unit: DashboardWorkflowComputingUnit) => {
        if (!unit || unit.metrics.cpuUsage === "NaN" || unit.resourceLimits.cpuLimit === "NaN") {
          return 0;
        }

        try {
          const usage = this.parseResourceValue(unit.metrics.cpuUsage);
          const limit = this.parseResourceValue(unit.resourceLimits.cpuLimit);
          return Math.min(Math.round((usage / limit) * 100), 100);
        } catch (e) {
          return 0;
        }
      })
    );
  }

  // Get the current memory usage percentage of the selected computing unit
  public getMemoryUsagePercentage(): Observable<number> {
    return this.selectedUnitSubject.pipe(
      filter((unit): unit is DashboardWorkflowComputingUnit => !!unit),
      map((unit: DashboardWorkflowComputingUnit) => {
        if (!unit || unit.metrics.memoryUsage === "NaN" || unit.resourceLimits.memoryLimit === "NaN") {
          return 0;
        }

        try {
          const usage = this.parseResourceValue(unit.metrics.memoryUsage);
          const limit = this.parseResourceValue(unit.resourceLimits.memoryLimit);
          return Math.min(Math.round((usage / limit) * 100), 100);
        } catch (e) {
          return 0;
        }
      })
    );
  }

  // Helper method to parse resource values like "100m" or "1Gi"
  private parseResourceValue(resource: string): number {
    if (!resource || resource === "NaN") {
      return 0;
    }

    // Extract the number part
    const match = resource.match(/^(\d+(?:\.\d+)?)([a-zA-Z]*)$/);
    if (!match) {
      return 0;
    }

    const value = parseFloat(match[1]);
    const unit = match[2] || "";

    // Convert based on unit
    // CPU units: n (nano), u (micro), m (milli), or none (whole)
    // Memory units: Ki, Mi, Gi, or none (bytes)
    switch (unit) {
      case "n":
        return value / 1_000_000_000;
      case "u":
        return value / 1_000_000;
      case "m":
        return value / 1_000;
      case "Ki":
        return value * 1024;
      case "Mi":
        return value * 1024 * 1024;
      case "Gi":
        return value * 1024 * 1024 * 1024;
      default:
        return value;
    }
  }

  // Clean up on service destroy
  ngOnDestroy(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
      this.refreshSubscription = null;
    }

    if (this.connectionCheckSubscription) {
      this.connectionCheckSubscription.unsubscribe();
      this.connectionCheckSubscription = null;
    }

    if (this.connectionTimeoutTimer) {
      this.connectionTimeoutTimer.unsubscribe();
    }

    this.selectedUnitSubject.complete();
    this.allUnitsSubject.complete();
    this.connectedSubject.complete();
    this.isCreatingUnitSubject.complete();
    this.isConnectingUnitSubject.complete();
    this.autoRunAfterConnectSubject.complete();
    this.workflowIdSubject.complete();
  }

  // Public API to check if computing unit manager is enabled
  public isCuManagerEnabled(): boolean {
    return environment.computingUnitManagerEnabled;
  }

  /**
   * Clear the currently selected computing unit and update connection status
   */
  public clearSelectedComputingUnit(): void {
    this.selectedUnitSubject.next(null);
    this.connectedSubject.next(false);
  }

  /**
   * Sets the current workflow ID for the service to use when connecting
   * @param workflowId The ID of the current workflow
   */
  public setWorkflowId(workflowId: number | undefined): void {
    this.workflowIdSubject.next(workflowId);
  }

  /**
   * Get the current state of the unit creation process as observable
   */
  public isCreatingUnit(): Observable<boolean> {
    return this.isCreatingUnitSubject.asObservable();
  }

  /**
   * Get the current state of the unit connection process as observable
   */
  public isConnectingToUnit(): Observable<boolean> {
    return this.isConnectingUnitSubject.asObservable();
  }

  /**
   * Get whether the service is waiting to auto-run after connection as observable
   */
  public isAutoRunAfterConnect(): Observable<boolean> {
    return this.autoRunAfterConnectSubject.asObservable();
  }

  /**
   * Get the current state of the unit creation process synchronously
   */
  public get isCreatingUnitValue(): boolean {
    return this.isCreatingUnitSubject.value;
  }

  /**
   * Get the current state of the unit connection process synchronously
   */
  public get isConnectingToUnitValue(): boolean {
    return this.isConnectingUnitSubject.value;
  }

  /**
   * Get whether the service is waiting to auto-run after connection synchronously
   */
  public get isAutoRunAfterConnectValue(): boolean {
    return this.autoRunAfterConnectSubject.value;
  }

  /**
   * Create a new computing unit with default settings and connect to it
   */
  public createAndConnectComputingUnit(): Observable<boolean> {
    if (!environment.computingUnitManagerEnabled || !this.workflowIdSubject.value) {
      return of(false);
    }

    // Set to creating state
    this.isCreatingUnitSubject.next(true);

    // Create response subject to track overall process success
    const processCompleteSubject = new Subject<boolean>();

    // Get the available configurations
    this.computingUnitService
      .getComputingUnitLimitOptions()
      .pipe(
        untilDestroyed(this),
        mergeMap(({ cpuLimitOptions, memoryLimitOptions }) => {
          const defaultCpu = cpuLimitOptions[0] || "1";
          const defaultMemory = memoryLimitOptions[0] || "1Gi";
          const workflowId = this.workflowIdSubject.value;
          const unitName = workflowId ? `Workflow ${workflowId} Unit` : "Default Unit";

          // Create the computing unit
          return this.computingUnitService.createComputingUnit(unitName, defaultCpu, defaultMemory);
        }),
        catchError((err: Error) => {
          this.isCreatingUnitSubject.next(false);
          this.notificationService.error(`Failed to create computing unit: ${err}`);
          processCompleteSubject.next(false);
          return of(null);
        })
      )
      .subscribe({
        next: (unit: DashboardWorkflowComputingUnit | null) => {
          // Reset creation state
          this.isCreatingUnitSubject.next(false);

          if (unit) {
            // Connect to the newly created unit
            this.connectToComputingUnit(unit)
              .pipe(untilDestroyed(this))
              .subscribe(connected => {
                processCompleteSubject.next(connected);
              });
          }
        },
        error: (err: unknown) => {
          this.isCreatingUnitSubject.next(false);
          this.notificationService.error(`Failed to create computing unit: ${err}`);
          processCompleteSubject.next(false);
        },
      });

    return processCompleteSubject.asObservable();
  }

  /**
   * Connect to a specific computing unit
   * @param unit The computing unit to connect to
   * @returns An observable that emits true when connected, false on failure
   */
  public connectToComputingUnit(unit: DashboardWorkflowComputingUnit): Observable<boolean> {
    if (!this.workflowIdSubject.value) {
      return of(false);
    }

    // Create response subject to track connection success
    const connectedSubject = new Subject<boolean>();

    // Set the connecting state
    this.isConnectingUnitSubject.next(true);

    // Select the unit in the service
    this.selectComputingUnit(unit);

    // Connect to the unit's websocket
    this.workflowWebsocketService.closeWebsocket();
    this.workflowWebsocketService.openWebsocket(this.workflowIdSubject.value, undefined, unit.computingUnit.cuid);

    // Check connection status frequently
    const connectionCheck = interval(200)
      .pipe(
        untilDestroyed(this),
        filter(() => this.workflowWebsocketService.isConnected)
      )
      .subscribe(() => {
        // Update connection status
        this.connectedSubject.next(true);

        // Update the unit status if needed
        if (this.selectedUnitSubject.value && this.selectedUnitSubject.value.status !== "Running") {
          const updatedUnit = {
            ...this.selectedUnitSubject.value,
            status: "Running",
          };
          this.selectedUnitSubject.next(updatedUnit);

          // Update the list as well
          this.updateUnitInList(updatedUnit);
        }

        // Complete the connection process
        this.isConnectingUnitSubject.next(false);
        connectedSubject.next(true);
        connectionCheck.unsubscribe();

        // Clear any timeout
        if (this.connectionTimeoutTimer) {
          this.connectionTimeoutTimer.unsubscribe();
          this.connectionTimeoutTimer = null;
        }
      });

    // Set connection timeout
    this.connectionTimeoutTimer = timer(this.CONNECTION_TIMEOUT_MS).subscribe(() => {
      if (connectionCheck && !connectionCheck.closed) {
        connectionCheck.unsubscribe();
        this.isConnectingUnitSubject.next(false);
        this.notificationService.error("Failed to connect to computing unit after timeout. Please try again.");
        connectedSubject.next(false);
        this.connectionTimeoutTimer = null;
      }
    });

    return connectedSubject.asObservable();
  }

  /**
   * Helper method to update a unit in the all units list
   */
  private updateUnitInList(unit: DashboardWorkflowComputingUnit): void {
    const updatedList = this.allUnitsSubject.value.map(existingUnit =>
      existingUnit.computingUnit.cuid === unit.computingUnit.cuid ? unit : existingUnit
    );
    this.allUnitsSubject.next(updatedList);
  }

  /**
   * Create a computing unit, connect to it, and prepare to run the workflow
   * @param executionName The name to use for the execution
   * @param enableEmailNotification Whether to enable email notifications
   * @returns Observable<boolean> that emits true when the unit is connected and ready
   */
  public createConnectAndPrepareRun(executionName: string, enableEmailNotification: boolean): Observable<boolean> {
    // Check if already connected to a running computing unit
    if (
      this.workflowWebsocketService.isConnected &&
      this.selectedUnitSubject.value &&
      this.selectedUnitSubject.value.status === "Running"
    ) {
      // Set execution parameters
      this.setAutoRunParameters(executionName, enableEmailNotification);

      // Already connected, return immediately
      return of(true);
    }

    // Set auto-run flag and parameters
    this.setAutoRunParameters(executionName, enableEmailNotification);

    // Create and connect to a computing unit
    return this.createAndConnectComputingUnit();
  }

  /**
   * Set parameters for auto-run after connection
   */
  private setAutoRunParameters(executionName: string, enableEmailNotification: boolean): void {
    // Store these in instance variables for the monitoring method to use
    this.autoRunExecutionName = executionName;
    this.autoRunEnableEmailNotification = enableEmailNotification;
    this.autoRunAfterConnectSubject.next(true);
  }

  /**
   * Monitor execution state changes for auto-run workflow
   */
  private monitorExecutionStateForAutoRun(): void {
    // Subscribe to connection status
    this.connectedSubject
      .pipe(
        untilDestroyed(this),
        filter(connected => connected && this.autoRunAfterConnectSubject.value)
      )
      .subscribe(() => {
        // Execute the workflow with the stored parameters
        this.executeWorkflowService.executeWorkflowWithEmailNotification(
          this.autoRunExecutionName,
          this.autoRunEnableEmailNotification
        );

        // Set a timeout to clear the flag if execution doesn't start quickly
        // This prevents the button from staying in "Submitting" state indefinitely
        setTimeout(() => {
          if (this.autoRunAfterConnectSubject.value) {
            console.warn("Auto-run flag not cleared by execution, resetting manually");
            this.autoRunAfterConnectSubject.next(false);
          }
        }, 5000);
      });

    // Monitor execution state changes to clear auto-run flag
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // Clear the autoRunAfterConnect flag when execution starts or completes
        if (
          event.current.state === ExecutionState.Initializing ||
          event.current.state === ExecutionState.Running ||
          event.current.state === ExecutionState.Failed ||
          event.current.state === ExecutionState.Killed ||
          event.current.state === ExecutionState.Completed
        ) {
          this.autoRunAfterConnectSubject.next(false);
        }
      });

    // Also monitor websocket connection changes
    interval(1000)
      .pipe(
        untilDestroyed(this),
        filter(() => this.isConnectingUnitSubject.value)
      )
      .subscribe(() => {
        // If we're in connecting state but websocket connects, update connection status
        if (this.workflowWebsocketService.isConnected) {
          this.connectedSubject.next(true);

          // If we have a selected unit, update its status to Running
          if (this.selectedUnitSubject.value) {
            const updatedUnit = {
              ...this.selectedUnitSubject.value,
              status: "Running",
            };
            this.selectedUnitSubject.next(updatedUnit);
            this.updateUnitInList(updatedUnit);
          }
        }
      });
  }
}

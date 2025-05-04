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

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private executeWorkflowService: ExecuteWorkflowService,
    private notificationService: NotificationService
  ) {
    // Initialize the service by loading computing units
    this.initializeService();

    // Monitor websocket connection status
    this.monitorConnectionStatus();
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
        const previousConnected = this.connectedSubject.value;

        // Only update when connection status actually changes
        if (previousConnected !== isConnected) {
          // Update the connection status
          this.connectedSubject.next(isConnected);

          // Update the selected computing unit status based on connection
          if (this.selectedUnitSubject.value) {
            const currentStatus = this.selectedUnitSubject.value.status;
            let newStatus = currentStatus;

            // Determine the new status based on connection change
            if (isConnected) {
              // If we just connected and unit was disconnected, mark as Running
              newStatus = currentStatus === "Disconnected" ? "Running" : currentStatus;
            } else {
              // If we just disconnected, mark as Disconnected
              newStatus = "Disconnected";
            }

            // Only update if status changed
            if (currentStatus !== newStatus) {
              const updatedUnit = {
                ...this.selectedUnitSubject.value,
                status: newStatus,
              };
              this.selectedUnitSubject.next(updatedUnit);

              // Update only this unit in the list
              this.updateUnitInList(updatedUnit);
            }
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

          // Get current selected unit's CUID
          const selectedCuid = this.selectedUnitSubject.value?.computingUnit.cuid;

          if (this.isUsingLocalComputingUnit) {
            // In local mode, ensure units show correct status based on connection
            const modifiedUnits = units.map(unit => ({
              ...unit,
              status: isConnected ? "Running" : "Disconnected",
            }));
            this.updateComputingUnits(modifiedUnits);
          } else {
            // In cuManager mode, handle each unit individually:
            // - Only mark the selected unit as disconnected when websocket is disconnected
            // - Keep other units' status as reported by the backend
            let modifiedUnits = [...units];

            // If there's a selected unit and we're not connected
            if (selectedCuid && !isConnected) {
              // Only mark the currently selected unit as disconnected
              modifiedUnits = modifiedUnits.map(unit => {
                if (unit.computingUnit.cuid === selectedCuid) {
                  return {
                    ...unit,
                    status: "Disconnected",
                  };
                }
                return unit;
              });
            }

            this.updateComputingUnits(modifiedUnits);
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
    this.isConnectingUnitSubject.next(false);
    this.isCreatingUnitSubject.next(false);
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
        mergeMap(({ cpuLimitOptions, memoryLimitOptions }) => {
          const defaultCpu = cpuLimitOptions[0] || "1";
          const defaultMemory = memoryLimitOptions[0] || "1Gi";
          const workflowId = this.workflowIdSubject.value;
          const unitName = workflowId ? `Workflow ${workflowId} Unit` : "Default Unit";

          // Create the computing unit
          return this.computingUnitService.createComputingUnit(unitName, defaultCpu, defaultMemory);
        }),
        catchError((err: unknown) => {
          this.isCreatingUnitSubject.next(false);
          this.notificationService.error(`Failed to create computing unit: ${err}`);
          processCompleteSubject.next(false);
          return of(null);
        }),
        untilDestroyed(this)
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
          } else {
            processCompleteSubject.next(false);
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

    // Check connection status more frequently
    const connectionCheck = interval(200)
      .pipe(
        filter(() => this.workflowWebsocketService.isConnected),
        take(1), // Take only the first connection event
        untilDestroyed(this)
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
   * Helper method to update a single unit in the units list
   */
  private updateUnitInList(updatedUnit: DashboardWorkflowComputingUnit): void {
    const updatedUnitsList = this.allUnitsSubject.value.map(unit =>
      unit.computingUnit.cuid === updatedUnit.computingUnit.cuid ? updatedUnit : unit
    );
    this.allUnitsSubject.next(updatedUnitsList);
  }

  /**
   * Create a computing unit and connect to it (no auto-run)
   * @returns Observable<boolean> that emits true when the unit is connected and ready
   */
  public createAndConnect(): Observable<boolean> {
    // Create and connect to a computing unit
    return this.createAndConnectComputingUnit();
  }

  /**
   * Terminate a computing unit, ensuring websocket is closed first
   * @param cuid The ID of the computing unit to terminate
   * @returns Observable that completes when the termination process is done
   */
  public terminateComputingUnit(cuid: number): Observable<boolean> {
    // Create a subject to track the termination process
    const terminationSubject = new Subject<boolean>();

    // First check if this is the currently selected unit
    const isSelectedUnit = this.selectedUnitSubject.value?.computingUnit.cuid === cuid;

    // If this is the selected unit, close the websocket first
    if (isSelectedUnit && this.workflowWebsocketService.isConnected) {
      // Close the websocket connection
      this.workflowWebsocketService.closeWebsocket();

      // Clear the selected unit or mark as terminating
      if (this.selectedUnitSubject.value) {
        const updatedUnit = {
          ...this.selectedUnitSubject.value,
          status: "Terminating",
        };
        this.selectedUnitSubject.next(updatedUnit);
        this.updateUnitInList(updatedUnit);
      }

      // Update connection status
      this.connectedSubject.next(false);
    }

    // Now terminate the unit
    this.computingUnitService
      .terminateComputingUnit(cuid)
      .pipe(
        catchError((err: unknown) => {
          this.notificationService.error(`Failed to terminate computing unit: ${err}`);
          terminationSubject.next(false);
          return of(null);
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: () => {
          // Successfully terminated, refresh the list
          this.computingUnitService
            .listComputingUnits()
            .pipe(untilDestroyed(this))
            .subscribe(units => {
              this.updateComputingUnits(units);

              // If the terminated unit was selected and there are no running units,
              // set to null to trigger "NoComputingUnit" state
              if (isSelectedUnit) {
                const runningUnit = units.find(u => u.status === "Running");
                if (!runningUnit) {
                  this.selectedUnitSubject.next(null);
                  this.connectedSubject.next(false);

                  // Also ensure connecting flags are reset
                  this.isConnectingUnitSubject.next(false);
                  this.isCreatingUnitSubject.next(false);
                }
              }

              terminationSubject.next(true);
            });
        },
        error: (err: unknown) => {
          this.notificationService.error(`Failed to terminate computing unit: ${err}`);
          terminationSubject.next(false);
        },
      });

    return terminationSubject.asObservable();
  }
}

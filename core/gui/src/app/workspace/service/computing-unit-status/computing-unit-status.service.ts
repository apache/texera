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
import { BehaviorSubject, Observable, interval, Subscription, Subject, timer, of, merge } from "rxjs";
import { filter, map, switchMap, tap, take, mergeMap, catchError, distinctUntilChanged } from "rxjs/operators";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ComputingUnitConnectionState } from "../../types/computing-unit-connection.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";

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

  // New behavior subjects for tracking connection process
  private isCreatingUnitSubject = new BehaviorSubject<boolean>(false);
  private isConnectingUnitSubject = new BehaviorSubject<boolean>(false);

  private readonly refreshComputingUnitListSignal = new Subject<void>();

  // Refresh interval in milliseconds
  private readonly REFRESH_INTERVAL_MS = 2000;
  private readonly CONNECTION_CHECK_INTERVAL_MS = 1000;
  private refreshSubscription: Subscription | null = null;
  private currentConnectedCuid?: number;

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
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
      .subscribe(units => {
        // Update the computing units
        this.updateComputingUnits(units);
      });

    // Set up periodic refresh
    this.startRefreshInterval();
  }

  private refreshComputingUnitList(): void {
    this.refreshComputingUnitListSignal.next();
  }

  // Update computing units list and the selected unit
  private updateComputingUnits(units: DashboardWorkflowComputingUnit[]): void {
    // Update the all units list
    this.allUnitsSubject.next(units);

    const updatedUnit = units.find(
      unit => unit.computingUnit.cuid === this.selectedUnitSubject.value?.computingUnit.cuid
    );

    if (updatedUnit) {
      this.selectedUnitSubject.next(updatedUnit);
    } else {
      // Our selected unit is no longer available
      this.selectedUnitSubject.next(null);
    }
  }

  // Monitor the connection status of the websocket service
  private monitorConnectionStatus(): void {
    this.workflowWebsocketService // use websocket’s native stream
      .getConnectionStatusStream()
      .pipe(
        distinctUntilChanged(), // react only to real changes
        untilDestroyed(this)
      )
      .subscribe(isConnected => {
        /* ---------- update the selected CU’s status ---------- */
        if (this.selectedUnitSubject.value) {
          const cur = this.selectedUnitSubject.value;
          const desired = isConnected ? (cur.status === "Disconnected" ? "Running" : cur.status) : "Disconnected";

          if (cur.status !== desired) {
            const updated = { ...cur, status: desired };
            this.selectedUnitSubject.next(updated);
            this.updateUnitInList(updated);
          }
        }

        /* ---------- trigger a one-off refresh of the CU list ---------- */
        this.refreshComputingUnitList();
      });
  }

  // Start the interval to refresh computing unit data
  private startRefreshInterval(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
    }

    this.refreshSubscription = this.refreshComputingUnitListSignal
      .pipe(
        switchMap(() => this.computingUnitService.listComputingUnits()),
        untilDestroyed(this)
      )
      .subscribe(units => {
        // same post-processing logic as before
        const isConnected = this.workflowWebsocketService.isConnected;
        const selectedCuid = this.selectedUnitSubject.value?.computingUnit.cuid;

        let modifiedUnits = [...units];
        if (selectedCuid && !isConnected) {
          modifiedUnits = modifiedUnits.map(u =>
            u.computingUnit.cuid === selectedCuid ? { ...u, status: "Disconnected" } : u
          );
        }
        this.updateComputingUnits(modifiedUnits);
      });
  }

  /**
   * Select a computing unit **by its CUID** and emit the updated selection.
   */
  public selectComputingUnit(wid: number | undefined, cuid: number): void {
    const trySelect = (unit: DashboardWorkflowComputingUnit) => {
      // open websocket if needed
      if (isDefined(wid) && this.currentConnectedCuid !== cuid) {
        if (this.workflowWebsocketService.isConnected) {
          this.workflowWebsocketService.closeWebsocket();
          this.workflowStatusService.clearStatus();
        }
        this.workflowWebsocketService.openWebsocket(wid, 1 /* uid */, cuid);
        this.currentConnectedCuid = cuid;
      }
      this.selectedUnitSubject.next(unit);
    };

    // try immediate lookup in the current cache
    const cachedUnit = this.allUnitsSubject.value.find(u => u.computingUnit.cuid === cuid);
    if (cachedUnit) {
      trySelect(cachedUnit);
      return;
    }

    // otherwise trigger a refresh and wait until the unit appears once
    this.refreshComputingUnitList();

    this.allUnitsSubject
      .pipe(
        filter(units => units.some(u => u.computingUnit.cuid === cuid)),
        take(1), // auto-unsubscribe after first match
        untilDestroyed(this)
      )
      .subscribe(units => {
        const unit = units.find(u => u.computingUnit.cuid === cuid)!;
        trySelect(unit);
      });
  }
  // Observable for the currently selected computing unit
  public getSelectedComputingUnit(): Observable<DashboardWorkflowComputingUnit | null> {
    return this.selectedUnitSubject.asObservable();
  }

  // Observable for all available computing units
  public getAllComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return this.allUnitsSubject.asObservable();
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

  // Clean up on service destroy
  ngOnDestroy(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
      this.refreshSubscription = null;
    }

    this.selectedUnitSubject.complete();
    this.allUnitsSubject.complete();
    this.isCreatingUnitSubject.complete();
    this.isConnectingUnitSubject.complete();
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
   * Helper method to update a single unit in the units list
   */
  private updateUnitInList(updatedUnit: DashboardWorkflowComputingUnit): void {
    const updatedUnitsList = this.allUnitsSubject.value.map(unit =>
      unit.computingUnit.cuid === updatedUnit.computingUnit.cuid ? updatedUnit : unit
    );
    this.allUnitsSubject.next(updatedUnitsList);
  }

  /**
   * Terminate a computing unit, ensuring websocket is closed first
   * @param cuid The ID of the computing unit to terminate
   * @returns Observable that completes when the termination process is done
   */
  public terminateComputingUnit(cuid: number): Observable<boolean> {
    const isSelected = this.selectedUnitSubject.value?.computingUnit.cuid === cuid;

    if (isSelected && this.workflowWebsocketService.isConnected) {
      this.workflowWebsocketService.closeWebsocket();
      this.workflowStatusService.clearStatus();

      const terminatingUnit = {
        ...this.selectedUnitSubject.value!,
        status: "Terminating",
      };
      this.selectedUnitSubject.next(terminatingUnit);
      this.updateUnitInList(terminatingUnit);
    }

    return this.computingUnitService.terminateComputingUnit(cuid).pipe(
      tap(() => {
        // trigger a single refresh; the refresh pipeline will
        // pull the new list and call updateComputingUnits()
        this.refreshComputingUnitList();
      }),
      map(() => true),
      catchError((err: unknown) => {
        this.notificationService.error(`Failed to terminate computing unit: ${err}`);
        return of(false);
      }),
      take(1) // complete after first emission
    );
  }

  /**
   * Get the current selected computing unit value synchronously
   */
  public getSelectedComputingUnitValue(): DashboardWorkflowComputingUnit | null {
    return this.selectedUnitSubject.value;
  }
}

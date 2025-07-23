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
import { BehaviorSubject, Observable, interval, Subscription, Subject, timer, of, merge, forkJoin } from "rxjs";
import { filter, map, switchMap, tap, take, mergeMap, catchError, distinctUntilChanged } from "rxjs/operators";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ComputingUnitState } from "../../types/computing-unit-connection.interface";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { UserService } from "../../../common/service/user/user.service";

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
  private readonly allComputingUnitsSubject = new BehaviorSubject<DashboardWorkflowComputingUnit[]>([]);

  private readonly refreshComputingUnitListSignal = new Subject<void>();

  // Refresh interval in milliseconds
  private readonly REFRESH_INTERVAL_MS = 2000;
  private refreshSubscription: Subscription | null = null;
  private currentConnectedCuid?: number;
  private selectedUnitPoll?: Subscription;

  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private userService: UserService
  ) {
    // Initialize the service by loading computing units
    this.initializeService();

    // Monitor websocket connection status
    this.monitorConnectionStatus();
  }

  // Initialize the service with available computing units
  private initializeService(): void {
    this.computingUnitService
      .listComputingUnits()
      .pipe(untilDestroyed(this))
      .subscribe(units => {
        this.setComputingUnitsState(units);
      });

    // Set up periodic refresh
    this.startRefreshInterval();
  }

  public refreshComputingUnitList(): void {
    this.refreshComputingUnitListSignal.next();
  }

  private startPollingSelectedUnit(cuid: number): void {
    // cancel previous poll, if any
    this.selectedUnitPoll?.unsubscribe();

    this.selectedUnitPoll = interval(this.REFRESH_INTERVAL_MS)
      .pipe(
        // each tick → get fresh data for *this* cuid
        switchMap(() => this.computingUnitService.getComputingUnit(cuid)),
        untilDestroyed(this)
      )
      .subscribe(unit => {
        this.updateUnitInList(unit);
      }); // merge into cache
  }

  private stopPollingSelectedUnit(): void {
    this.selectedUnitPoll?.unsubscribe();
    this.selectedUnitPoll = undefined;
  }
  // Update computing units list and the selected unit
  private setComputingUnitsState(units: DashboardWorkflowComputingUnit[]): void {
    this.allComputingUnitsSubject.next(units);

    const updatedSelectedUnit = units.find(
      unit => unit.computingUnit.cuid === this.selectedUnitSubject.value?.computingUnit.cuid
    );

    if (updatedSelectedUnit) {
      this.selectedUnitSubject.next(updatedSelectedUnit);
    } else if (this.selectedUnitSubject.value) {
      // The selected unit is no longer in the list
      this.selectedUnitSubject.next(null);
      this.stopPollingSelectedUnit();
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
        this.setComputingUnitsState(units);
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
        this.workflowWebsocketService.openWebsocket(wid, this.userService.getCurrentUser()?.uid, cuid);
        this.currentConnectedCuid = cuid;
        this.selectedUnitSubject.next(unit);
        this.startPollingSelectedUnit(cuid);
      }
    };

    // try immediate lookup in the current cache
    const cachedUnit = this.allComputingUnitsSubject.value.find(u => u.computingUnit.cuid === cuid);

    if (cachedUnit) {
      trySelect(cachedUnit);
      return;
    }

    // otherwise trigger a refresh and wait until the unit appears once
    this.refreshComputingUnitList();

    this.allComputingUnitsSubject
      .pipe(
        filter(units => units.some(u => u.computingUnit.cuid === cuid)),
        take(1),
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
    return this.allComputingUnitsSubject;
  }

  // Get the current status of the selected computing unit as string
  public getStatus(): Observable<ComputingUnitState> {
    return this.selectedUnitSubject.pipe(
      map((unit: DashboardWorkflowComputingUnit | null) => {
        if (!unit) {
          return ComputingUnitState.NoComputingUnit;
        }

        // Convert string status to enum
        switch (unit.status) {
          case "Running":
            return ComputingUnitState.Running;
          case "Pending":
            return ComputingUnitState.Pending;
          default:
            return ComputingUnitState.Pending;
        }
      })
    );
  }

  // Clean up on service destroy
  ngOnDestroy(): void {
    this.refreshSubscription?.unsubscribe();
    this.selectedUnitPoll?.unsubscribe();

    this.selectedUnitSubject.complete();
    this.allComputingUnitsSubject.complete();
  }

  /**
   * Helper method to update a single unit in the units list
   */
  private updateUnitInList(updatedUnit: DashboardWorkflowComputingUnit): void {
    const merged: DashboardWorkflowComputingUnit[] = this.allComputingUnitsSubject.value.map(u =>
      u.computingUnit.cuid === updatedUnit.computingUnit.cuid ? updatedUnit : u
    );

    this.setComputingUnitsState(merged);
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
    }

    return this.computingUnitService.terminateComputingUnit(cuid).pipe(
      tap(() => {
        // trigger a single refresh; the refresh pipeline will
        // pull the new list and call updateComputingUnits()
        this.refreshComputingUnitList();
      }),
      map(() => true),
      catchError((err: unknown) => {
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

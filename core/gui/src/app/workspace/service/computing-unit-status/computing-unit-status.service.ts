import { Injectable, OnDestroy } from "@angular/core";
import { BehaviorSubject, Observable, interval, Subscription } from "rxjs";
import { filter, map, switchMap, tap } from "rxjs/operators";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { WorkflowComputingUnitManagingService } from "../workflow-computing-unit/workflow-computing-unit-managing.service";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

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
  
  // Refresh interval in milliseconds
  private readonly REFRESH_INTERVAL_MS = 2000;
  private readonly CONNECTION_CHECK_INTERVAL_MS = 1000;
  private refreshSubscription: Subscription | null = null;
  private connectionCheckSubscription: Subscription | null = null;
  
  // Flag to track if we're using a local computing unit
  private isUsingLocalComputingUnit: boolean = false;
  
  constructor(
    private computingUnitService: WorkflowComputingUnitManagingService,
    private workflowWebsocketService: WorkflowWebsocketService
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
        error: err => console.error("Failed to fetch computing units:", err)
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
          const newStatus = isConnected ? 
            // If the unit was previously in disconnected state but we're now connected,
            // set it to Running, otherwise keep the current status
            (currentStatus === "Disconnected" ? "Running" : currentStatus) : 
            // If we lost connection, always set to Disconnected
            "Disconnected";

          if (currentStatus !== newStatus) {
            const updatedUnit = {
              ...this.selectedUnitSubject.value,
              status: newStatus
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
              status: "Disconnected"
            }));
            this.updateComputingUnits(disconnectedUnits);
          } else {
            // In local mode with connection, ensure unit shows as Running
            if (this.isUsingLocalComputingUnit) {
              const modifiedUnits = units.map(unit => ({
                ...unit,
                // Local units should always be Running when connected
                status: "Running" 
              }));
              this.updateComputingUnits(modifiedUnits);
            } else {
              // In cuManager mode with connection, use the units as is
              this.updateComputingUnits(units);
            }
          }
        },
        error: err => console.error("Failed to refresh computing units:", err)
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
        status: isConnected ? "Running" : "Disconnected"
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
  public getStatus(): Observable<string> {
    return this.selectedUnitSubject.pipe(
      map((unit: DashboardWorkflowComputingUnit | null) => unit?.status || "No Computing Unit")
    );
  }
  
  // Get whether the selected computing unit is in a ready state
  public isReady(): Observable<boolean> {
    return this.selectedUnitSubject.pipe(
      map((unit: DashboardWorkflowComputingUnit | null) => {
        // Must have a unit, the unit status must be Running, and the websocket must be connected
        return !!unit && 
               unit.status === "Running" && 
               this.workflowWebsocketService.isConnected;
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
    
    this.selectedUnitSubject.complete();
    this.allUnitsSubject.complete();
    this.connectedSubject.complete();
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
} 
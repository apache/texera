import { Injectable } from "@angular/core";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { Observable, of } from "rxjs";

@Injectable()
export class MockComputingUnitStatusService {
  listComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return of([]);
  }

  getSelectedComputingUnit(): Observable<DashboardWorkflowComputingUnit | null> {
    return of(null);
  }

  getSelectedComputingUnitValue(): DashboardWorkflowComputingUnit | null {
    return null;
  }

  getAllComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return of([]);
  }

  selectComputingUnit(): void {}

  startPolling(): void {}

  stopPolling(): void {}
}

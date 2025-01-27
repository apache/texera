import { Injectable } from "@angular/core";
import {HttpClient, HttpParams} from "@angular/common/http";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import {
  DashboardWorkflowComputingUnit,
  WorkflowComputingUnitMetrics
} from "../../types/workflow-computing-unit";

export const COMPUTING_UNIT_BASE_URL = "computing-unit";
export const COMPUTING_UNIT_METRICS_BASE_URL = "resource-metrics";
export const COMPUTING_UNIT_CREATE_URL = `${COMPUTING_UNIT_BASE_URL}/create`;
export const COMPUTING_UNIT_TERMINATE_URL = `${COMPUTING_UNIT_BASE_URL}/terminate`;
export const COMPUTING_UNIT_LIST_URL = `${COMPUTING_UNIT_BASE_URL}`;
export const COMPUTING_UNIT_METRIC_URL = `${COMPUTING_UNIT_METRICS_BASE_URL}/pod-metrics`

@Injectable({
  providedIn: "root",
})
export class WorkflowComputingUnitManagingService {
  constructor(private http: HttpClient) {}

  /**
   * Create a new workflow computing unit (pod).
   * @param name The name for the computing unit.
   * @param unitType
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  public createComputingUnit(name: string, unitType: string = "k8s_pod"): Observable<DashboardWorkflowComputingUnit> {
    const body = { name, unitType };

    return this.http.post<DashboardWorkflowComputingUnit>(
      `${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_CREATE_URL}`,
      body
    );
  }

  /**
   * Terminate a computing unit (pod) by its URI.
   * @returns An Observable of the server response.
   * @param uri
   */
  public terminateComputingUnit(uri: string): Observable<Response> {
    const body = { uri: uri, name: "dummy" };

    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_TERMINATE_URL}`, body);
  }

  /**
   * List all active computing units.
   * @returns An Observable of a list of WorkflowComputingUnit.
   */
  public listComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    return this.http.get<DashboardWorkflowComputingUnit[]>(
      `${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_LIST_URL}`
    );
  }

  /**
   * Get a computing units resource metrics
   *
   */
  public getComputingUnitMetrics(cuid: number): Observable<WorkflowComputingUnitMetrics> {
    let params = new HttpParams().set("computingUnitCUID",cuid)
    return this.http.get<WorkflowComputingUnitMetrics>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_METRIC_URL}`,  { params });
  }
}

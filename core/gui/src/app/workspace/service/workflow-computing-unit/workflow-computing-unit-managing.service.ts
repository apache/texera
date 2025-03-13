import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, of } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { environment } from "../../../../environments/environment";
import { assert } from "../../../common/util/assert";

export const COMPUTING_UNIT_BASE_URL = "computing-unit";
export const COMPUTING_UNIT_CREATE_URL = `${COMPUTING_UNIT_BASE_URL}/create`;
export const COMPUTING_UNIT_TERMINATE_URL = `${COMPUTING_UNIT_BASE_URL}/terminate`;
export const COMPUTING_UNIT_LIST_URL = `${COMPUTING_UNIT_BASE_URL}`;

@Injectable({
  providedIn: "root",
})
export class WorkflowComputingUnitManagingService {
  constructor(private http: HttpClient) {}

  /**
   * Create a new workflow computing unit (pod).
   * @param name The name for the computing unit.
   * @param cpuLimit The cpu resource limit for the computing unit.
   * @param memoryLimit The memory resource limit for the computing unit.
   * @param unitType
   * @returns An Observable of the created WorkflowComputingUnit.
   */
  public createComputingUnit(
    name: string,
    cpuLimit: string,
    memoryLimit: string,
    unitType: string = "k8s_pod"
  ): Observable<DashboardWorkflowComputingUnit> {
    const body = { name, cpuLimit, memoryLimit, unitType };

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
    assert(environment.computingUnitManagerEnabled, "computing unit manage is disabled.");
    const body = { uri: uri, name: "dummy" };

    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_TERMINATE_URL}`, body);
  }

  /**
   * List all active computing units.
   * @returns An Observable of a list of WorkflowComputingUnit.
   */
  public listComputingUnits(): Observable<DashboardWorkflowComputingUnit[]> {
    if (environment.computingUnitManagerEnabled) {
      return this.http.get<DashboardWorkflowComputingUnit[]>(
        `${AppSettings.getApiEndpoint()}/${COMPUTING_UNIT_LIST_URL}`
      );
    } else {
      // Create a default single WorkflowComputingUnit
      const defaultComputingUnit: DashboardWorkflowComputingUnit = {
        computingUnit: {
          cuid: 1,
          uid: 1,
          name: "Local Computing Unit",
          creationTime: Date.now(),
          terminateTime: undefined,
        },
        uri: "http://localhost:8085",
        status: "Running",
        metrics: {
          cpuUsage: "NaN",
          memoryUsage: "NaN",
        },
        resourceLimits: {
          cpuLimit: "NaN",
          memoryLimit: "NaN",
        },
      };

      return of([defaultComputingUnit]);
    }
  }
}

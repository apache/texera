import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { HttpClient } from "@angular/common/http";
import { WorkflowExecutionsEntry } from "../../type/workflow-executions-entry";

export const WORKFLOW_EXECUTIONS_API_BASE_URL = `${AppSettings.getApiEndpoint()}/executions`;

@Injectable({
  providedIn: "root",
})
export class WorkflowExecutionsService {
  constructor(private http: HttpClient) {}

  /**
   * retrieves a list of execution for a particular workflow from backend database
   */
  retrieveWorkflowExecutions(wid: number): Observable<WorkflowExecutionsEntry[]> {
    return this.http.get<WorkflowExecutionsEntry[]>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}`);
  }
}

import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
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

  groupSetIsBookmarked(wid: number, eIds: number[], isBookmarked: boolean): Observable<Object> {
    return this.http.put(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/set_execution_bookmarks`, {
      wid,
      eIds,
      isBookmarked,
    });
  }

  groupDeleteWorkflowExecutions(wid: number, eIds: number[]): Observable<Object> {
    return this.http.put(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/delete_executions`, {
      wid,
      eIds,
    });
  }

  updateWorkflowExecutionsName(wid: number | undefined, eId: number, executionName: string): Observable<Response> {
    return this.http.post<Response>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/update_execution_name`, {
      wid,
      eId,
      executionName,
    });
  }
}

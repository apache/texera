import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { HttpClient } from "@angular/common/http";
import { WorkflowExecutionsEntry } from "../../type/workflow-executions-entry";
import { NgbdModalWorkflowExecutionsComponent } from "../../component/feature-container/saved-workflow-section/ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";

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

  setIsBookmarked(wid: number, eId: number, isBookmarked: boolean): Observable<Object> {
    return this.http.put(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/set_execution_bookmark`, {
      wid,
      eId,
      isBookmarked,
    });
  }
}

import { Injectable } from "@angular/core";
import {Observable} from "rxjs";
import {AppSettings} from "../../../common/app-setting";
import {HttpClient} from "@angular/common/http";
import {WorkflowExecutionsEntry} from "../../type/workflow-executions-entry";

export const WORKFLOW_EXECUTIONS_API_BASE_URL = `${AppSettings.getApiEndpoint()}/executions`;
export const WORKFLOW_EXECUTIONS_NEW_ENTRY = WORKFLOW_EXECUTIONS_API_BASE_URL + "/new";

@Injectable({
  providedIn: "root",
})
export class WorkflowExecutionsService {
  constructor(private http: HttpClient) {}

  /**
   * Insert an entry in the database to indicate a new execution is running
   * @param wid the workflow id
   * @return message of success/failure
   */
  public startNewExecution(wid: number): Observable<Response> {
      return this.http.post<Response>(`${WORKFLOW_EXECUTIONS_NEW_ENTRY}/${wid}`, null);
  }

  /**
   * retrieves a list of execution for a particular workflow from backend database
   */
  retrieveWorkflowExecutions(wid: number): Observable<WorkflowExecutionsEntry[]> {
    return this.http.get<WorkflowExecutionsEntry[]>(
      `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}`
    );
  }
}

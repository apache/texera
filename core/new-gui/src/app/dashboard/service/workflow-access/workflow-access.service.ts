import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { Workflow } from "../../../common/type/workflow";
import { AccessEntry } from "../../type/access.interface";

export const WORKFLOW_ACCESS_URL = `${AppSettings.getApiEndpoint()}/workflow/access`;
export const WORKFLOW_ACCESS_GRANT_URL = WORKFLOW_ACCESS_URL + "/grant";
export const WORKFLOW_ACCESS_LIST_URL = WORKFLOW_ACCESS_URL + "/list";
export const WORKFLOW_ACCESS_REVOKE_URL = WORKFLOW_ACCESS_URL + "/revoke";
export const WORKFLOW_OWNER_URL = WORKFLOW_ACCESS_URL + "/owner";

@Injectable({
  providedIn: "root",
})
export class WorkflowAccessService {
  constructor(private http: HttpClient) {}

  /**
   * Assign a new access to/Modify an existing access of another user
   * @param workflow the workflow that is about to be shared
   * @param username the username of target user
   * @param accessLevel the type of access offered
   * @return hashmap indicating all current accesses, ex: {"Jim": "Write"}
   */
  public grantUserWorkflowAccess(workflow: Workflow, username: string, accessLevel: string): Observable<Response> {
    return this.http.post<Response>(`${WORKFLOW_ACCESS_GRANT_URL}/${workflow.wid}/${username}/${accessLevel}`, null);
  }

  /**
   * Retrieve all shared accesses of the given workflow
   * @param workflow the current workflow
   * @return message of success
   */
  public retrieveGrantedWorkflowAccessList(workflow: Workflow): Observable<ReadonlyArray<AccessEntry>> {
    return this.http.get<ReadonlyArray<AccessEntry>>(`${WORKFLOW_ACCESS_LIST_URL}/${workflow.wid}`);
  }

  /**
   * Remove an existing access of another user
   * @param workflow the current workflow
   * @param username the username of target user
   * @return message of success
   */
  public revokeWorkflowAccess(workflow: Workflow, username: string): Observable<Response> {
    return this.http.delete<Response>(`${WORKFLOW_ACCESS_REVOKE_URL}/${workflow.wid}/${username}`);
  }

  public getWorkflowOwner(workflow: Workflow): Observable<Readonly<{ ownerName: string }>> {
    return this.http.get<Readonly<{ ownerName: string }>>(`${WORKFLOW_OWNER_URL}/${workflow.wid}`);
  }
}

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { AccessEntry } from "../../type/access.interface";
import { WorkflowAccessLevel } from "../../type/dashboard-workflow-entry";
import { WorkflowMetadata } from "../../type/workflow-metadata.interface";

export const WORKFLOW_ACCESS_URL = `${AppSettings.getApiEndpoint()}/workflow/access`;
export const WORKFLOW_ACCESS_GRANT_URL = WORKFLOW_ACCESS_URL + "/grant";
export const WORKFLOW_ACCESS_LIST_URL = WORKFLOW_ACCESS_URL + "/list";
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
  public grantUserWorkflowAccess(workflow: WorkflowMetadata, username: string, accessLevel: WorkflowAccessLevel): Observable<void> {
    return this.http.post<void>(`${WORKFLOW_ACCESS_GRANT_URL}/${workflow.wid}/${username}/${accessLevel}`, null);
  }

  /**
   * Retrieve all shared accesses of the given workflow
   * @param workflow the current workflow
   * @return message of success
   */
  public retrieveGrantedWorkflowAccessList(workflow: WorkflowMetadata): Observable<ReadonlyArray<AccessEntry>> {
    return this.http.get<ReadonlyArray<AccessEntry>>(`${WORKFLOW_ACCESS_LIST_URL}/${workflow.wid}`);
  }

  public getWorkflowOwner(workflow: WorkflowMetadata): Observable<{ownerName: string}> {
    return this.http.get<{ownerName: string}>(`${WORKFLOW_OWNER_URL}/${workflow.wid}`);
  }
}

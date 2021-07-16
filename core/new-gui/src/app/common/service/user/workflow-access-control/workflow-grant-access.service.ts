import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { AppSettings } from '../../../app-setting';
import { Workflow } from '../../../type/workflow';


export const WORKFLOW_ACCESS_URL = 'workflow-access';
export const WORKFLOW_ACCESS_GRANT_URL = WORKFLOW_ACCESS_URL + '/grant';
export const WORKFLOW_ACCESS_LIST_URL = WORKFLOW_ACCESS_URL + '/list';
export const WORKFLOW_ACCESS_REVOKE_URL = WORKFLOW_ACCESS_URL + '/revoke';
export interface UserWorkflowAccess {
  userName: string;
  accessLevel: string;

}

@Injectable({
  providedIn: 'root'
})
export class WorkflowGrantAccessService {

  constructor(private http: HttpClient) {
  }

  /**
   * Assign a new access to/Modify an existing access of another user
   * @param workflow the workflow that is about to be shared
   * @param username the username of target user
   * @param accessLevel the type of access offered
   * @return hashmap indicating all current accesses, ex: {"Jim": "Write"}
   */
  public grantAccess(workflow: Workflow, username: string, accessLevel: string): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_GRANT_URL}/${workflow.wid}/${username}/${accessLevel}`, null);
  }

  /**
   * Retrieve all shared accesses of the given workflow
   * @param workflow the current workflow
   * @return message of success
   */
  public retrieveGrantedList(workflow: Workflow): Observable<Readonly<UserWorkflowAccess>[]> {
    return this.http.get<Readonly<UserWorkflowAccess>[]>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_LIST_URL}/${workflow.wid}`);
  }


  /**
   * Remove an existing access of another user
   * @param workflow the current workflow
   * @param username the username of target user
   * @return message of success
   */
  public revokeAccess(workflow: Workflow, username: string): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_REVOKE_URL}/${workflow.wid}/${username}`, null);
  }
}

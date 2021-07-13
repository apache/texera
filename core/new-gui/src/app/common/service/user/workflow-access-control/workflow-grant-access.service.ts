import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { map } from 'rxjs/operators';
import { AppSettings } from '../../../app-setting';
import { Workflow, WorkflowContent } from '../../../type/workflow';
import { jsonCast } from '../../../util/storage';

export const WORKFLOW_ACCESS_URL = 'workflowaccess';

@Injectable({
  providedIn: 'root'
})
export class WorkflowGrantAccessService {
  constructor(private http: HttpClient) { }

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid)
   * @param workflow
   * @param uid
   * @param accessType
   */
  public grantWorkflowAccess(workflow: Workflow, uid: number, accessType: string): void {
    console.log(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_URL}/share/${workflow.wid}/${uid}/${accessType}`);
    this.http.post<any>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_ACCESS_URL}/share/${workflow.wid}/${uid}/${accessType}`, null).subscribe();
  }


  public testConnection(): void{
    console.log("connected");
  }
}

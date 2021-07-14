import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { map } from 'rxjs/operators';
import { AppSettings } from '../../../app-setting';
import { Workflow, WorkflowContent } from '../../../type/workflow';
import { jsonCast } from '../../../util/storage';

export const WORKFLOW_URL = 'workflow';
export const WORKFLOW_PERSIST_URL = WORKFLOW_URL + '/persist';
export const WORKFLOW_LIST_URL = WORKFLOW_URL + '/list';
export const WORKFLOW_CREATE_URL = WORKFLOW_URL + '/create';

@Injectable({
  providedIn: 'root'
})
export class WorkflowPersistService {
  constructor(private http: HttpClient) {
  }

  /**
   * persists a workflow to backend database and returns its updated information (e.g., new wid)
   * @param workflow
   */
  public persistWorkflow(workflow: Workflow): Observable<Workflow> {
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_PERSIST_URL}`, {
      wid: workflow.wid,
      name: workflow.name,
      content: JSON.stringify(workflow.content)
    })
      .filter((updatedWorkflow: Workflow) => updatedWorkflow != null)
      .pipe(map(WorkflowPersistService.parseWorkflowInfo));
  }

  /**
   * creates a workflow and insert it to backend database and return its information
   * @param newWorkflowName
   * @param newWorkflowContent
   */
  public createWorkflow(newWorkflowContent: WorkflowContent, newWorkflowName: string = 'Untitled workflow'): Observable<Workflow> {
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_CREATE_URL}`, {
      name: newWorkflowName,
      content: JSON.stringify(newWorkflowContent)
    })
      .filter((createdWorkflow: Workflow) => createdWorkflow != null)
      .pipe(map(WorkflowPersistService.parseWorkflowInfo));
  }

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid, the workflow id.
   */
  public retrieveWorkflow(wid: number): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_URL}/${wid}`)
      .filter((workflow: Workflow) => workflow != null)
      .pipe(map(WorkflowPersistService.parseWorkflowInfo));
  }

  /**
   * retrieves a list of workflows from backend database that belongs to the user in the session.
   */
  public retrieveWorkflowsBySessionUser(): Observable<Workflow[]> {
    return this.http.get<Workflow[]>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_LIST_URL}`)
      .pipe(map((workflows: Workflow[]) => workflows.map(WorkflowPersistService.parseWorkflowInfo)));
  }

  /**
   * deletes the given workflow, the user in the session must own the workflow.
   */
  public deleteWorkflow(wid: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_URL}/${wid}`);
  }

  /**
   * helper function to parse WorkflowInfo from a JSON string. In some case, for example reading from backend, the content would returned
   * as a JSON string.
   * @param workflow
   * @private
   */
  private static parseWorkflowInfo(workflow: Workflow): Workflow {
    if (workflow != null && typeof workflow.content === 'string') {
      workflow.content = jsonCast<WorkflowContent>(workflow.content);
    }
    return workflow;
  }
}

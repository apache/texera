import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AppSettings } from '../../../app-setting';
import { WorkflowInfo, Workflow } from '../../../type/workflow';
import { Observable } from 'rxjs/Observable';
import { map } from 'rxjs/operators';
import { jsonCast } from '../../../util/storage';

export const WORKFLOW_URL = 'user/dictionary/validate';

@Injectable({
  providedIn: 'root'
})

export class WorkflowPersistService {

  constructor(public http: HttpClient) {
  }

  public persistWorkflow(workflow: Workflow): Observable<Workflow> {
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/workflow/persist`, {
      wid: workflow.wid,
      name: workflow.name,
      content: JSON.stringify(workflow.content)
    }).pipe(map(WorkflowPersistService.parseWorkflowInfo));
  }

  public retrieveWorkflow(workflowID: string): Observable<Workflow> {
    return this.http.get<Workflow>(`${AppSettings.getApiEndpoint()}/workflow/get/${workflowID}`)
      .pipe(map(WorkflowPersistService.parseWorkflowInfo));
  }

  public retrieveWorkflowsBySessionUser(): Observable<Workflow[]> {
    return this.http.get<Workflow[]>(`${AppSettings.getApiEndpoint()}/workflow/get`)
      .pipe(map((workflows: Workflow[]) => workflows.map(WorkflowPersistService.parseWorkflowInfo)));
  }


  public deleteWorkflow(workflow: Workflow) {
    return null;
  }

  private static parseWorkflowInfo(workflow: Workflow): Workflow {
    if (typeof workflow.content === 'string') {
      workflow.content = jsonCast<WorkflowInfo>(workflow.content);
    }
    return workflow;
  }
}

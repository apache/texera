import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {AppSettings} from '../../../app-setting';
import {Workflow} from '../../../type/workflow';
import {Observable} from 'rxjs/Observable';

export const WORKFLOW_URL = 'user/dictionary/validate';

@Injectable({
  providedIn: 'root'
})

export class WorkflowPersistService {

  constructor(public http: HttpClient) {
  }

  public saveWorkflow(savedWorkflow: string, workflowName: string, workflowID: string | null): Observable<Workflow> {
    const formData: FormData = new FormData();
    // TODO: change to use CacheWorkflowService.

    if (workflowID != null) {
      formData.append('wfId', workflowID);
    }
    formData.append('name', workflowName);
    formData.append('content', savedWorkflow);
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/workflow/save-workflow`, formData);
  }

  public getSavedWorkflows(): Observable<Workflow[]> {
    return this.http.get<Workflow[]>(
      `${AppSettings.getApiEndpoint()}/workflow/get`);
  }


  public deleteSavedWorkflow(deleteProject: Workflow) {
    return null;
  }
}

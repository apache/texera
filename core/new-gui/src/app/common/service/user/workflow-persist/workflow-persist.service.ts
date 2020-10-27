import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {AppSettings} from '../../../app-setting';
import {Workflow} from '../../../type/workflow';

export const WORKFLOW_URL = 'user/dictionary/validate';

@Injectable({
  providedIn: 'root'
})

export class WorkflowPersistService {

  constructor(public http: HttpClient) {
  }

  public saveWorkflow(savedWorkflow: string, workflowName: string) {
    const formData: FormData = new FormData();
    const currentWfId = localStorage.getItem('wfId');
    if (currentWfId != null) {
      formData.append('wfId', currentWfId);
    }
    formData.append('name', workflowName);
    formData.append('content', savedWorkflow);
    return this.http.post<Workflow>(`${AppSettings.getApiEndpoint()}/workflow/save-workflow`, formData);
  }
}

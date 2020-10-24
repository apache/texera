import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {AppSettings} from '../../../app-setting';
import {Observable} from 'rxjs';
import {WorkflowWebResponse} from '../../../type/workflow';

export const WORKFLOW_URL = 'user/dictionary/validate';

@Injectable({
  providedIn: 'root'
})

export class WorkflowPersistService {

  constructor(public http: HttpClient) {
  }

  public saveWorkflow(userID: number | undefined, savedWorkflow: string | null) {
    if (userID === undefined || savedWorkflow == null) {
      console.log('not it is null');
      return Observable.of(undefined);
    }
    const formData: FormData = new FormData();
    formData.append('userId', userID.toString());
    formData.append('workflowBody', savedWorkflow);
    this.http.post<WorkflowWebResponse>(
      `${AppSettings.getApiEndpoint()}/workflow/save-workflow`, formData).flatMap(
      res => {
        return Observable.of(res);
      }
    ).subscribe(
      (response) => {
        console.log('response received');
        localStorage.setItem('wfId', JSON.stringify(response.workflow.wfId));
      },
      (error) => {
        console.error('error caught in component' + error);
      });
  }
}

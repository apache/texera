import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {AppSettings} from '../../../app-setting';
import {Observable} from 'rxjs';
import {Workflow} from '../../../type/workflow';

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
    this.http.post<Workflow>(
      `${AppSettings.getApiEndpoint()}/workflow/save-workflow`, formData).flatMap(
      res => {
        return Observable.of(res);
      }
    ).subscribe(
      (workflow) => {
        console.log('response received');
        localStorage.removeItem('wfId');
        localStorage.setItem('wfId', JSON.stringify(workflow.wfId));
      },
      (error) => {
        console.error('error caught in component' + error);
      });
  }
}

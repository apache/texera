import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {GenericWebResponse} from '../../../type/generic-web-response';
import {AppSettings} from '../../../app-setting';
import {Observable} from 'rxjs';

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
    // formData.append('workflowID', userID.toString());
    formData.append('workflowBody', savedWorkflow);
    console.log(formData);
    console.log(`${AppSettings.getApiEndpoint()}/workflow/save-workflow`);
    this.http.post<GenericWebResponse>(
      `${AppSettings.getApiEndpoint()}/workflow/save-workflow`, formData).flatMap(
      res => {
        console.log('done');
        return Observable.of(res);
      }
    ).subscribe(res => console.log(res));
  }
}

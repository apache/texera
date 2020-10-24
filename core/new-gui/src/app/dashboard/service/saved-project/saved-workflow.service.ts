import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';
import {SavedWorkflow} from '../../type/saved-workflow';
import {Workflow} from '../../../common/type/workflow';
import {AppSettings} from '../../../common/app-setting';

/**
 * Saved Project service should be able to get all the
 * saved-project data from the back end for a specific user.
 * Users can also add a new project or delete an existing project
 * by calling methods in service.
 * Currently using a StubSavedProjectService to upload the mock
 * data to the dashboard.
 *
 * @author Zhaomin Li
 */
@Injectable()
export class SavedWorkflowService {

  constructor(private http: HttpClient) {
  }

  public getSavedWorkflows(): Observable<SavedWorkflow[]> {

    this.http.get<Workflow[]>(
      `${AppSettings.getApiEndpoint()}/workflow/get`).flatMap(
      res => {
        return Observable.of(res);
      }
    ).subscribe(
      (workflows) => {
        console.log(workflows);
      },
      (error) => {
        console.error('error caught in component' + error);
      });
    return Observable.of([]);
  }


  public deleteSavedProjectData(deleteProject: SavedWorkflow) {
    return null;
  }
}

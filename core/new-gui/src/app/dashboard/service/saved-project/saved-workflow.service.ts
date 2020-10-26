import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';
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

  public getSavedWorkflows(): Observable<Workflow[]> {

    return this.http.get<Workflow[]>(
      `${AppSettings.getApiEndpoint()}/workflow/get`);
  }


  public deleteSavedProjectData(deleteProject: Workflow) {
    return null;
  }
}

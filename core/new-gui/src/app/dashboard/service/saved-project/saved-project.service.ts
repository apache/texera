import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../type/saved-project';

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
export class SavedProjectService {

  constructor(private http: HttpClient) { }

  public getSavedProjectData(): Observable<SavedProject[]> {
    return Observable.of([]);
  }


  public deleteSavedProjectData(deleteProject: SavedProject) {
    return null;
  }
}

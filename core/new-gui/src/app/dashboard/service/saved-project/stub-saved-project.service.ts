import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';
import {SavedProject} from '../../type/saved-project';

import {MOCK_SAVED_PROJECT_LIST} from './mock-saved-project.data';

@Injectable()
export class StubSavedProjectService {

  constructor(private http: HttpClient) {
  }

  public getSavedProjectData(): Observable<SavedProject[]> {
    return Observable.of(MOCK_SAVED_PROJECT_LIST);
  }

  public deleteSavedProjectData(deleteProject: SavedProject) {
    return null;
  }
}

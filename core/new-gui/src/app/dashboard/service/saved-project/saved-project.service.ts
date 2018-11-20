import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../type/saved-project';

@Injectable()
export class SavedProjectService {

  constructor(private http: HttpClient) { }

  public getSavedProjectData(): Observable<SavedProject[]> {
    return Observable.of([]); // change
  }

  public deleteSavedProjectData(deleteProject: SavedProject) {
    return null;
  }
}

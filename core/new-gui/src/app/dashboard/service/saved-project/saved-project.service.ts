import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../type/saved-project';

@Injectable()
export class SavedProjectService {

  constructor(private http: Http) { }

  public getSavedProjectData(): Observable<SavedProject[]> {
    return null;
  }
}

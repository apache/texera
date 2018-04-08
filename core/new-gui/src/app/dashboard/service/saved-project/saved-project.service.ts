import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../type/saved-project';

@Injectable()
export class SavedProjectService {

  constructor() { }

  public getSavedProjectData(): Observable<SavedProject[]> {
    return null;
  }
}

import { TestBed, inject } from '@angular/core/testing';

import { SavedProjectService } from './saved-project.service';

import { HttpClient } from '@angular/common/http';

import { marbles} from 'rxjs-marbles';
import { Observable } from 'rxjs/Observable';

class StubHttpClient {
  constructor() { }
}

describe('SavedProjectService', () => {

  let service: SavedProjectService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        SavedProjectService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });

    service = TestBed.get(SavedProjectService);
  });

  it('should be created', inject([SavedProjectService], (injectedService: SavedProjectService) => {
    expect(injectedService).toBeTruthy();
  }));


  it('should return the same observable of array as expected if getSavedProjectData is called ', () => {
    const saveDataObservable = service.getSavedProjectData();
    console.log(saveDataObservable, Observable.of([]));

    // the current service test is in hard-coded style since there is no service with can give feedback

    saveDataObservable.subscribe(data => {
      expect(data).toEqual([]);
    });

  });
});

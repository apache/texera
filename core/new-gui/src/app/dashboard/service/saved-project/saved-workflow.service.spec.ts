import {inject, TestBed} from '@angular/core/testing';

import {SavedWorkflowService} from './saved-workflow.service';

import {HttpClient} from '@angular/common/http';

class StubHttpClient {
  constructor() { }
}

describe('SavedProjectService', () => {

  let service: SavedWorkflowService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        SavedWorkflowService,
        {provide: HttpClient, useClass: StubHttpClient}
      ]
    });

    service = TestBed.get(SavedWorkflowService);
  });

  it('should be created', inject([SavedWorkflowService], (injectedService: SavedWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));


  it('should return the same observable of array as expected if getSavedProjectData is called ', () => {
    const saveDataObservable = service.getSavedWorkflows();

    // the current service test is in hard-coded style since there is no service with can give feedback

    saveDataObservable.subscribe(data => {
      expect(data).toEqual([]);
    });

  });
});

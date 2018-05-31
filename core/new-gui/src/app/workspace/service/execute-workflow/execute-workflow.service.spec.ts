import { TestBed, inject } from '@angular/core/testing';

import { ExecuteWorkflowService } from './execute-workflow.service';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { Observable } from 'rxjs/Observable';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';
import { HttpClient } from '@angular/common/http';


class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public post(url: string, body: string, headers: Object): Observable<any> {
    return Observable.of(MOCK_RESULT_DATA).delay(1);
  }

}

describe('ExecuteWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient}
      ]
    });
  });

  it('should be created', inject([ExecuteWorkflowService], (service: ExecuteWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});

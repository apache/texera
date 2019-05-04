import { TestBed, inject } from '@angular/core/testing';

import { SaveWorkflowService, SavedWorkflow } from './save-workflow.service';
import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from '../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { marbles } from '../../../../../node_modules/rxjs-marbles';
import { OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { mockOperatorMetaData } from '../operator-metadata/mock-operator-metadata.data';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';

describe('SaveWorkflowService', () => {
  let autoSaveWorkflowService: SaveWorkflowService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        SaveWorkflowService,
        WorkflowActionService,
        JointUIService,
        WorkflowUtilService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService  },
        { provide: HttpClient}
      ]
    });
    autoSaveWorkflowService = TestBed.get(SaveWorkflowService);
  });

  fit('should be created', inject([SaveWorkflowService], (service: SaveWorkflowService) => {
    expect(service).toBeTruthy();
  }));
  it('should test if the link add event was being triggered or not', marbles((m) => {
  }));

  fit ('should trigger the operator added event when user add the operators', marbles((m) => {
    const testOperator = mockResultPredicate;
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const testPoint: Point = {x: 100, y: 100};
    const marbleValues = {
      operator: testOperator, offset: testPoint
    };
    spyOn(workflowActionService, 'addOperator').and.returnValue(
      m.hot('-a-', marbleValues)
    );

    const testStream = workflowActionService.getTexeraGraph().getOperatorAddStream().map(() => 'a');
    const expectedStream = m.hot('-a-');
    m.expect(testStream).toBeObservable(expectedStream);
  }));

  it ('should be reload into the link when user refreshes the page', marbles((m) => {

  }));

  it ('should be reload into the operator types when user refreshes the page', marbles((m) => {

  }));
});

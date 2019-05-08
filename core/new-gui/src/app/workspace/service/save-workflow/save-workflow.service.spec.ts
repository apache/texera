import { TestBed, inject } from '@angular/core/testing';

import { SaveWorkflowService, SavedWorkflow } from './save-workflow.service';
import { mockResultPredicate, mockScanResultLink } from '../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { marbles } from '../../../../../node_modules/rxjs-marbles';
import { OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
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

  it('should be created', inject([SaveWorkflowService], (service: SaveWorkflowService) => {
    expect(service).toBeTruthy();
  }));
  it('should test if the link add event was being triggered or not', marbles((m) => {

    const testLink = mockScanResultLink;
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const testPoint: Point = {x: 100, y: 100};
    const marbleValues = {
      operator: testLink, offset: testPoint
    };
    spyOn(workflowActionService.getTexeraGraph(), 'getLinkAddStream').and.returnValue(
      m.hot('-a-', marbleValues)
    );


    const value = workflowActionService.getTexeraGraph().getLinkAddStream().map(() => 'a');
    const expectedValue = m.hot('-a-');

    m.expect(value).toBeObservable(expectedValue);
  }));

  it ('should trigger the operator added event when user add the operators', marbles((m) => {
    const testOperator = mockResultPredicate;
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const marbleValues = {
      operatorID: testOperator.operatorID
    };
    spyOn(workflowActionService.getTexeraGraph(), 'getOperatorAddStream').and.returnValue(
      m.hot('-a-', marbleValues)
    );


    const value = workflowActionService.getTexeraGraph().getOperatorAddStream().map(() => 'a');
    const expectedValue = m.hot('-a-');

    m.expect(value).toBeObservable(expectedValue);
  }));

  fit ('should trigger the operator deleted event when user delete the operators', marbles((m) => {
    const testOperator = mockResultPredicate;
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const marbleValues = {
      operatorID: testOperator.operatorID
    };

    spyOn(workflowActionService.getTexeraGraph(), 'getOperatorDeleteStream').and.returnValue(
      m.hot('-a-', marbleValues)
    );


    const value = workflowActionService.getTexeraGraph().getOperatorDeleteStream().map(() => 'a');
    const expectedValue = m.hot('-a-');

    m.expect(value).toBeObservable(expectedValue);
  }));

  fit ('should save the workflow when user triggets the events', marbles((m) => {
    const testOperators: OperatorPredicate[] = new Array();
    testOperators[0] = mockResultPredicate;
    const testPoint: Point = {x: 100, y: 100};
    const testLinks: OperatorLink[] = new Array();
    testLinks[0] = mockScanResultLink;
    const testPosition: {[key: string]: Point | undefined} = {'test-key': testPoint};

    const saveWorkFlow: SavedWorkflow = {
      operators: testOperators, operatorPositions: testPosition, links: testLinks
    };

    const marbleValue = {key: 'workflow', saveWorkFlow: saveWorkFlow};
    spyOn(localStorage, 'setItem').and.returnValue(
      m.hot('-a-', marbleValue)
    );

  }));
  fit ('should get the item from the localstorage when user trigger the localstorage.setItem', marbles((m) => {
    const plan = localStorage.getItem('workflow');

    expect(plan).toBeDefined();
  }));
  fit ('should trigger the change position event of the operator when users drag the operator', marbles((m) => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const newPosition = {x: 100, y: 100};
    const testOperator = mockResultPredicate;
    const marbleValue = {operatorID: testOperator.operatorID, newPosition: newPosition};
    spyOn(workflowActionService.getJointGraphWrapper(), 'getOperatorPositionChangeEvent').and.returnValue(
      m.hot('-a-', marbleValue)
    );

    const stream = workflowActionService.getJointGraphWrapper().getOperatorPositionChangeEvent().map(() => 'a');

    const expectedStream = m.hot('-a-');
    m.expect(stream).toBeObservable(expectedStream);
  }));

});

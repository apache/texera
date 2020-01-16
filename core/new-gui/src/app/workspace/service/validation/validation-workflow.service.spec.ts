import { TestBed, inject } from '@angular/core/testing';
import { ValidationWorkflowService } from './validation-workflow.service';
import {
  mockScanPredicate, mockResultPredicate, mockScanResultLink, mockPoint
} from '../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '.././joint-ui/joint-ui.service';
import { marbles } from 'rxjs-marbles';

describe('ValidationWorkflowService', () => {
  let validationWorkflowService: ValidationWorkflowService;
  let workflowActionservice: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        UndoRedoService,
        ValidationWorkflowService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }]
    });

    validationWorkflowService = TestBed.get(ValidationWorkflowService);
    workflowActionservice = TestBed.get(WorkflowActionService);

  });

  it('should be created', inject([ValidationWorkflowService], (service: ValidationWorkflowService) => {
    expect(service).toBeTruthy();
  }));

  it('should receive true from validateOperator when operator box is connected and required properties are complete ',
  () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    const newProperty = { 'tableName': 'test-table' };
    workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    expect(validationWorkflowService.validateOperator(mockResultPredicate.operatorID)).toBeTruthy();
    expect(validationWorkflowService.validateOperator(mockScanPredicate.operatorID)).toBeTruthy();
  }
  );

  it('should subscribe the changes of validateOperatorStream when operator box is connected and required properties are complete ',
  marbles((m) => {
    const testEvents = m.hot('-a-b-c----d-', {
      'a': () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
      'b': () => workflowActionservice.addOperator(mockResultPredicate, mockPoint),
      'c': () => workflowActionservice.addLink(mockScanResultLink),
      'd': () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { 'tableName': 'test-table' })
    });

    testEvents.subscribe(action => action());

    const expected = m.hot('-u-v-(yz)-m-', {
      'u': {operatorID: '1', status: false},
      'v': {operatorID: '3', status: false},
      'y': {operatorID: '1', status: false},
      'z': {operatorID: '3', status: true},
      'm': {operatorID: '1', status: true}
    });

    m.expect(validationWorkflowService.getOperatorValidationStream()).toBeObservable(expected);
  }
  ));

  it('should receive false from validateOperator when operator box is not connected or required properties are not complete ',
  () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    expect(validationWorkflowService.validateOperator(mockResultPredicate.operatorID)).toBeTruthy();
    expect(validationWorkflowService.validateOperator(mockScanPredicate.operatorID)).toBeFalsy();
  });

  it('should subscribe the changes of validateOperatorStream when one operator box is deleted after valid status ',
  marbles((m) => {
    const testEvents = m.hot('-a-b-c----d-e-----', {
      'a': () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
      'b': () => workflowActionservice.addOperator(mockResultPredicate, mockPoint),
      'c': () => workflowActionservice.addLink(mockScanResultLink),
      'd': () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { 'tableName': 'test-table' }),
      'e': () => workflowActionservice.deleteOperator(mockResultPredicate.operatorID)
    });

    testEvents.subscribe(action => action());

    const expected = m.hot('-t-u-(vw)-x-(yz)-)', {
      't': {operatorID: '1', status: false},
      'u': {operatorID: '3', status: false},
      'v': {operatorID: '1', status: false},
      'w': {operatorID: '3', status: true},
      'x': {operatorID: '1', status: true},
      'y': {operatorID: '1', status: false}, // If one of the oprator is deleted, the other one is invaild since it is isolated
      'z': {operatorID: '3', status: false},

    });

    m.expect(validationWorkflowService.getOperatorValidationStream()).toBeObservable(expected);
  }
  ));

  it('should subscribe the changes of validateOperatorStream when operator link is deleted after valid status ',
  marbles((m) => {
    const testEvents = m.hot('-a-b-c----d-e----', {
      'a': () => workflowActionservice.addOperator(mockScanPredicate, mockPoint),
      'b': () => workflowActionservice.addOperator(mockResultPredicate, mockPoint),
      'c': () => workflowActionservice.addLink(mockScanResultLink),
      'd': () => workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, { 'tableName': 'test-table' }),
      'e': () => workflowActionservice.deleteLinkWithID('link-1')
    });

    testEvents.subscribe(action => action());

    const expected = m.hot('-t-u-(vw)-x-(yz)-', {
      't': {operatorID: '1', status: false},
      'u': {operatorID: '3', status: false},
      'v': {operatorID: '1', status: false},
      'w': {operatorID: '3', status: true},
      'x': {operatorID: '1', status: true},
      'y': {operatorID: '1', status: false}, // If the link is deleted, two operators are isolated and are invalid
      'z': {operatorID: '3', status: false}

    });

    m.expect(validationWorkflowService.getOperatorValidationStream()).toBeObservable(expected);
  }
  ));



});

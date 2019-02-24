import { TestBed, inject } from '@angular/core/testing';
import { ValidationWorkflowService } from './validation-workflow.service';
import {
  mockScanPredicate, mockResultPredicate, mockSentimentPredicate, mockScanResultLink,
  mockScanSentimentLink, mockSentimentResultLink, mockFalseResultSentimentLink, mockFalseSentimentScanLink,
  mockPoint
} from '../workflow-graph/model/mock-workflow-data';
import { mockExecutionEmptyResult } from '../execute-workflow/mock-result-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowGraph } from '../workflow-graph/model/workflow-graph';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '.././joint-ui/joint-ui.service';
import { marbles } from 'rxjs-marbles';
import { values } from 'lodash-es';
describe('ValidationWorkflowService', () => {
  let validationWorkflowService: ValidationWorkflowService;
  let workflowActionservice: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
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

  fit('should receive true from validateOperator when operator box is connected and required properties are complete ',
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

  fit('should receive false from validateOperator when operator box is not connected or required properties are not complete ',
  () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    expect(validationWorkflowService.validateOperator(mockResultPredicate.operatorID)).toBeTruthy();
    expect(validationWorkflowService.validateOperator(mockScanPredicate.operatorID)).toBeFalsy();
  });





});

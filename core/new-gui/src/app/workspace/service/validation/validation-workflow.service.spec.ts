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
describe('ValidationWorkflowService', () => {
  let validationWorkflowService: ValidationWorkflowService;
  let workflowActionservice: WorkflowActionService;
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;

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
    texeraGraph = (workflowActionservice as any).texeraGraph;
    jointGraph = (workflowActionservice as any).jointGraph;
  });

  it('should be created', inject([ValidationWorkflowService], (service: ValidationWorkflowService) => {
    expect(service).toBeTruthy();
  }));

  it('should receive true from operatorValidationStream when operator box is connected and required properties are complete ',
  () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);
    const newProperty = { table: 'test-table' };
    workflowActionservice.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    validationWorkflowService.getOperatorValidationStream().subscribe(value => {
    if (value.operatorID === '1') {
      expect(value.status).toBeTruthy();
    } else if (value.operatorID === '3') {
      expect(value.status).toBeTruthy();
    }
  });
  }
  );

  it('should receive false from operatorValidationStream when operator box is not connected or required properties are not complete ',
  () => {
    workflowActionservice.addOperator(mockScanPredicate, mockPoint);
    workflowActionservice.addOperator(mockResultPredicate, mockPoint);
    workflowActionservice.addLink(mockScanResultLink);

    validationWorkflowService.getOperatorValidationStream().subscribe(value => {
      if (value.operatorID === '1') {
        expect(value.status).toBeFalsy();
      } else if (value.operatorID === '3') {
        expect(value.status).toBeTruthy();
      }
    });
  }
  );



});

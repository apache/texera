import { JointGraphReadonly } from './../../../types/joint-graph';
import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly.interface';
import { WorkflowGraph } from './../../../types/workflow-graph';
import {
  mockScanPredicate, mockResultPredicate, mockSentimentPredicate, mockScanResultLink,
  mockScanSentimentLink, mockSentimentResultLink, mockFalseResultSentimentLink, mockFalseSentimentScanLink,
  mockPoint
} from './mock-workflow-data';
import { TestBed, inject } from '@angular/core/testing';

import { WorkflowActionService } from './workflow-action.service';
import { marbles } from 'rxjs-marbles';

describe('WorkflowActionService', () => {

  let service: WorkflowActionService;
  let texeraGraph: WorkflowGraphReadonly;
  let jointGraphWrapper: JointGraphReadonly;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowActionService]
    });
    service = TestBed.get(WorkflowActionService);
    texeraGraph = service.getTexeraGraph();
    jointGraphWrapper = service.getJointGraphWrapper();
  });

  it('should be created', inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));


});

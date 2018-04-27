import { WorkflowGraph } from './../../../types/workflow-graph';
import { mockScanSourcePredicate, mockPoint } from './mock-workflow-data';
import { TestBed, inject } from '@angular/core/testing';

import { WorkflowActionService } from './workflow-action.service';
import { marbles } from 'rxjs-marbles';

describe('WorkflowActionService', () => {

  let service: WorkflowActionService;
  let texeraGraph: WorkflowGraph;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowActionService]
    });
    service = TestBed.get(WorkflowActionService);
    texeraGraph = (service as any).texeraGraph;
  });

  it('should be created', inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should emit event when addOperator is called', marbles((m) => {
    const eventStream = '-e-';
    m.hot(eventStream).subscribe(
      event => service.addOperator(mockScanSourcePredicate, mockPoint)
    );

    const outputStream = service._onAddOperatorAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);
  }));

  it('should throw an error when adding an existed operator', () => {
    texeraGraph.addOperator(mockScanSourcePredicate);
    expect(() => {
      service.addOperator(mockScanSourcePredicate, mockPoint);
    }).toThrowError(new RegExp('already exists'));
  });

  it('should emit event when deleteOperator is called', marbles((m) => {
    texeraGraph.addOperator(mockScanSourcePredicate);

    const eventStream = '-e-';
    m.hot(eventStream).subscribe(
      event => service.deleteOperator(mockScanSourcePredicate.operatorID)
    );

    const outputStream = service._onDeleteOperatorAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);
  }));

});

import { WorkflowGraph } from './../../../types/workflow-graph';
import { getMockScanPredicate, getMockPoint } from './mock-workflow-data';
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
      event => service.addOperator(getMockScanPredicate(), getMockPoint())
    );

    const outputStream = service._onAddOperatorAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);
  }));

  it('should throw an error when adding an existed operator', () => {
    texeraGraph.addOperator(getMockScanPredicate());
    expect(() => {
      service.addOperator(getMockScanPredicate(), getMockPoint());
    }).toThrowError(new RegExp('already exists'));
  });

  it('should emit event when deleteOperator is called', marbles((m) => {
    texeraGraph.addOperator(getMockScanPredicate());

    const eventStream = '-e-';
    m.hot(eventStream).subscribe(
      event => service.deleteOperator(getMockScanPredicate().operatorID)
    );

    const outputStream = service._onDeleteOperatorAction().map(value => 'e');
    m.expect(outputStream).toBeObservable(eventStream);
  }));

});

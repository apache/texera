import { TestBed, inject } from '@angular/core/testing';

import { WorkflowActionService } from './workflow-action.service';

describe('WorkflowActionService', () => {

  let service: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowActionService]
    });
    service = TestBed.get(WorkflowActionService);
  });

  it('should be created', inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should emit event when addOperator is called', () => {
    let eventValue: any;
    service._onAddOperatorAction().subscribe(
      value => eventValue = value
    );

    service.addOperator(<any>'test', <any>'test');
    expect(eventValue).toBeTruthy();

  });

  it('should emit event when deleteOperator is called', () => {
    let eventValue: any;
    service._onDeleteOperatorAction().subscribe(
      value => eventValue = value
    );

    service.deleteOperator(<any>'test');
    expect(eventValue).toBeTruthy();

  });

});

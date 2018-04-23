import { TestBed, inject } from '@angular/core/testing';

import { WorkflowModelActionService } from './workflow-model-action.service';

describe('WorkflowModelActionService', () => {

  let service: WorkflowModelActionService = null;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowModelActionService]
    });
    service = TestBed.get(WorkflowModelActionService);
  });

  it('should be created', inject([WorkflowModelActionService], (injectedService: WorkflowModelActionService) => {
    expect(injectedService).toBeTruthy();
  }));

  it('should emit event when addOperator is called', () => {
    let eventValue: any;
    service.onAddOperatorAction().subscribe(
      value => eventValue = value
    );

    service.addOperator(<any>'test', <any>'test');
    expect(eventValue).toBeTruthy();

  });

  it('should emit event when deleteOperator is called', () => {
    let eventValue: any;
    service.onDeleteOperatorAction().subscribe(
      value => eventValue = value
    );

    service.deleteOperator(<any>'test');
    expect(eventValue).toBeTruthy();

  });

});

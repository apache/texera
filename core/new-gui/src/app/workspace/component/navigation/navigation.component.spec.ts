import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from "@angular/platform-browser";

import { NavigationComponent } from './navigation.component';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';

import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';

import { Observable } from 'rxjs/Observable';
import { StubExecuteWorkflowService } from '../../service/execute-workflow/stub-execute-workflow.service';
import { marbles, Context } from "rxjs-marbles";

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;
  let executeWorkFlowService: ExecuteWorkflowService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NavigationComponent ],
      imports: [CustomNgMaterialModule ],
      providers: [
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ExecuteWorkflowService, useClass: StubExecuteWorkflowService }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NavigationComponent);
    component = fixture.componentInstance;
    executeWorkFlowService = TestBed.get(ExecuteWorkflowService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should execute the workflow when run button is clicked', marbles((m) => {
    const runButtonElement = fixture.debugElement.query(By.css('.texera-workspace-navigation-run'));
    m.hot('-e-').do(event => runButtonElement.triggerEventHandler('click', null)).subscribe();

    const executionEndStream = executeWorkFlowService.getExecuteEndedStream().map(value => 'e');

    const expectedStream = '-e-';
    m.expect(executionEndStream).toBeObservable(expectedStream);

  }));

  it('should show spinner when the workflow execution begins and hide spinner when execution ends', marbles((m) => {
    // expect initially there is no spinner

    expect(component.showSpinner).toBeFalsy();
    let Spinner = fixture.debugElement.query(By.css('.texera-loading-spinner'));
    expect(Spinner).toBeFalsy();

    m.hot('-e-').do(event => component.onClickRun()).subscribe();

    executeWorkFlowService.getExecuteStartedStream().subscribe(
      event => {
        fixture.detectChanges();
        expect(component.showSpinner).toBeTruthy();
        Spinner = fixture.debugElement.query(By.css('.texera-loading-spinner'));
        expect(Spinner).toBeTruthy();
      }
    );

    executeWorkFlowService.getExecuteEndedStream().subscribe(
      event => {
        fixture.detectChanges();
        expect(component.showSpinner).toBeFalsy();
        let Spinner = fixture.debugElement.query(By.css('.texera-loading-spinner'));
        expect(Spinner).toBeFalsy();
      }
    );

  }));

});

import { RouterTestingModule } from '@angular/router/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { NavigationComponent } from './navigation.component';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { TourService } from 'ngx-tour-ng-bootstrap';

import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';

import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';

import { Observable } from 'rxjs/Observable';
import { marbles } from 'rxjs-marbles';
import { HttpClient } from '@angular/common/http';
import { mockExecutionResult } from '../../service/execute-workflow/mock-result-data';

class StubHttpClient {

  public post<T>(): Observable<string> { return Observable.of('a'); }

}

describe('NavigationComponent', () => {
  let component: NavigationComponent;
  let fixture: ComponentFixture<NavigationComponent>;
  let executeWorkFlowService: ExecuteWorkflowService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NavigationComponent],
      imports: [
        CustomNgMaterialModule,
        RouterTestingModule.withRoutes([]),
      ],
      providers: [
        WorkflowActionService,
        JointUIService,
        ExecuteWorkflowService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient },
        TourService,
      ]
    }).compileComponents();
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

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockExecutionResult)
    );


    const runButtonElement = fixture.debugElement.query(By.css('.texera-navigation-run-button'));
    m.hot('-e-').do(event => runButtonElement.triggerEventHandler('click', null)).subscribe();

    const executionEndStream = executeWorkFlowService.getExecuteEndedStream().map(value => 'e');

    const expectedStream = '-e-';
    m.expect(executionEndStream).toBeObservable(expectedStream);

  }));

  it('should show spinner when the workflow execution begins and hide spinner when execution ends', marbles((m) => {

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockExecutionResult)
    );

    // expect initially there is no spinner

    expect(component.showSpinner).toBeFalsy();
    let spinner = fixture.debugElement.query(By.css('.texera-navigation-loading-spinner'));
    expect(spinner).toBeFalsy();

    m.hot('-e-').do(() => component.onClickRun()).subscribe();

    executeWorkFlowService.getExecuteStartedStream().subscribe(
      () => {
        fixture.detectChanges();
        expect(component.showSpinner).toBeTruthy();
        spinner = fixture.debugElement.query(By.css('.texera-navigation-loading-spinner'));
        expect(spinner).toBeTruthy();
      }
    );

    executeWorkFlowService.getExecuteEndedStream().subscribe(
      () => {
        fixture.detectChanges();
        expect(component.showSpinner).toBeFalsy();
        spinner = fixture.debugElement.query(By.css('.texera-navigation-loading-spinner'));
        expect(spinner).toBeFalsy();
      }
    );

  }));

});

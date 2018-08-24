import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultPanelComponent } from './result-panel.component';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { CustomNgMaterialModule } from './../../../common/custom-ng-material.module';

import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { NgbModule, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { marbles } from 'rxjs-marbles';
import { mockExecutionResult, mockResultData,
  mockExecutionErrorResult, mockExecutionEmptyResult } from '../../service/execute-workflow/mock-result-data';
import { By } from '@angular/platform-browser';
import { Observable } from 'rxjs/Observable';
import { HttpClient } from '@angular/common/http';


class StubHttpClient {
  constructor() {}

  public post(): Observable<string> { return Observable.of('a'); }
}

describe('ResultPanelComponent', () => {
  let component: ResultPanelComponent;
  let fixture: ComponentFixture<ResultPanelComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let ngbModel: NgbModal;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResultPanelComponent ],
      imports: [
        NgbModule.forRoot(),
        CustomNgMaterialModule
      ],
      providers: [
        WorkflowActionService,
        JointUIService,
        ExecuteWorkflowService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.get(ExecuteWorkflowService);
    ngbModel = TestBed.get(NgbModal);
    fixture.detectChanges();
  });

  it('should create', () => {
    const messageDiv = fixture.debugElement.query(By.css('.texera-panel-message'));
    const tableDiv = fixture.debugElement.query(By.css('.result-table'));
    const tableHtmlElement: HTMLElement = tableDiv.nativeElement;

    // Tests to check if, initially, messageDiv does not exist while result-table
    //  exists but no visible.
    expect(messageDiv).toBeFalsy();
    expect(tableDiv).toBeTruthy();

    // We only test its attribute because the style isn't directly accessbile
    //  by the element, rather it was used through this attribute 'hidden'
    expect(tableHtmlElement.hasAttribute('hidden')).toBeTruthy();
    expect(component).toBeTruthy();
  });


  it('should change the content of result panel correctly', marbles((m) => {

    const endMarbleString = '-e-|';
    const endMarblevalues = {
      e: mockExecutionResult
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarblevalues)
    );

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);

    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete: () => {
        const mockColumns = Object.keys(mockResultData[0]);
        expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
        expect(testComponent.currentColumns).toBeTruthy();
        expect(testComponent.currentDataSource).toBeTruthy();
      }
    });

  }));

  it(`should create error message and update the Component's properties when the execution result size is 0`, marbles((m) => {
    const endMarbleString = '-e-|';
    const endMarbleValues = {
      e: mockExecutionEmptyResult
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarbleValues)
    );

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);
    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete: () => {
        expect(testComponent.message).toEqual(`execution doesn't have any results`);
        expect(testComponent.currentDataSource).toBeFalsy();
        expect(testComponent.currentColumns).toBeFalsy();
        expect(testComponent.currentDisplayColumns).toBeFalsy();
        expect(testComponent.showMessage).toBeTruthy();
      }
    });
  }));

  it(`should throw an error when displayResultTable() is called with execution result that has 0 size`, () => {

    // This is a way to get the private method in Components. Since this edge case can
    //  never be reached in the public method, this architecture is required.

    expect(() =>
      (component as any).displayResultTable(mockExecutionEmptyResult)
    ).toThrowError( new RegExp(`result data should not be empty`));

  });

  it('should respond to error and print error messages', marbles((m) => {
    const endMarbleString = '-e-|';
    const endMarbleValues = {
      e: mockExecutionErrorResult
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarbleValues)
    );

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);

    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete : () => {
        expect(testComponent.showMessage).toBeTruthy();
        expect(testComponent.message.length).toBeGreaterThan(0);
      }
    });

  }));

  it('should update the result panel when new execution result arrives', marbles((m) => {
    const endMarbleString = '-a-b-|';
    const endMarblevalues = {
      a: mockExecutionErrorResult,
      b: mockExecutionResult
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarblevalues)
    );

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);

    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete: () => {
        const mockColumns = Object.keys(mockResultData[0]);
        expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
        expect(testComponent.currentColumns).toBeTruthy();
        expect(testComponent.currentDataSource).toBeTruthy();
      }
    });
  }));

  it('should generate the result table correctly on the user interface', () => {

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockExecutionResult)
    );

    executeWorkflowService.getExecuteEndedStream().subscribe();

    executeWorkflowService.executeWorkflow();

    fixture.detectChanges();


    const resultTable = fixture.debugElement.query(By.css('.result-table'));
    expect(resultTable).toBeTruthy();
  });

});

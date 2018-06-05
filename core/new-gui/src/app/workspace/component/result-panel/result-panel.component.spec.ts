import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultPanelComponent } from './result-panel.component';
import { ExecuteWorkflowService } from "./../../service/execute-workflow/execute-workflow.service";
import { StubExecuteWorkflowService } from "./../../service/execute-workflow/stub-execute-workflow.service";
import { CustomNgMaterialModule } from "./../../../common/custom-ng-material.module";

import { WorkflowActionService } from "./../../service/workflow-graph/model/workflow-action.service";
import { JointUIService } from "./../../service/joint-ui/joint-ui.service";
import { OperatorMetadataService } from "./../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "./../../service/operator-metadata/stub-operator-metadata.service";
import { NgbModule, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { marbles } from 'rxjs-marbles';
import { MOCK_EXECUTION_RESULT, MOCK_RESULT_DATA, MOCK_EXECUTION_ERROR } from '../../service/execute-workflow/mock-result-data';
import { MatTableDataSource } from '@angular/material';
import { By } from '@angular/platform-browser';


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
        { provide: ExecuteWorkflowService, useClass: StubExecuteWorkflowService },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
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
    expect(component).toBeTruthy();
  });


  it('should change the content of result panel correctly', marbles((m) => {

    const endMarbleString = '-e-|';
    const endMarblevalues = {
      e: MOCK_EXECUTION_RESULT
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarblevalues)
    )

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);

    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete: () => {
        const mockColumns = Object.keys(MOCK_RESULT_DATA[0]);
        expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
        expect(testComponent.currentColumns).toBeTruthy();
        expect(testComponent.currentDataSource).toBeTruthy();
      }
    });

  }));

  it('should respond to error and print error messages', marbles((m) => {
    const endMarbleString = '-e-|';
    const endMarbleValues = {
      e: MOCK_EXECUTION_ERROR
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
    })

  }));

  it('should update the result panel when new execution result arrives', marbles((m) => {
    const endMarbleString = '-a-b-|';
    const endMarblevalues = {
      a: MOCK_EXECUTION_ERROR,
      b: MOCK_EXECUTION_RESULT
    };

    spyOn(executeWorkflowService, 'getExecuteEndedStream').and.returnValue(
      m.hot(endMarbleString, endMarblevalues)
    );

    const testComponent = new ResultPanelComponent(executeWorkflowService, ngbModel);

    executeWorkflowService.getExecuteEndedStream().subscribe({
      complete: () => {
        const mockColumns = Object.keys(MOCK_RESULT_DATA[0]);
        expect(testComponent.currentDisplayColumns).toEqual(mockColumns);
        expect(testComponent.currentColumns).toBeTruthy();
        expect(testComponent.currentDataSource).toBeTruthy();
      }
    });

  }));

  it('should generate the result table correctly on the user interface', marbles((m) => {

    executeWorkflowService.executeWorkflow();

    fixture.detectChanges();


    const resultTable = fixture.debugElement.query(By.css('.result-table'));
    expect(resultTable).toBeTruthy();

  }));

  it('should successfully open the row detail modal when a row from the result is clicked', marbles((m) => {
    executeWorkflowService.executeWorkflow();
    fixture.detectChanges();

    const resultTableRow = fixture.debugElement.query(By.css('.result-table-row'));
    expect(resultTableRow).toBeTruthy();

    resultTableRow.triggerEventHandler('click', null);

    expect(component.currentDisplayRow).toBeTruthy();



  }));

});

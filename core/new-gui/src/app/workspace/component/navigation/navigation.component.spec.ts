// import { RouterTestingModule } from '@angular/router/testing';
// import { async, ComponentFixture, TestBed } from '@angular/core/testing';
// import { By } from '@angular/platform-browser';

// import { NavigationComponent } from './navigation.component';
// import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
// import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
// import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
// import { ValidationWorkflowService } from '../../service/validation/validation-workflow.service';

// import { CustomNgMaterialModule } from '../../../common/custom-ng-material.module';

// import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
// import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
// import { JointUIService } from '../../service/joint-ui/joint-ui.service';

// import { Observable } from 'rxjs';
// import { marbles } from 'rxjs-marbles';
// import { HttpClient } from '@angular/common/http';
// import { mockExecutionResult } from '../../service/execute-workflow/mock-result-data';
// import { JointGraphWrapper } from '../../service/workflow-graph/model/joint-graph-wrapper';
// import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';
// import { mockScanPredicate, mockPoint } from '../../service/workflow-graph/model/mock-workflow-data';
// import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
// import { environment } from '../../../../environments/environment';

// class StubHttpClient {

//   public post<T>(): Observable<string> { return Observable.of('a'); }
//   public get<T>(): Observable<string> { return Observable.of('a'); }

// }

// describe('NavigationComponent', () => {
//   let component: NavigationComponent;
//   let fixture: ComponentFixture<NavigationComponent>;
//   let executeWorkFlowService: ExecuteWorkflowService;
//   let workflowActionService: WorkflowActionService;
//   let workflowStatusService: WorkflowStatusService;
//   let undoRedoService: UndoRedoService;
//   let validationWorkflowService: ValidationWorkflowService;
//   beforeEach(async(() => {
//     TestBed.configureTestingModule({
//       declarations: [NavigationComponent],
//       imports: [
//         CustomNgMaterialModule,
//         RouterTestingModule.withRoutes([]),
//       ],
//       providers: [
//         WorkflowActionService,
//         WorkflowUtilService,
//         JointUIService,
//         ExecuteWorkflowService,
//         UndoRedoService,
//         ValidationWorkflowService,
//         { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
//         { provide: HttpClient, useClass: StubHttpClient },
//         WorkflowStatusService
//       ]
//     }).compileComponents();
//   }));

//   beforeEach(() => {
//     fixture = TestBed.createComponent(NavigationComponent);
//     component = fixture.componentInstance;
//     executeWorkFlowService = TestBed.get(ExecuteWorkflowService);
//     workflowActionService = TestBed.get(WorkflowActionService);
//     workflowStatusService = TestBed.get(WorkflowStatusService);
//     undoRedoService = TestBed.get(UndoRedoService);
//     validationWorkflowService = TestBed.get(ValidationWorkflowService);
//     fixture.detectChanges();
//     environment.pauseResumeEnabled = true;
//   });

//   it('should create', () => {
//     expect(component).toBeTruthy();
//   });

//   // it('should execute the workflow when run button is clicked', marbles((m) => {

//   //   const httpClient: HttpClient = TestBed.get(HttpClient);
//   //   spyOn(httpClient, 'post').and.returnValue(
//   //     Observable.of(mockExecutionResult)
//   //   );

//   //   const runButtonElement = fixture.debugElement.query(By.css('.texera-navigation-run-button'));
//   //   m.hot('-e-').do(event => runButtonElement.triggerEventHandler('click', null)).subscribe();

//   //   const executionEndStream = executeWorkFlowService.getExecuteEndedStream().map(value => 'e');

//   //   const expectedStream = '-e-';
//   //   m.expect(executionEndStream).toBeObservable(expectedStream);

//   // }));

//   // it('should show pause/resume button when the workflow execution begins and hide the button when execution ends', marbles((m) => {

//   //   const httpClient: HttpClient = TestBed.get(HttpClient);
//   //   spyOn(httpClient, 'post').and.returnValue(
//   //     Observable.of(mockExecutionResult)
//   //   );

//   //   expect(component.isWorkflowRunning).toBeFalsy();
//   //   expect(component.isWorkflowPaused).toBeFalsy();

//   //   executeWorkFlowService.getExecuteStartedStream().subscribe(
//   //     () => {
//   //       fixture.detectChanges();
//   //       expect(component.isWorkflowRunning).toBeTruthy();
//   //       expect(component.isWorkflowPaused).toBeFalsy();
//   //     }
//   //   );

//   //   executeWorkFlowService.getExecuteEndedStream().subscribe(
//   //     () => {
//   //       fixture.detectChanges();
//   //       expect(component.isWorkflowRunning).toBeFalsy();
//   //       expect(component.isWorkflowPaused).toBeFalsy();
//   //     }
//   //   );

//   //   m.hot('-e-').do(() => component.onButtonClick()).subscribe();

//   // }));

//   // it('should call pauseWorkflow function when isWorkflowPaused is false', () => {
//   //   const pauseWorkflowSpy = spyOn(executeWorkFlowService, 'pauseWorkflow').and.callThrough();
//   //   component.isWorkflowRunning = true;
//   //   component.isWorkflowPaused = false;

//   //   (executeWorkFlowService as any).workflowExecutionID = 'MOCK_EXECUTION_ID';

//   //   component.onButtonClick();
//   //   expect(pauseWorkflowSpy).toHaveBeenCalled();
//   // });

//   // it('should call resumeWorkflow function when isWorkflowPaused is true', () => {
//   //   const resumeWorkflowSpy = spyOn(executeWorkFlowService, 'resumeWorkflow').and.callThrough();
//   //   component.isWorkflowRunning = true;
//   //   component.isWorkflowPaused = true;

//   //   (executeWorkFlowService as any).workflowExecutionID = 'MOCK_EXECUTION_ID';

//   //   component.onButtonClick();
//   //   expect(resumeWorkflowSpy).toHaveBeenCalled();
//   // });

//   // it('should not call resumeWorkflow or pauseWorkflow if the workflow is not currently running', () => {

//   //   const httpClient: HttpClient = TestBed.get(HttpClient);
//   //   spyOn(httpClient, 'post').and.returnValue(
//   //     Observable.of(mockExecutionResult)
//   //   );

//   //   const pauseWorkflowSpy = spyOn(executeWorkFlowService, 'pauseWorkflow').and.callThrough();
//   //   const resumeWorkflowSpy = spyOn(executeWorkFlowService, 'resumeWorkflow').and.callThrough();

//   //   component.onButtonClick();
//   //   expect(pauseWorkflowSpy).toHaveBeenCalledTimes(0);
//   //   expect(resumeWorkflowSpy).toHaveBeenCalledTimes(0);
//   // });

//   // it('should not call downloadExecutionResult if there is no valid execution result currently', () => {
//   //   const httpClient: HttpClient = TestBed.get(HttpClient);
//   //   spyOn(httpClient, 'post').and.returnValue(
//   //     Observable.of(mockExecutionResult)
//   //   );

//   //   const downloadExecutionSpy = spyOn(executeWorkFlowService, 'downloadWorkflowExecutionResult').and.callThrough();

//   //   component.onClickDownloadExecutionResult('txt');
//   //   expect(downloadExecutionSpy).toHaveBeenCalledTimes(0);
//   // });

//   // it('it should update isWorkflowPaused variable to true when 0 is returned from getExecutionPauseResumeStream', marbles((m) => {
//   //   const endMarbleString = '-e-|';
//   //   const endMarblevalues = {
//   //     e: 0
//   //   };

//   //   spyOn(executeWorkFlowService, 'getExecutionPauseResumeStream').and.returnValue(
//   //     m.hot(endMarbleString, endMarblevalues)
//   //   );

//   //   const mockComponent = new NavigationComponent(executeWorkFlowService,
//   //     workflowActionService, workflowStatusService, undoRedoService, validationWorkflowService);

//   //   executeWorkFlowService.getExecutionPauseResumeStream()
//   //     .subscribe({
//   //       complete: () => {
//   //         expect(mockComponent.isWorkflowPaused).toBeTruthy();
//   //       }
//   //     });
//   // }));

//   // it('it should update isWorkflowPaused variable to false when 1 is returned from getExecutionPauseResumeStream', marbles((m) => {
//   //   const endMarbleString = '-e-|';
//   //   const endMarblevalues = {
//   //     e: 1
//   //   };

//   //   spyOn(executeWorkFlowService, 'getExecutionPauseResumeStream').and.returnValue(
//   //     m.hot(endMarbleString, endMarblevalues)
//   //   );

//   //   const mockComponent = new NavigationComponent(executeWorkFlowService,
//   //     workflowActionService, workflowStatusService, undoRedoService, validationWorkflowService);

//   //   executeWorkFlowService.getExecutionPauseResumeStream()
//   //     .subscribe({
//   //       complete: () => {
//   //         expect(mockComponent.isWorkflowPaused).toBeFalsy();
//   //       }
//   //     });
//   // }));

//   it('should change zoom to be smaller when user click on the zoom out buttons', marbles((m) => {
//      // expect initially the zoom ratio is 1;
//    const originalZoomRatio = 1;

//    m.hot('-e-').do(() => component.onClickZoomOut()).subscribe();
//    workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(
//      newRatio => {
//        fixture.detectChanges();
//        expect(newRatio).toBeLessThan(originalZoomRatio);
//        expect(newRatio).toEqual(originalZoomRatio - JointGraphWrapper.ZOOM_CLICK_DIFF);
//      }
//    );

//   }));

//   it('should change zoom to be bigger when user click on the zoom in buttons', marbles((m) => {

//     // expect initially the zoom ratio is 1;
//     const originalZoomRatio = 1;

//     m.hot('-e-').do(() => component.onClickZoomIn()).subscribe();
//     workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(
//       newRatio => {
//         fixture.detectChanges();
//         expect(newRatio).toBeGreaterThan(originalZoomRatio);
//         expect(newRatio).toEqual(originalZoomRatio + JointGraphWrapper.ZOOM_CLICK_DIFF);
//       }
//     );
//   }));

//   it('should execute the zoom in function when the user click on the Zoom In button', marbles((m) => {
//     m.hot('-e-').do(event => component.onClickZoomIn()).subscribe();
//     const zoomEndStream = workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().map(value => 'e');
//     const expectedStream = '-e-';
//     m.expect(zoomEndStream).toBeObservable(expectedStream);
//   }));

//   it('should execute the zoom out function when the user click on the Zoom Out button', marbles((m) => {
//     m.hot('-e-').do(event => component.onClickZoomOut()).subscribe();
//     const zoomEndStream = workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().map(value => 'e');
//     const expectedStream = '-e-';
//     m.expect(zoomEndStream).toBeObservable(expectedStream);
//   }));

//   it('should not increase zoom ratio when the user click on the zoom in button if zoom ratio already reaches maximum', marbles((m) => {
//     workflowActionService.getJointGraphWrapper().setZoomProperty(JointGraphWrapper.ZOOM_MAXIMUM);
//     m.hot('-e-').do(() => component.onClickZoomIn()).subscribe();
//     const zoomEndStream = workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().map(value => 'e');
//     const expectedStream = '---';
//     m.expect(zoomEndStream).toBeObservable(expectedStream);
//   }));

//   it('should not decrease zoom ratio when the user click on the zoom out button if zoom ratio already reaches minimum', marbles((m) => {
//     workflowActionService.getJointGraphWrapper().setZoomProperty(JointGraphWrapper.ZOOM_MINIMUM);
//     m.hot('-e-').do(() => component.onClickZoomOut()).subscribe();
//     const zoomEndStream = workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().map(value => 'e');
//     const expectedStream = '---';
//     m.expect(zoomEndStream).toBeObservable(expectedStream);
//   }));

//   it('should execute restore default when the user click on restore button', marbles((m) => {
//     m.hot('-e-').do(event => component.onClickRestoreZoomOffsetDefaullt()).subscribe();
//     const restoreEndStream = workflowActionService.getJointGraphWrapper().getRestorePaperOffsetStream().map(value => 'e');
//     const expectStream = '-e-';
//     m.expect(restoreEndStream).toBeObservable(expectStream);
//   }));

//   it('should delete all operators on the graph when user clicks on the delete all button', marbles((m) => {
//     m.hot('-e-').do(() => {
//       workflowActionService.addOperator(mockScanPredicate, mockPoint);
//       component.onClickDeleteAllOperators();
//     }).subscribe();
//     expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(0);
//   }));

//   // // TODO: this test case related to websocket is not stable, find out why and fix it
//   // xdescribe('when executionStatus is enabled', () => {
//   //   beforeAll(() => {
//   //     environment.executionStatusEnabled = true;
//   //   });

//   //   afterAll(() => {
//   //     environment.executionStatusEnabled = false;
//   //   });

//   //   it('should send workflowId to websocket when run button is clicked', () => {
//   //     const checkWorkflowSpy = spyOn(workflowStatusService, 'checkStatus').and.stub();
//   //     component.onButtonClick();
//   //     expect(checkWorkflowSpy).toHaveBeenCalled();
//   //   });
//   // });

// });

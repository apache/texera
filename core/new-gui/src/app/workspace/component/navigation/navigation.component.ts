import { Component, OnInit, NgModule } from '@angular/core';
import { ExecuteWorkflowService } from './../../service/execute-workflow/execute-workflow.service';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { environment } from '../../../../environments/environment';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { JointGraphWrapper } from '../../service/workflow-graph/model/joint-graph-wrapper';

import { ExecutionResult } from './../../types/execute-workflow.interface';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { WebsocketService} from '../../service/websocket/websocket.service';
import {NgbModal, NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import { Observable } from 'rxjs';

/**
 * NavigationComponent is the top level navigation bar that shows
 *  the Texera title and workflow execution button
 *
 * This Component will be the only Component capable of executing
 *  the workflow in the WorkflowEditor Component.
 *
 * Clicking the run button on the top-right hand corner will begin
 *  the execution. During execution, the run button will be replaced
 *  with a pause/resume button to show that graph is under execution.
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
 */
@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})

export class NavigationComponent implements OnInit {
  public static autoSaveState = 'Saved';
  public isWorkflowRunning: boolean = false; // set this to true when the workflow is started
  public isWorkflowPaused: boolean = false; // this will be modified by clicking pause/resume while the workflow is running

  // variable binded with HTML to decide if the running spinner should show
  public showSpinner = false;
  public executionResultID: string | undefined;
  public showDataResultID: boolean = false;
  constructor(private executeWorkflowService: ExecuteWorkflowService,
    public tourService: TourService, private modalService: NgbModal,
    private workflowActionService: WorkflowActionService,
    private workflowStatusService: WorkflowStatusService,
    ) {
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    executeWorkflowService.getExecuteEndedStream().subscribe(
      executionResult => {
        // update execution result ID for downloading if execution is valid
        this.handleResultData(executionResult);
        this.isWorkflowRunning = false;
        this.isWorkflowPaused = false;
      },
      () => {
        this.executionResultID = undefined;
        this.showDataResultID = false;
        this.isWorkflowRunning = false;
        this.isWorkflowPaused = false;
      }
    );

    // update the pause/resume button after a pause/resume request
    //  is returned from the backend.
    // this will swap button between pause and resume
    executeWorkflowService.getExecutionPauseResumeStream()
      .subscribe(state => this.isWorkflowPaused = (state === 0));

    workflowStatusService.status
      .subscribe(msg => console.log('Engine Current Status: ' + msg));
  }

  ngOnInit() {
  }
  /**
   * Executes the current existing workflow on the JointJS paper. It will
   *  also set the `isWorkflowRunning` variable to true to show that the backend
   *  is loading the workflow by displaying the pause/resume button.
   */
  public onButtonClick(): void {
    if (! environment.pauseResumeEnabled) {
      if (! this.isWorkflowRunning) {
        this.isWorkflowRunning = true;
        this.executeWorkflowService.executeWorkflow();
      }
    } else {
      if (!this.isWorkflowRunning && !this.isWorkflowPaused) {
        // when a new workflow begins, reset the execution result ID.
        this.executionResultID = undefined;
        this.showDataResultID = true;
        this.isWorkflowRunning = true;
        const workflowId = this.executeWorkflowService.executeWorkflow();
        console.log('checking status of workfolw: ', workflowId);
        this.checkStatus(workflowId);
      } else if (this.isWorkflowRunning && this.isWorkflowPaused) {
        this.executeWorkflowService.resumeWorkflow();
      } else if (this.isWorkflowRunning && !this.isWorkflowPaused) {
        this.executeWorkflowService.pauseWorkflow();
      } else {
        throw new Error('internal error: workflow cannot be both running and paused');
      }
    }
  }
  public getRunButtonText(): string {
    if (! environment.pauseResumeEnabled) {
      return 'Run';
    } else {
      if (!this.isWorkflowRunning && !this.isWorkflowPaused) {
        return 'Run';
      } else if (this.isWorkflowRunning && this.isWorkflowPaused) {
        return 'Resume';
      } else if (this.isWorkflowRunning && !this.isWorkflowPaused) {
        return 'Pause';
      } else {
        throw new Error('internal error: workflow cannot be both running and paused');
      }
    }
  }

  public runSpinner(): boolean {
    if (! environment.pauseResumeEnabled) {
      if (this.isWorkflowRunning && !this.isWorkflowPaused) {
        return true;
      } else {
        return false;
      }
    } else {
      if (!this.isWorkflowRunning && !this.isWorkflowPaused) {
        return false;
      } else if (this.isWorkflowRunning && this.isWorkflowPaused) {
        return false;
      } else if (this.isWorkflowRunning && !this.isWorkflowPaused) {
        return true;
      } else {
        throw new Error('internal error: workflow cannot be both running and paused');
      }
    }
  }

  /**
   * This method checks whether the zoom ratio reaches minimum. If it is minimum, this method
   *  will disable the zoom out button on the navigation bar.
   */
  public isZoomRatioMin(): boolean {
    return this.workflowActionService.getJointGraphWrapper().isZoomRatioMin();
  }

  /**
   * This method checks whether the zoom ratio reaches maximum. If it is maximum, this method
   *  will disable the zoom in button on the navigation bar.
   */
  public isZoomRatioMax(): boolean {
    return this.workflowActionService.getJointGraphWrapper().isZoomRatioMax();
  }

  /**
   * This method will decrease the zoom ratio and send the new zoom ratio value
   *  to the joint graph wrapper to change overall zoom ratio that is used in
   *  zoom buttons and mouse wheel zoom.
   *
   * If the zoom ratio already reaches minimum, this method will not do anything.
   */
  public onClickZoomOut(): void {

    // if zoom is already at minimum, don't zoom out again.
    if (this.isZoomRatioMin()) { return; }

    // make the ratio small.
    this.workflowActionService.getJointGraphWrapper()
      .setZoomProperty(this.workflowActionService.getJointGraphWrapper().getZoomRatio() - JointGraphWrapper.ZOOM_CLICK_DIFF);
  }

  /**
   * This method will increase the zoom ratio and send the new zoom ratio value
   *  to the joint graph wrapper to change overall zoom ratio that is used in
   *  zoom buttons and mouse wheel zoom.
   *
   * If the zoom ratio already reaches maximum, this method will not do anything.
   */
  public onClickZoomIn(): void {

    // if zoom is already reach maximum, don't zoom in again.
    if (this.isZoomRatioMax()) { return; }

    // make the ratio big.
    this.workflowActionService.getJointGraphWrapper()
      .setZoomProperty(this.workflowActionService.getJointGraphWrapper().getZoomRatio() + JointGraphWrapper.ZOOM_CLICK_DIFF);
  }

  /**
   * This is the handler for the execution result download button.
   *
   * This sends the finished execution result ID to the backend to download execution result in
   *  excel format.
   */
  public onClickDownloadExecutionResult(downloadType: string): void {
    // If there is no valid executionResultID to download from right now, exit immediately
    if (this.executionResultID === undefined) {
      return;
    }
    this.executeWorkflowService.downloadWorkflowExecutionResult(this.executionResultID, downloadType);
  }

  /**
   * Restore paper default zoom ratio and paper offset
   */
  public onClickRestoreZoomOffsetDefaullt(): void {
    this.workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset();
  }

  /**
   * this function is for show the infomation of each operators in a workflow
   */
  public loadOperatorsData(): void {
    // get the workflow information from executeWorkflowService
    if (!this.isWorkflowRunning) {
      const workflowID = this.executeWorkflowService.executeWorkflow();
      console.log('workflow ID is : ', workflowID);
      const modelRef = this.modalService.open(NagivationNgbModalComponent);
      Observable.from(modelRef.result)
        .filter(userDecision => userDecision === true)
        .subscribe(() => {

        },
        error => {}
        );
    }

  }
  /**
   * Handler for the execution result to extract successful execution ID
   */
  private handleResultData(response: ExecutionResult): void {
    // backend returns error, display error message
    if (response.code === 1) {
      this.executionResultID = undefined;
      return;
    }

    // execution success, but result is empty, also display message
    if (response.result.length === 0) {
      this.executionResultID = undefined;
      return;
    }

    // set the current execution result ID to the result ID
    this.executionResultID = response.resultID;
  }

  private checkStatus(workflowId: string) {
    console.log('frontend send new message to engine: ', workflowId);
    this.workflowStatusService.status.next(workflowId);
  }
}

/**
 *
 * NgbModalComponent is the pop-up window that will be
 *  displayed when the user clicks on a specific row
 *  to show the displays of that row.
 *
 * User can exit the pop-up window by
 *  1. Clicking the dismiss button on the top-right hand corner
 *      of the Modal
 *  2. Clicking the `Close` button at the bottom-right
 *  3. Clicking any shaded area that is not the pop-up window
 *  4. Pressing `Esc` button on the keyboard
 */
@Component({
  selector: 'texera-navigation-ngbmodal',
  templateUrl: './navigation-modal.component.html',
  styleUrls: ['./navigation.component.scss'],
  providers: [WorkflowStatusService, WebsocketService]
})
export class NagivationNgbModalComponent {
  public showWorkflowDataResult: string | undefined;
  constructor(public activeModal: NgbActiveModal,
    public workflowStatusService: WorkflowStatusService) {
      this.workflowStatusService.status
          .subscribe(msg => {
            if (msg !== null) {
              this.showWorkflowDataResult = msg;
            }
          });
    }

  // public confirmDelete() {
  //   this.activeModal.close(true);
  // }
}

import {Component, OnInit} from '@angular/core';
import {ExecuteWorkflowService} from '../../service/execute-workflow/execute-workflow.service';
import {UndoRedoService} from '../../service/undo-redo/undo-redo.service';
import {TourService} from 'ngx-tour-ng-bootstrap';
import {WorkflowActionService} from '../../service/workflow-graph/model/workflow-action.service';
import {JointGraphWrapper} from '../../service/workflow-graph/model/joint-graph-wrapper';
import {ValidationWorkflowService} from '../../service/validation/validation-workflow.service';
import {ExecutionState} from '../../types/execute-workflow.interface';
import {WorkflowStatusService} from '../../service/workflow-status/workflow-status.service';
import {UserService} from '../../../common/service/user/user.service';
import {WorkflowPersistService} from '../../../common/service/user/workflow-persist/workflow-persist.service';
import {SaveWorkflowService} from '../../service/save-workflow/save-workflow.service';

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
  public executionState: ExecutionState;  // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not

  // variable binded with HTML to decide if the running spinner should show
  public buttonText = 'Run';
  public showSpinner = false;
  public disable = false;
  public executionResultID: string | undefined;

  // tslint:disable-next-line:member-ordering
  constructor(
    public executeWorkflowService: ExecuteWorkflowService,
    public tourService: TourService,
    public workflowActionService: WorkflowActionService,
    public workflowStatusService: WorkflowStatusService,
    public undoRedo: UndoRedoService,
    public validationWorkflowService: ValidationWorkflowService,
    private saveWorkflowService: SaveWorkflowService,
    public workflowPersistService: WorkflowPersistService,
    private userService: UserService
  ) {
    this.executionState = executeWorkflowService.getExecutionState().state;
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    const initBehavior = this.getBehavior();
    this.buttonText = initBehavior.text;
    this.showSpinner = initBehavior.spinner;
    this.disable = initBehavior.disable;
    this.onClickHandler = initBehavior.onClick;

    executeWorkflowService.getExecutionStateStream().subscribe(
      event => {
        this.executionState = event.current.state;
        if (event.current.state === ExecutionState.Completed) {
          this.executionResultID = event.current.resultID;
        }
        const behavior = this.getBehavior();
        this.buttonText = behavior.text;
        this.showSpinner = behavior.spinner;
        this.disable = behavior.disable;
        this.onClickHandler = behavior.onClick;
      }
    );

    // set the map of operatorStatusMap
    validationWorkflowService.getWorkflowValidationErrorStream()
      .subscribe(value => this.isWorkflowValid = Object.keys(value.errors).length === 0);
  }

  public onClickHandler = () => {
  };

  ngOnInit() {
  }

  public getBehavior(): {
    text: string,
    spinner: boolean,
    disable: boolean,
    onClick: () => void
  } {
    if (!this.isWorkflowValid) {
      return {
        text: 'Run', spinner: false, disable: true, onClick: () => {
        }
      };
    }
    switch (this.executionState) {
      case ExecutionState.Uninitialized:
      case ExecutionState.Completed:
      case ExecutionState.Failed:
        return {
          text: 'Run', spinner: false, disable: false,
          onClick: () => this.executeWorkflowService.executeWorkflow()
        };
      case ExecutionState.WaitingToRun:
        return {
          text: 'Submitting', spinner: true, disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Running:
        return {
          text: 'Pause', spinner: true, disable: false,
          onClick: () => this.executeWorkflowService.pauseWorkflow()
        };
      case ExecutionState.Paused:
      case ExecutionState.BreakpointTriggered:
        return {
          text: 'Resume', spinner: false, disable: false,
          onClick: () => this.executeWorkflowService.resumeWorkflow()
        };
      case ExecutionState.Pausing:
        return {
          text: 'Pausing', spinner: true, disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Resuming:
        return {
          text: 'Resuming', spinner: true, disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Recovering:
        return {
          text: 'Recovering', spinner: true, disable: true,
          onClick: () => {
          }
        };
    }
  }

  public handleKill(): void {
    this.executeWorkflowService.killWorkflow();
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
    if (this.isZoomRatioMin()) {
      return;
    }

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
    if (this.isZoomRatioMax()) {
      return;
    }

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
  public onClickRestoreZoomOffsetDefault(): void {
    this.workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset();
  }

  /**
   * Delete all operators on the graph
   */
  public onClickDeleteAllOperators(): void {
    const allOperatorIDs = this.workflowActionService.getTexeraGraph().getAllOperators().map(op => op.operatorID);
    this.workflowActionService.deleteOperatorsAndLinks(allOperatorIDs, []);
  }

  /**
   * Returns true if there's any operator on the graph; false otherwise
   */
  public hasOperators(): boolean {
    return this.workflowActionService.getTexeraGraph().getAllOperators().length > 0;
  }

  public onClickSaveWorkflow(): void {
    if (!this.userService.isLogin()) {
      alert('please login');
    } else {
      this.workflowPersistService.saveWorkflow(this.userService.getUser()?.userID, this.saveWorkflowService.getSavedWorkflow());
    }
  }


}

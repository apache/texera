import { DatePipe, Location } from '@angular/common';
import { Component, Input, OnInit } from '@angular/core';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { environment } from '../../../../environments/environment';
import { Version } from '../../../../environments/version';
import { UserService } from '../../../common/service/user/user.service';
import { WorkflowPersistService } from '../../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../../common/type/workflow';
import { WorkflowCacheService } from '../../service/cache-workflow/workflow-cache.service';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { UndoRedoService } from '../../service/undo-redo/undo-redo.service';
import { ValidationWorkflowService } from '../../service/validation/validation-workflow.service';
import { JointGraphWrapper } from '../../service/workflow-graph/model/joint-graph-wrapper';
import { WorkflowActionService } from '../../service/workflow-graph/model/workflow-action.service';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ExecutionState } from '../../types/execute-workflow.interface';

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
  @Input() public autoSaveState: string = '';
  @Input() public currentWorkflowName: string = '';  // reset workflowName

  public gitCommitHash: string = Version.raw;
  public executionState: ExecutionState;  // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isSaving: boolean = false;

  // variable bound with HTML to decide if the running spinner should show
  public runButtonText = 'Run';
  public runIcon = 'play-circle';
  public runDisable = false;
  public executionResultID: string|undefined;

  // whether user dashboard is enabled and accessible from the workspace
  public userSystemEnabled: boolean = environment.userSystemEnabled;
  public onClickRunHandler: () => void;

  constructor(
    public executeWorkflowService: ExecuteWorkflowService,
    public tourService: TourService,
    public workflowActionService: WorkflowActionService,
    public workflowStatusService: WorkflowStatusService,
    public undoRedo: UndoRedoService,
    public validationWorkflowService: ValidationWorkflowService,
    public workflowPersistService: WorkflowPersistService,
    public userService: UserService,
    private datePipe: DatePipe,
    private location: Location,
    private undoRedoService: UndoRedoService,
    private workflowCacheService: WorkflowCacheService
  ) {
    this.executionState = executeWorkflowService.getExecutionState().state;
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    const initBehavior = this.getRunButtonBehavior(this.executionState, this.isWorkflowValid);
    this.runButtonText = initBehavior.text;
    this.runIcon = initBehavior.icon;
    this.runDisable = initBehavior.disable;
    this.onClickRunHandler = initBehavior.onClick;

  }

  ngOnInit(): void {
    this.executeWorkflowService.getExecutionStateStream().subscribe(
      event => {
        this.executionState = event.current.state;
        switch (event.current.state) {
          case ExecutionState.Completed:
            this.executionResultID = event.current.resultID;
            break;
        }
        this.applyRunButtonBehavior(this.getRunButtonBehavior(this.executionState, this.isWorkflowValid));
      }
    );

    // set the map of operatorStatusMap
    this.validationWorkflowService.getWorkflowValidationErrorStream()
        .subscribe(value => {
          this.isWorkflowValid = Object.keys(value.errors).length === 0;
          this.applyRunButtonBehavior(this.getRunButtonBehavior(this.executionState, this.isWorkflowValid));
        });

    // handle cached workflow change
    this.workflowActionService.workflowChanged().debounceTime(5).subscribe(() => {
      const workflow: Workflow|undefined = this.workflowActionService.getWorkflow();
      if (workflow !== undefined) {
        this.currentWorkflowName = workflow.name;

        if (environment.userSystemEnabled && this.userService.isLogin()) {
          this.autoSaveState = workflow.lastModifiedTime == null ?
            'Not Saved' :
            'Saved at  ' + this.datePipe.transform(workflow.lastModifiedTime, 'MM/dd/yyyy HH:mm:ss zzz', 'UTC');

        }
      }
    });
  }

  // apply a behavior to the run button via bound variables
  public applyRunButtonBehavior(
    behavior: {
      text: string,
      icon: string,
      disable: boolean,
      onClick: () => void
    }
  ) {
    this.runButtonText = behavior.text;
    this.runIcon = behavior.icon;
    this.runDisable = behavior.disable;
    this.onClickRunHandler = behavior.onClick;
  }

  public getRunButtonBehavior(executionState: ExecutionState, isWorkflowValid: boolean): {
    text: string,
    icon: string,
    disable: boolean,
    onClick: () => void
  } {
    if (!isWorkflowValid) {
      return {
        text: 'Error', icon: 'exclamation-circle', disable: true, onClick: () => {
        }
      };
    }
    switch (executionState) {
      case ExecutionState.Uninitialized:
      case ExecutionState.Completed:
      case ExecutionState.Failed:
        return {
          text: 'Run', icon: 'play-circle', disable: false,
          onClick: () => this.executeWorkflowService.executeWorkflow()
        };
      case ExecutionState.WaitingToRun:
        return {
          text: 'Submitting', icon: 'loading', disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Running:
        return {
          text: 'Pause', icon: 'loading', disable: false,
          onClick: () => this.executeWorkflowService.pauseWorkflow()
        };
      case ExecutionState.Paused:
      case ExecutionState.BreakpointTriggered:
        return {
          text: 'Resume', icon: 'pause-circle', disable: false,
          onClick: () => this.executeWorkflowService.resumeWorkflow()
        };
      case ExecutionState.Pausing:
        return {
          text: 'Pausing', icon: 'loading', disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Resuming:
        return {
          text: 'Resuming', icon: 'loading', disable: true,
          onClick: () => {
          }
        };
      case ExecutionState.Recovering:
        return {
          text: 'Recovering', icon: 'loading', disable: true,
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
   * Persist the cachedWorkflow in the localStorage, save it to the backend database. It also fetches the new workflow.wid
   * and updates it in the cache if this is a new workflow.
   */
  public persistCachedWorkflow(): void {
    this.isSaving = true;
    this.workflowPersistService.persistWorkflow(this.workflowActionService.getWorkflow())
        .subscribe((updatedWorkflow: Workflow) => {
          this.workflowCacheService.setCacheWorkflow(updatedWorkflow);
          this.isSaving = false;
        }, error => {
          alert(error);
          this.isSaving = false;
        });
  }

  /**
   * Handler for changing workflow name input box, updates the cachedWorkflow and persist to database.
   */
  onWorkflowNameChange() {
    this.currentWorkflowName = this.currentWorkflowName.trim();
    this.workflowActionService.setWorkflowName(this.currentWorkflowName);
    if (this.userService.isLogin()) {
      this.persistCachedWorkflow();
    }

  }

  /**
   * Handler for create new workflow button. It resets the cachedWorkflow and undo redo stack. It removes the workflow id from the URL
   * since at this moment, the new workflow is not persisted to database.
   */
  onClickCreateNewWorkflow() {
    this.workflowCacheService.resetCachedWorkflow();
    this.workflowActionService.reloadWorkflow(this.workflowCacheService.getCachedWorkflow());
    this.undoRedoService.clearUndoStack();
    this.undoRedoService.clearRedoStack();
    this.location.go('/');

  }
}

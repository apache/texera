import { DatePipe, Location } from '@angular/common';
import { Component, ElementRef, Input, OnInit, ViewChild } from '@angular/core';
import { TourService } from 'ngx-tour-ng-bootstrap';
import { environment } from '../../../../environments/environment';
import { UserService } from '../../../common/service/user/user.service';
import { WorkflowPersistService } from '../../../common/service/user/workflow-persist/workflow-persist.service';
import { Workflow } from '../../../common/type/workflow';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { UndoRedoService } from '../../service/undo-redo/undo-redo.service';
import { ValidationWorkflowService } from '../../service/validation/validation-workflow.service';
import { WorkflowCacheService } from '../../service/workflow-cache/workflow-cache.service';
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

  public executionState: ExecutionState;  // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isSaving: boolean = false;

  @Input() public autoSaveState: string = '';
  @Input() public currentWorkflowName: string = '';  // reset workflowName
  @ViewChild('nameInput') nameInputBox: ElementRef<HTMLElement>|undefined;

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
    private location: Location,
    public undoRedoService: UndoRedoService,
    public validationWorkflowService: ValidationWorkflowService,
    public workflowPersistService: WorkflowPersistService,
    public userService: UserService,
    private workflowCacheService: WorkflowCacheService,
    private datePipe: DatePipe
  ) {
    this.executionState = executeWorkflowService.getExecutionState().state;
    // return the run button after the execution is finished, either
    //  when the value is valid or invalid
    const initBehavior = this.getRunButtonBehavior(this.executionState, this.isWorkflowValid);
    this.runButtonText = initBehavior.text;
    this.runIcon = initBehavior.icon;
    this.runDisable = initBehavior.disable;
    this.onClickRunHandler = initBehavior.onClick;
    // this.currentWorkflowName = this.workflowCacheService.getCachedWorkflow();

    executeWorkflowService.getExecutionStateStream().subscribe(
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
    validationWorkflowService.getWorkflowValidationErrorStream()
                             .subscribe(value => {
                               this.isWorkflowValid = Object.keys(value.errors).length === 0;
                               this.applyRunButtonBehavior(this.getRunButtonBehavior(this.executionState, this.isWorkflowValid));
                             });

    this.registerWorkflowMetadataDisplayRefresh();
  }

  ngOnInit() {
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
   * Delete all operators (including hidden ones) on the graph.
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

  /**
   * Groups highlighted operators on the graph.
   */
  public onClickGroupOperators(): void {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    if (this.highlightedElementsGroupable()) {
      const group = this.workflowActionService.getOperatorGroup().getNewGroup(highlightedOperators);
      this.workflowActionService.addGroups(group);
    }
  }

  /**
   * Returns true if currently highlighted elements are all operators
   * and if they are groupable.
   */
  public highlightedElementsGroupable(): boolean {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length === 0 &&
      this.workflowActionService.getOperatorGroup().operatorsGroupable(highlightedOperators);
  }

  /**
   * Ungroups highlighted groups on the graph.
   */
  public onClickUngroupOperators(): void {
    const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
    if (this.highlightedElementsUngroupable()) {
      this.workflowActionService.unGroupGroups(...highlightedGroups);
    }
  }

  /**
   * Returns true if currently highlighted elements are all groups.
   */
  public highlightedElementsUngroupable(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length > 0 &&
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length === 0;
  }

  public persistWorkflow(): void {
    this.isSaving = true;
    this.workflowPersistService.persistWorkflow(this.workflowActionService.getWorkflow())
        .subscribe((updatedWorkflow: Workflow) => {
          this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
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
    this.workflowActionService.setWorkflowName(this.currentWorkflowName);
    if (this.userService.isLogin()) {
      this.persistWorkflow();
    }
  }

  onClickCreateNewWorkflow() {
    this.workflowActionService.resetAsNewWorkflow();
    this.location.go('/');
  }

  registerWorkflowMetadataDisplayRefresh() {
    this.workflowActionService.workflowMetaDataChanged().debounceTime(100)
        .subscribe(() => {
          this.currentWorkflowName = this.workflowActionService.getWorkflowMetadata()?.name;
          this.autoSaveState = this.workflowActionService.getWorkflowMetadata().lastModifiedTime === undefined ?
            '' : 'Saved at ' + this.datePipe.transform(this.workflowActionService.getWorkflowMetadata().lastModifiedTime,
            'MM/dd/yyyy HH:mm:ss zzz', Intl.DateTimeFormat().resolvedOptions().timeZone, 'en');

        });
  }
}

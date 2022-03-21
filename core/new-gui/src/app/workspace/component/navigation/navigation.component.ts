import { DatePipe, Location } from "@angular/common";
import { ChangeDetectorRef, Component, ElementRef, Input, OnInit, ViewChild } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { UserService } from "../../../common/service/user/user.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "../../../common/type/workflow";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { JointGraphWrapper } from "../../service/workflow-graph/model/joint-graph-wrapper";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { merge } from "rxjs";
import { WorkflowResultExportService } from "../../service/workflow-result-export/workflow-result-export.service";
import { debounceTime } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { isSink } from "../../service/workflow-graph/model/workflow-graph";
import { WorkflowVersionService } from "../../../dashboard/service/workflow-version/workflow-version.service";
import { WorkflowCollabService } from "../../service/workflow-collab/workflow-collab.service";

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
@UntilDestroy()
@Component({
  selector: "texera-navigation",
  templateUrl: "./navigation.component.html",
  styleUrls: ["./navigation.component.scss"],
})
export class NavigationComponent implements OnInit {
  public executionState: ExecutionState; // set this to true when the workflow is started
  public ExecutionState = ExecutionState; // make Angular HTML access enum definition
  public isWorkflowValid: boolean = true; // this will check whether the workflow error or not
  public isSaving: boolean = false;

  @Input() public autoSaveState: string = "";
  @Input() public currentWorkflowName: string = ""; // reset workflowName
  @Input() public particularVersionDate: string = ""; // placeholder for the metadata information of a particular workflow version
  @ViewChild("nameInput") nameInputBox: ElementRef<HTMLElement> | undefined;

  // variable bound with HTML to decide if the running spinner should show
  public runButtonText = "Run";
  public runIcon = "play-circle";
  public runDisable = false;

  // whether user dashboard is enabled and accessible from the workspace
  public userSystemEnabled: boolean = environment.userSystemEnabled;
  public workflowCollabEnabled: boolean = environment.workflowCollabEnabled;
  public lockGranted: boolean = true;
  public workflowReadonly: boolean = false;
  // flag to display a particular version in the current canvas
  public displayParticularWorkflowVersion: boolean = false;
  public onClickRunHandler: () => void;

  // whether the disable-operator-button should be enabled
  public isDisableOperatorClickable: boolean = false;
  public isDisableOperator: boolean = true;

  public operatorCacheEnabled: boolean = environment.operatorCacheEnabled;
  public isCacheOperatorClickable: boolean = false;
  public isCacheOperator: boolean = true;

  public static readonly COLLAB_RELOAD_WAIT_TIME = 500;

  constructor(
    public executeWorkflowService: ExecuteWorkflowService,
    public workflowActionService: WorkflowActionService,
    public workflowWebsocketService: WorkflowWebsocketService,
    private location: Location,
    public undoRedoService: UndoRedoService,
    public validationWorkflowService: ValidationWorkflowService,
    public workflowPersistService: WorkflowPersistService,
    public workflowVersionService: WorkflowVersionService,
    public userService: UserService,
    private datePipe: DatePipe,
    public workflowResultExportService: WorkflowResultExportService,
    public workflowCollabService: WorkflowCollabService,
    public workflowUtilService: WorkflowUtilService,
    public changeDetectionRef: ChangeDetectorRef
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
  }

  public ngOnInit(): void {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.executionState = event.current.state;
        this.applyRunButtonBehavior(this.getRunButtonBehavior(this.executionState, this.isWorkflowValid));
      });

    // set the map of operatorStatusMap
    this.validationWorkflowService
      .getWorkflowValidationErrorStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        this.isWorkflowValid = Object.keys(value.errors).length === 0;
        this.applyRunButtonBehavior(this.getRunButtonBehavior(this.executionState, this.isWorkflowValid));
      });

    this.registerWorkflowMetadataDisplayRefresh();
    this.handleWorkflowVersionDisplay();
    this.handleDisableOperatorStatusChange();
    this.handleCacheOperatorStatusChange();
    this.handleLockChange();
    this.handleWorkflowAccessChange();
  }

  // apply a behavior to the run button via bound variables
  public applyRunButtonBehavior(behavior: { text: string; icon: string; disable: boolean; onClick: () => void }) {
    this.runButtonText = behavior.text;
    this.runIcon = behavior.icon;
    this.runDisable = behavior.disable;
    this.onClickRunHandler = behavior.onClick;
  }

  public getRunButtonBehavior(
    executionState: ExecutionState,
    isWorkflowValid: boolean
  ): {
    text: string;
    icon: string;
    disable: boolean;
    onClick: () => void;
  } {
    if (!isWorkflowValid) {
      return {
        text: "Error",
        icon: "exclamation-circle",
        disable: true,
        onClick: () => {},
      };
    }
    switch (executionState) {
      case ExecutionState.Uninitialized:
      case ExecutionState.Completed:
      case ExecutionState.Aborted:
        return {
          text: "Run",
          icon: "play-circle",
          disable: false,
          onClick: () => this.executeWorkflowService.executeWorkflow(),
        };
      case ExecutionState.Initializing:
        return {
          text: "Submitting",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Running:
        return {
          text: "Pause",
          icon: "loading",
          disable: false,
          onClick: () => this.executeWorkflowService.pauseWorkflow(),
        };
      case ExecutionState.Paused:
      case ExecutionState.BreakpointTriggered:
        return {
          text: "Resume",
          icon: "pause-circle",
          disable: false,
          onClick: () => this.executeWorkflowService.resumeWorkflow(),
        };
      case ExecutionState.Pausing:
        return {
          text: "Pausing",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Resuming:
        return {
          text: "Resuming",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
      case ExecutionState.Recovering:
        return {
          text: "Recovering",
          icon: "loading",
          disable: true,
          onClick: () => {},
        };
    }
  }

  public onClickAddCommentBox(): void {
    this.workflowActionService.addCommentBox(this.workflowUtilService.getNewCommentBox());
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
    this.workflowActionService
      .getJointGraphWrapper()
      .setZoomProperty(
        this.workflowActionService.getJointGraphWrapper().getZoomRatio() - JointGraphWrapper.ZOOM_CLICK_DIFF
      );
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
    this.workflowActionService
      .getJointGraphWrapper()
      .setZoomProperty(
        this.workflowActionService.getJointGraphWrapper().getZoomRatio() + JointGraphWrapper.ZOOM_CLICK_DIFF
      );
  }

  /**
   * This method will run the autoLayout function
   *
   */
  public onClickAutoLayout(): void {
    if (!this.hasOperators()) {
      return;
    }
    this.workflowActionService.autoLayoutWorkflow();
  }

  /**
   * This is the handler for the execution result export button.
   *
   */
  public onClickExportExecutionResult(exportType: string): void {
    this.workflowResultExportService.exportWorkflowExecutionResult(exportType, this.currentWorkflowName);
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
    const allOperatorIDs = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .map(op => op.operatorID);
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
    return (
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length === 0 &&
      this.workflowActionService.getOperatorGroup().operatorsGroupable(highlightedOperators)
    );
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
   * callback function when user clicks the "disable operator" icon:
   * this.isDisableOperator indicates whether the operators should be disabled or enabled
   */
  public onClickDisableOperators(): void {
    if (this.isDisableOperator) {
      this.workflowActionService.disableOperators(this.effectivelyHighlightedOperators());
    } else {
      this.workflowActionService.enableOperators(this.effectivelyHighlightedOperators());
    }
  }

  public onClickCacheOperators(): void {
    const effectiveHighlightedOperators = this.effectivelyHighlightedOperators();
    const effectiveHighlightedOperatorsExcludeSink = effectiveHighlightedOperators.filter(
      op => !isSink(this.workflowActionService.getTexeraGraph().getOperator(op))
    );

    if (this.isCacheOperator) {
      this.workflowActionService.cacheOperators(effectiveHighlightedOperatorsExcludeSink);
    } else {
      this.workflowActionService.unCacheOperators(effectiveHighlightedOperatorsExcludeSink);
    }
  }

  /**
   * Returns true if currently highlighted elements are all groups.
   */
  public highlightedElementsUngroupable(): boolean {
    return (
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs().length > 0 &&
      this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length === 0
    );
  }

  public persistWorkflow(): void {
    if (this.workflowCollabService.isLockGranted()) {
      this.isSaving = true;
      this.workflowPersistService
        .persistWorkflow(this.workflowActionService.getWorkflow())
        .pipe(untilDestroyed(this))
        .subscribe(
          (updatedWorkflow: Workflow) => {
            this.workflowActionService.setWorkflowMetadata(updatedWorkflow);
            this.isSaving = false;
          },
          (error: unknown) => {
            alert(error);
            this.isSaving = false;
          }
        );
    }
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
    this.location.go("/");
  }

  onClickAcquireLock() {
    this.workflowCollabService.acquireLock();
  }

  registerWorkflowMetadataDisplayRefresh() {
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(debounceTime(100))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentWorkflowName = this.workflowActionService.getWorkflowMetadata()?.name;
        this.autoSaveState =
          this.workflowActionService.getWorkflowMetadata().lastModifiedTime === undefined
            ? ""
            : "Saved at " +
              this.datePipe.transform(
                this.workflowActionService.getWorkflowMetadata().lastModifiedTime,
                "MM/dd/yyyy HH:mm:ss zzz",
                Intl.DateTimeFormat().resolvedOptions().timeZone,
                "en"
              );
      });
  }

  onClickGetAllVersions() {
    this.workflowVersionService.clickDisplayWorkflowVersions();
  }

  private handleWorkflowVersionDisplay(): void {
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(untilDestroyed(this))
      .subscribe(displayVersionFlag => {
        this.particularVersionDate =
          this.workflowActionService.getWorkflowMetadata().creationTime === undefined
            ? ""
            : "" +
              this.datePipe.transform(
                this.workflowActionService.getWorkflowMetadata().creationTime,
                "MM/dd/yyyy HH:mm:ss zzz",
                Intl.DateTimeFormat().resolvedOptions().timeZone,
                "en"
              );
        this.displayParticularWorkflowVersion = displayVersionFlag;
      });
  }

  closeParticularVersionDisplay() {
    this.workflowVersionService.closeParticularVersionDisplay();
  }

  revertToVersion() {
    this.workflowVersionService.revertToVersion();
    // after swapping the workflows to point to the particular version, persist it in DB
    this.persistWorkflow();
    setTimeout(() => {
      this.workflowCollabService.requestOthersToReload();
    }, NavigationComponent.COLLAB_RELOAD_WAIT_TIME);
  }

  /**
   * Updates the status of the disable operator icon:
   * If all selected operators are disabled, then click it will re-enable the operators
   * If any of the selected operator is not disabled, then click will disable all selected operators
   */
  handleDisableOperatorStatusChange() {
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getTexeraGraph().getDisabledOperatorsChangedStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const effectiveHighlightedOperators = this.effectivelyHighlightedOperators();
        const allDisabled = this.effectivelyHighlightedOperators().every(op =>
          this.workflowActionService.getTexeraGraph().isOperatorDisabled(op)
        );

        this.isDisableOperator = !allDisabled;
        this.isDisableOperatorClickable = effectiveHighlightedOperators.length !== 0;
      });
  }

  handleCacheOperatorStatusChange() {
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream(),
      this.workflowActionService.getTexeraGraph().getCachedOperatorsChangedStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const effectiveHighlightedOperators = this.effectivelyHighlightedOperators();
        const effectiveHighlightedOperatorsExcludeSink = effectiveHighlightedOperators.filter(
          op => !isSink(this.workflowActionService.getTexeraGraph().getOperator(op))
        );

        const allCached = effectiveHighlightedOperatorsExcludeSink.every(op =>
          this.workflowActionService.getTexeraGraph().isOperatorCached(op)
        );

        this.isCacheOperator = !allCached;
        this.isCacheOperatorClickable = effectiveHighlightedOperatorsExcludeSink.length !== 0;
      });
  }

  private handleLockChange(): void {
    this.workflowCollabService
      .getLockStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe((lockGranted: boolean) => {
        this.lockGranted = lockGranted;
        this.changeDetectionRef.detectChanges();
      });
  }

  private handleWorkflowAccessChange(): void {
    this.workflowCollabService
      .getWorkflowAccessStream()
      .pipe(untilDestroyed(this))
      .subscribe((workflowReadonly: boolean) => {
        this.workflowReadonly = workflowReadonly;
      });
  }

  /**
   * Gets all highlighted operators, and all operators in the highlighted groups
   */
  effectivelyHighlightedOperators(): readonly string[] {
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const highlightedGroups = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();

    const operatorInHighlightedGroups: string[] = highlightedGroups.flatMap(g =>
      Array.from(this.workflowActionService.getOperatorGroup().getGroup(g).operators.keys())
    );

    const effectiveHighlightedOperators = new Set<string>();
    highlightedOperators.forEach(op => effectiveHighlightedOperators.add(op));
    operatorInHighlightedGroups.forEach(op => effectiveHighlightedOperators.add(op));
    return Array.from(effectiveHighlightedOperators);
  }
}

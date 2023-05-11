import { ChangeDetectorRef, Component, OnInit } from "@angular/core";
import { merge } from "rxjs";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState, ExecutionStateInfo } from "../../types/execute-workflow.interface";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { VisualizationFrameComponent } from "./visualization-frame/visualization-frame.component";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DynamicComponentConfig } from "../../../common/type/dynamic-component-config";
import { DebuggerFrameComponent } from "./debugger-frame/debugger-frame.component";
import { isPythonUdf, isSink } from "../../service/workflow-graph/model/workflow-graph";
import { environment } from "../../../../environments/environment";
import { WorkflowVersionService } from "../../../dashboard/user/service/workflow-version/workflow-version.service";

export type ResultFrameComponent =
  | ResultTableFrameComponent
  | VisualizationFrameComponent
  | ConsoleFrameComponent
  | DebuggerFrameComponent;

export type ResultFrameComponentConfig = DynamicComponentConfig<ResultFrameComponent>;

/**
 * ResultPanelComponent is the bottom level area that displays the
 *  execution result of a workflow after the execution finishes.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-panel",
  templateUrl: "./result-panel.component.html",
  styleUrls: ["./result-panel.component.scss"],
})
export class ResultPanelComponent implements OnInit {
  frameComponentConfigs: Map<string, ResultFrameComponentConfig> = new Map();

  // the highlighted operator ID for display result table / visualization / breakpoint
  currentOperatorId?: string | undefined;

  showResultPanel: boolean = false;
  previewWorkflowVersion: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private workflowVersionService: WorkflowVersionService,
    private changeDetectorRef: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.registerAutoRerenderResultPanel();
    this.registerAutoOpenResultPanel();
    this.handleResultPanelForVersionPreview();
  }

  handleResultPanelForVersionPreview() {
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(untilDestroyed(this))
      .subscribe(displayVersionFlag => {
        this.previewWorkflowVersion = displayVersionFlag;
      });
  }

  registerAutoOpenResultPanel() {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const currentlyHighlighted = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        // display panel on breakpoint hits and highlight breakpoint operator
        if (event.current.state === ExecutionState.BreakpointTriggered) {
          const breakpointOperator = this.executeWorkflowService.getBreakpointTriggerInfo()?.operatorID;
          if (breakpointOperator) {
            this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
            this.workflowActionService.getJointGraphWrapper().highlightOperators(breakpointOperator);
          }
          this.resultPanelToggleService.openResultPanel();
        }
        // display panel on abort (to show possible error messages)
        if (event.current.state === ExecutionState.Aborted) {
          this.resultPanelToggleService.openResultPanel();
        }
        // display panel when execution is completed and highlight sink to show results
        // condition must be (Running -> Completed) to prevent cases like
        //   (Uninitialized -> Completed) (a completed workflow is reloaded)
        if (event.previous.state === ExecutionState.Running && event.current.state === ExecutionState.Completed) {
          const activeSinkOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter(op => isSink(op))
            .filter(op => !op.isDisabled)
            .map(op => op.operatorID);

          if (activeSinkOperators.length > 0) {
            if (!(currentlyHighlighted.length == 1 && activeSinkOperators.includes(currentlyHighlighted[0]))) {
              this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
              this.workflowActionService.getJointGraphWrapper().highlightOperators(activeSinkOperators[0]);
            }
            this.resultPanelToggleService.openResultPanel();
          }
        }

        // display panel and highlight a python UDF operator when workflow starts running
        if (event.current.state === ExecutionState.Running) {
          const activePythonUDFOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter(op => isPythonUdf(op))
            .filter(op => !op.isDisabled)
            .map(op => op.operatorID);

          if (activePythonUDFOperators.length > 0) {
            if (!(currentlyHighlighted.length == 1 && activePythonUDFOperators.includes(activePythonUDFOperators[0]))) {
              this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
              this.workflowActionService.getJointGraphWrapper().highlightOperators(activePythonUDFOperators[0]);
            }
            this.resultPanelToggleService.openResultPanel();
          }
        }
      });
  }

  registerAutoRerenderResultPanel() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(filter(event => ResultPanelComponent.needRerenderOnStateChange(event))),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.resultPanelToggleService.getToggleChangeStream(),
      this.workflowResultService.getResultInitiateStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.rerenderResultPanel();
        this.changeDetectorRef.detectChanges();
      });
  }

  rerenderResultPanel(): void {
    // if the workflow on the paper is a version preview then this is a temporary workaround until a future PR
    // TODO: let the results be tied with an execution ID instead of a workflow ID
    if (this.previewWorkflowVersion) {
      return;
    }
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const currentHighlightedOperator = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;
    if (this.currentOperatorId !== currentHighlightedOperator) {
      // clear everything, prepare for state change
      this.clearResultPanel();
      this.currentOperatorId = currentHighlightedOperator;
    }
    // current result panel is closed or there is no operator highlighted, do nothing
    this.showResultPanel = this.resultPanelToggleService.isResultPanelOpen();
    if (!this.showResultPanel || !this.currentOperatorId) {
      return;
    }

    if (this.currentOperatorId) {
      this.displayResult(this.currentOperatorId);
      const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      if (isPythonUdf(operator)) {
        this.displayConsole(this.currentOperatorId);

        if (environment.debuggerEnabled && this.hasErrorOrBreakpoint()) {
          this.displayDebugger(this.currentOperatorId);
        }
      }
    }
  }

  hasErrorOrBreakpoint(): boolean {
    const executionState = this.executeWorkflowService.getExecutionState();
    return [ExecutionState.Aborted, ExecutionState.BreakpointTriggered].includes(executionState.state);
  }

  clearResultPanel(): void {
    this.frameComponentConfigs.clear();
  }

  displayConsole(operatorId: string) {
    this.frameComponentConfigs.set("Console", {
      component: ConsoleFrameComponent,
      componentInputs: { operatorId },
    });
  }

  displayDebugger(operatorId: string) {
    this.frameComponentConfigs.set("Debugger", {
      component: DebuggerFrameComponent,
      componentInputs: { operatorId },
    });
  }

  displayResult(operatorId: string) {
    const resultService = this.workflowResultService.getResultService(operatorId);
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      // display table result if has paginated results
      this.frameComponentConfigs.set("Result", {
        component: ResultTableFrameComponent,
        componentInputs: { operatorId },
      });
    } else if (resultService && resultService.getChartType()) {
      // display visualization result
      this.frameComponentConfigs.set("Result", {
        component: VisualizationFrameComponent,
        componentInputs: { operatorId },
      });
    }
  }

  private static needRerenderOnStateChange(event: {
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }): boolean {
    // transitioning from any state to failed state
    if (event.current.state === ExecutionState.Aborted) {
      return true;
    }
    // transitioning from any state to breakpoint triggered state
    if (event.current.state === ExecutionState.BreakpointTriggered) {
      return true;
    }
    // transition from uninitialized / completed to anything else indicates a new execution of the workflow
    return event.previous.state === ExecutionState.Uninitialized || event.previous.state === ExecutionState.Completed;
  }
}

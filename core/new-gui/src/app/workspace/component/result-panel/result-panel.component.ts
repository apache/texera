import { Component, OnInit } from "@angular/core";
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
import { PYTHON_UDF_SOURCE_V2_OP_TYPE, PYTHON_UDF_V2_OP_TYPE } from "../../service/workflow-graph/model/workflow-graph";
import { environment } from "../../../../environments/environment";

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
  currentOperatorId?: string;

  showResultPanel: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService
  ) {}

  ngOnInit(): void {
    this.registerAutoRerenderResultPanel();
    this.registerAutoOpenResultPanel();
  }

  registerAutoOpenResultPanel() {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const currentlyHighlighted = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        if (event.current.state === ExecutionState.BreakpointTriggered) {
          const breakpointOperator = this.executeWorkflowService.getBreakpointTriggerInfo()?.operatorID;
          if (breakpointOperator) {
            this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
            this.workflowActionService.getJointGraphWrapper().highlightOperators(breakpointOperator);
          }
          this.resultPanelToggleService.openResultPanel();
        }
        if (event.current.state === ExecutionState.Aborted) {
          this.resultPanelToggleService.openResultPanel();
        }
        if (event.current.state === ExecutionState.Completed || event.current.state === ExecutionState.Running) {
          const sinkOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter(op => op.operatorType.toLowerCase().includes("sink"));
          if (sinkOperators.length > 0 && !this.currentOperatorId) {
            this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
            this.workflowActionService.getJointGraphWrapper().highlightOperators(sinkOperators[0].operatorID);
          }
          this.resultPanelToggleService.openResultPanel();
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
      });
  }

  rerenderResultPanel(): void {
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
      if ([PYTHON_UDF_V2_OP_TYPE, PYTHON_UDF_SOURCE_V2_OP_TYPE].includes(operator.operatorType)) {
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
    if (event.previous.state === ExecutionState.Uninitialized || event.previous.state === ExecutionState.Completed) {
      return true;
    }
    return false;
  }
}

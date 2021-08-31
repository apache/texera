import { Component, OnInit } from "@angular/core";
import { merge } from "rxjs";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import {
  ExecutionState,
  ExecutionStateInfo
} from "../../types/execute-workflow.interface";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { VisualizationFrameComponent } from "./visualization-frame/visualization-frame.component";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DynamicComponentConfig } from "../../../common/type/dynamic-component-config";

export type ResultFrameComponent =
  | ResultTableFrameComponent
  | VisualizationFrameComponent
  | ConsoleFrameComponent;

export type ResultFrameComponentConfig =
  DynamicComponentConfig<ResultFrameComponent>;

/**
 * ResultPanelComponent is the bottom level area that displays the
 *  execution result of a workflow after the execution finishes.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-panel",
  templateUrl: "./result-panel.component.html",
  styleUrls: ["./result-panel.component.scss"]
})
export class ResultPanelComponent implements OnInit {
  frameComponentConfig?: ResultFrameComponentConfig;

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
      .subscribe((event) => {
        const currentlyHighlighted = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();
        if (event.current.state === ExecutionState.BreakpointTriggered) {
          const breakpointOperator =
            this.executeWorkflowService.getBreakpointTriggerInfo()?.operatorID;
          if (breakpointOperator) {
            this.workflowActionService
              .getJointGraphWrapper()
              .unhighlightOperators(...currentlyHighlighted);
            this.workflowActionService
              .getJointGraphWrapper()
              .highlightOperators(breakpointOperator);
          }
          this.resultPanelToggleService.openResultPanel();
        }
        if (event.current.state === ExecutionState.Failed) {
          this.resultPanelToggleService.openResultPanel();
        }
        if (
          event.current.state === ExecutionState.Completed ||
          event.current.state === ExecutionState.Running
        ) {
          const sinkOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter((op) => op.operatorType.toLowerCase().includes("sink"));
          if (sinkOperators.length > 0 && !this.currentOperatorId) {
            this.workflowActionService
              .getJointGraphWrapper()
              .unhighlightOperators(...currentlyHighlighted);
            this.workflowActionService
              .getJointGraphWrapper()
              .highlightOperators(sinkOperators[0].operatorID);
          }
          this.resultPanelToggleService.openResultPanel();
        }
      });
  }

  registerAutoRerenderResultPanel() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(
          filter((event) =>
            ResultPanelComponent.needRerenderOnStateChange(event)
          )
        ),
      this.workflowActionService
        .getJointGraphWrapper()
        .getJointOperatorHighlightStream(),
      this.workflowActionService
        .getJointGraphWrapper()
        .getJointOperatorUnhighlightStream(),
      this.resultPanelToggleService.getToggleChangeStream(),
      this.workflowResultService.getResultInitiateStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe((_) => {
        this.rerenderResultPanel();
      });
  }

  rerenderResultPanel(): void {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs();
    const currentHighlightedOperator =
      highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;
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

    // break this into another detect cycle, so that the dynamic component can be reloaded

    const executionState = this.executeWorkflowService.getExecutionState();
    if (
      executionState.state in
      [ExecutionState.Failed, ExecutionState.BreakpointTriggered]
    ) {
      this.switchFrameComponent({
        component: ConsoleFrameComponent,
        componentInputs: { operatorId: this.currentOperatorId }
      });
    } else {
      if (this.currentOperatorId) {
        if (
          this.workflowActionService
            .getTexeraGraph()
            .getOperator(this.currentOperatorId)
            .operatorType.toLowerCase()
            .includes("sink")
        ) {
          const resultService = this.workflowResultService.getResultService(
            this.currentOperatorId
          );
          const paginatedResultService =
            this.workflowResultService.getPaginatedResultService(
              this.currentOperatorId
            );
          if (paginatedResultService) {
            this.switchFrameComponent({
              component: ResultTableFrameComponent,
              componentInputs: { operatorId: this.currentOperatorId }
            });
          } else if (resultService && resultService.getChartType()) {
            this.switchFrameComponent({
              component: VisualizationFrameComponent,
              componentInputs: { operatorId: this.currentOperatorId }
            });
          }
        } else {
          this.switchFrameComponent({
            component: ConsoleFrameComponent,
            componentInputs: { operatorId: this.currentOperatorId }
          });
        }
      }
    }
  }

  clearResultPanel(): void {
    this.switchFrameComponent(undefined);
  }

  switchFrameComponent(targetComponentConfig?: ResultFrameComponentConfig) {
    if (
      this.frameComponentConfig?.component ===
        targetComponentConfig?.component &&
      this.frameComponentConfig?.componentInputs ===
        targetComponentConfig?.componentInputs
    ) {
      return;
    }
    this.frameComponentConfig = targetComponentConfig;
  }

  private static needRerenderOnStateChange(event: {
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }): boolean {
    // transitioning from any state to failed state
    if (event.current.state === ExecutionState.Failed) {
      return true;
    }
    // transitioning from any state to breakpoint triggered state
    if (event.current.state === ExecutionState.BreakpointTriggered) {
      return true;
    }

    // transition from uninitialized / completed to anything else indicates a new execution of the workflow
    if (
      event.previous.state === ExecutionState.Uninitialized ||
      event.previous.state === ExecutionState.Completed
    ) {
      return true;
    }
    return false;
  }
}

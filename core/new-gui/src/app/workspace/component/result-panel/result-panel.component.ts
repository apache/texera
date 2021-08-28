import { Component } from "@angular/core";
import { merge, timer } from "rxjs";
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

/**
 * ResultPanelComponent is the bottom level area that displays the
 *  execution result of a workflow after the execution finishes.
 *
 * The Component will display the result in an excel table format,
 *  where each row represents a result from the workflow,
 *  and each column represents the type of result the workflow returns.
 *
 * Clicking each row of the result table will create an pop-up window
 *  and display the detail of that row in a pretty json format.
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 */
@Component({
	selector: "texera-result-panel",
	templateUrl: "./result-panel.component.html",
	styleUrls: ["./result-panel.component.scss"]
})
export class ResultPanelComponent {
	frameComponent: any | undefined = undefined;

	// the highlighted operator ID for display result table / visualization / breakpoint
	resultPanelOperatorID: string | undefined;

	showResultPanel: boolean = false;

	constructor(
		private executeWorkflowService: ExecuteWorkflowService,
		private resultPanelToggleService: ResultPanelToggleService,
		private workflowActionService: WorkflowActionService,
		private workflowResultService: WorkflowResultService
	) {
		this.registerAutoRerenderResultPanel();
		this.registerAutoOpenResultPanel();
	}

	registerAutoOpenResultPanel() {
		this.executeWorkflowService.getExecutionStateStream().subscribe((event) => {
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
				if (sinkOperators.length > 0 && !this.resultPanelOperatorID) {
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
		).subscribe((_) => {
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
		if (this.resultPanelOperatorID !== currentHighlightedOperator) {
			// clear everything, prepare for state change

			this.clearResultPanel();
			this.resultPanelOperatorID = currentHighlightedOperator;
		}
		// current result panel is closed or there is no operator highlighted, do nothing
		this.showResultPanel = this.resultPanelToggleService.isResultPanelOpen();
		if (!this.showResultPanel || !this.resultPanelOperatorID) {
			return;
		}

		// break this into another detect cycle, so that the dynamic component can be reloaded
		timer(0).subscribe(() => {
			const executionState = this.executeWorkflowService.getExecutionState();
			if (executionState.state === ExecutionState.Failed) {
				this.switchFrameComponent(ConsoleFrameComponent);
			} else if (executionState.state === ExecutionState.BreakpointTriggered) {
				this.switchFrameComponent(ConsoleFrameComponent);
			} else {
				if (this.resultPanelOperatorID) {
					if (
						this.workflowActionService
							.getTexeraGraph()
							.getOperator(this.resultPanelOperatorID)
							.operatorType.toLowerCase()
							.includes("sink")
					) {
						const resultService = this.workflowResultService.getResultService(
							this.resultPanelOperatorID
						);
						const paginatedResultService =
							this.workflowResultService.getPaginatedResultService(
								this.resultPanelOperatorID
							);
						if (paginatedResultService) {
							this.switchFrameComponent(ResultTableFrameComponent);
						} else if (resultService && resultService.getChartType()) {
							this.switchFrameComponent(VisualizationFrameComponent);
						}
					} else {
						this.switchFrameComponent(ConsoleFrameComponent);
					}
				}
			}
		});
	}

	clearResultPanel(): void {
		this.switchFrameComponent(undefined);
	}

	switchFrameComponent(targetComponent: any) {
		if (this.frameComponent === targetComponent) {
			return;
		}
		this.frameComponent = targetComponent;
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

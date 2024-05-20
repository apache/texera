import { ChangeDetectorRef, Component, OnInit, Type, HostListener, OnDestroy } from "@angular/core";
import { merge } from "rxjs";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState, ExecutionStateInfo } from "../../types/execute-workflow.interface";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isPythonUdf, isSink } from "../../service/workflow-graph/model/workflow-graph";
import { WorkflowVersionService } from "../../../dashboard/user/service/workflow-version/workflow-version.service";
import { ErrorFrameComponent } from "./error-frame/error-frame.component";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { VisualizationFrameContentComponent } from "../visualization-panel-content/visualization-frame-content.component";

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
export class ResultPanelComponent implements OnInit, OnDestroy {
  frameComponentConfigs: Map<string, { component: Type<any>; componentInputs: {} }> = new Map();
  protected readonly window = window;
  id = -1;
  width = 800;
  height = 300;
  prevWidth = 800;
  prevHeight = 300;
  maxWidth = window.innerWidth;
  maxHeight = window.innerHeight;

  // the highlighted operator ID for display result table / visualization / breakpoint
  currentOperatorId?: string | undefined;

  previewWorkflowVersion: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowResultService: WorkflowResultService,
    private workflowVersionService: WorkflowVersionService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowConsoleService: WorkflowConsoleService,
    private resizeService: PanelResizeService
  ) {
    const width = localStorage.getItem("result-panel-width");
    if (width) this.width = Number(width);
    this.height = Number(localStorage.getItem("result-panel-height")) || this.height;
    this.resizeService.changePanelSize(this.width, this.height);
  }

  ngOnInit(): void {
    const style = localStorage.getItem("result-panel-style");
    if (style) document.getElementById("result-container")!.style.cssText = style;

    this.registerAutoRerenderResultPanel();
    this.registerAutoOpenResultPanel();
    this.handleResultPanelForVersionPreview();
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("result-panel-width", String(this.width));
    localStorage.setItem("result-panel-height", String(this.height));
    localStorage.setItem("result-panel-style", document.getElementById("result-container")!.style.cssText);
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

    if (this.executeWorkflowService.getExecutionState().state === ExecutionState.Failed) {
      if (this.currentOperatorId == null) {
        this.displayError(this.currentOperatorId);
      } else {
        const errorMessages = this.executeWorkflowService.getErrorMessages();
        if (errorMessages.filter(msg => msg.operatorId === this.currentOperatorId).length > 0) {
          this.displayError(this.currentOperatorId);
        } else {
          this.frameComponentConfigs.delete("Static Error");
        }
      }
    } else {
      this.frameComponentConfigs.delete("Static Error");
    }

    if (this.currentOperatorId) {
      this.displayResult(this.currentOperatorId);
      const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      if (this.workflowConsoleService.hasConsoleMessages(this.currentOperatorId) || isPythonUdf(operator)) {
        this.displayConsole(this.currentOperatorId, isPythonUdf(operator));
      }
    }
  }

  clearResultPanel(): void {
    this.frameComponentConfigs.clear();
  }

  displayConsole(operatorId: string, consoleInputEnabled: boolean) {
    this.frameComponentConfigs.set("Console", {
      component: ConsoleFrameComponent,
      componentInputs: { operatorId, consoleInputEnabled },
    });
  }

  displayError(operatorId: string | undefined) {
    this.frameComponentConfigs.set("Static Error", {
      component: ErrorFrameComponent,
      componentInputs: { operatorId },
    });
  }

  displayResult(operatorId: string) {
    const resultService = this.workflowResultService.getResultService(operatorId);
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      // display table result if it has paginated results
      this.frameComponentConfigs.set("Result", {
        component: ResultTableFrameComponent,
        componentInputs: { operatorId },
      });
    } else if (resultService) {
      // display visualization result
      this.frameComponentConfigs.set("Result", {
        component: VisualizationFrameContentComponent,
        componentInputs: { operatorId },
      });
    }
  }

  private static needRerenderOnStateChange(event: {
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }): boolean {
    // transitioning from any state to failed state
    if (event.current.state === ExecutionState.Failed) {
      return true;
    }

    // force refresh after fixing all editing-time errors
    if (event.previous.state === ExecutionState.Failed) {
      return true;
    }

    // transition from uninitialized / completed to anything else indicates a new execution of the workflow
    return event.previous.state === ExecutionState.Uninitialized || event.previous.state === ExecutionState.Completed;
  }

  openPanel() {
    this.height = this.prevHeight;
    this.width = this.prevWidth;
  }

  closePanel() {
    this.prevHeight = this.height;
    this.prevWidth = this.width;
    this.height = 32.5;
    this.width = 0;
  }

  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
      this.resizeService.changePanelSize(this.width, this.height);
    });
  }
}

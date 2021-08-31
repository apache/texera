import { Component, OnInit } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { ResultPanelToggleService } from "../../../service/result-panel-toggle/result-panel-toggle.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { BreakpointTriggerInfo } from "../../../types/workflow-common.interface";
import { ExecutionState } from "src/app/workspace/types/execute-workflow.interface";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-console-frame",
  templateUrl: "./console-frame.component.html",
  styleUrls: ["./console-frame.component.scss"]
})
export class ConsoleFrameComponent implements OnInit {
  // display error message:
  errorMessages?: Readonly<Record<string, string>>;
  // display breakpoint
  breakpointTriggerInfo?: BreakpointTriggerInfo;
  breakpointAction: boolean = false;

  // display print
  consoleMessages: ReadonlyArray<string> = [];

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowConsoleService: WorkflowConsoleService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderConsole();
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  ngOnInit(): void {
    // make sure the console is re-rendered upon state changes
    this.registerAutoConsoleRerender();
  }

  registerAutoConsoleRerender() {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe((event) => {
        if (
          event.previous.state === ExecutionState.BreakpointTriggered &&
          event.current.state === ExecutionState.Completed
        ) {
          // intentionally do nothing to leave the information displayed as it is
          // when kill a workflow after hitting breakpoint
        } else if (
          event.previous.state === ExecutionState.WaitingToRun &&
          event.current.state === ExecutionState.Running
        ) {
          // clear the console for the next execution
          this.clearConsole();
        } else {
          // re-render the console, this may update the console with error messages or console messages
          this.renderConsole();
        }
      });

    this.workflowConsoleService
      .getConsoleMessageUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe((_) => this.renderConsole());
  }

  onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }

  clearConsole() {
    this.consoleMessages = [];
    this.errorMessages = undefined;
    this.breakpointTriggerInfo = undefined;
  }

  renderConsole() {
    // try to fetch if we have breakpoint info
    const breakpointTriggerInfo =
      this.executeWorkflowService.getBreakpointTriggerInfo();

    if (this.operatorId) {
      // first display error messages if applicable
      if (this.operatorId === breakpointTriggerInfo?.operatorID) {
        // if we hit a breakpoint
        this.displayBreakpoint(breakpointTriggerInfo);
      } else {
        // otherwise we assume it's a fault
        this.displayFault();
      }

      // always display console messages
      this.displayConsoleMessages(this.operatorId);
    }
  }

  displayConsoleMessages(operatorID: string) {
    this.consoleMessages = operatorID
      ? this.workflowConsoleService.getConsoleMessages(operatorID) || []
      : [];
  }

  displayBreakpoint(breakpointTriggerInfo: BreakpointTriggerInfo) {
    this.breakpointTriggerInfo = breakpointTriggerInfo;
    this.breakpointAction = true;
    // const result = breakpointTriggerInfo.report.map(r => r.faultedTuple.tuple).filter(t => t !== undefined);
    // this.setupResultTable(result, result.length);
    const errorsMessages: Record<string, string> = {};
    breakpointTriggerInfo.report.forEach((r) => {
      const splitPath = r.actorPath.split("/");
      const workerName = splitPath[splitPath.length - 1];
      const workerText = "Worker " + workerName + ":                ";
      if (r.messages.toString().toLowerCase().includes("exception")) {
        errorsMessages[workerText] = r.messages.toString();
      }
    });
    this.errorMessages = errorsMessages;
  }

  displayFault() {
    this.errorMessages = this.executeWorkflowService.getErrorMessages();
  }

  displayConsoleMessages(operatorId: string) {
    this.consoleMessages = operatorId
      ? this.workflowConsoleService.getConsoleMessages(operatorId) || []
      : [];
  }
}

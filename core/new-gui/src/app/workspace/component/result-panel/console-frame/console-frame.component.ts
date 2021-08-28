import { Component, OnDestroy, OnInit } from '@angular/core';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { BreakpointTriggerInfo } from '../../../types/workflow-common.interface';
import { ExecutionState } from 'src/app/workspace/types/execute-workflow.interface';
import { WorkflowConsoleService } from '../../../service/workflow-console/workflow-console.service';
import { Subscription } from 'rxjs';

@Component({
  selector: 'texera-console-frame',
  templateUrl: './console-frame.component.html',
  styleUrls: ['./console-frame.component.scss']
})
export class ConsoleFrameComponent implements OnInit, OnDestroy {

  // display error message:
  errorMessages: Readonly<Record<string, string>> | undefined;
  // display breakpoint
  breakpointTriggerInfo: BreakpointTriggerInfo | undefined;
  breakpointAction: boolean = false;

  // display print
  consoleMessages: ReadonlyArray<string> = [];

  subscriptions = new Subscription();

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowConsoleService: WorkflowConsoleService
  ) { }

  ngOnInit(): void {
    // trigger the initial render
    this.renderConsole();

    // make sure the console is re-rendered upon state changes
    this.registerAutoConsoleRerender();
  }

  registerAutoConsoleRerender() {
    this.subscriptions.add(this.executeWorkflowService.getExecutionStateStream()
      .subscribe(event => {
        if (event.previous.state === ExecutionState.BreakpointTriggered && event.current.state === ExecutionState.Completed) {
          // intentionally do nothing to leave the information displayed as it is
          // when kill a workflow after hitting breakpoint
        } else if (event.previous.state === ExecutionState.WaitingToRun && event.current.state === ExecutionState.Running) {
          // clear the console for the next execution
          this.clearConsole();
        } else {
          // re-render the console, this may update the console with error messages or console messages
          this.renderConsole();
        }
      }));
    this.subscriptions.add(this.workflowConsoleService.getConsoleMessageUpdateStream().subscribe(_ => this.renderConsole()));
  }

  public onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }


  private clearConsole() {
    this.consoleMessages = [];
    this.errorMessages = undefined;
    this.breakpointTriggerInfo = undefined;
  }

  private renderConsole() {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

    // try to fetch if we have breakpoint info
    const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();

    if (resultPanelOperatorID) {
      // first display error messages if applicable
      if (resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
        // if we hit a breakpoint
        this.displayBreakpoint(breakpointTriggerInfo);
      } else {
        // otherwise we assume it's a fault
        this.displayFault();
      }

      // always display console messages
      this.displayConsoleMessages(resultPanelOperatorID);
    }
  }

  private displayConsoleMessages(operatorID: string) {
    this.consoleMessages = operatorID ?
      this.workflowConsoleService.getConsoleMessages(operatorID) || [] : [];
  }

  private displayBreakpoint(breakpointTriggerInfo: BreakpointTriggerInfo) {
    this.breakpointTriggerInfo = breakpointTriggerInfo;
    this.breakpointAction = true;
    // const result = breakpointTriggerInfo.report.map(r => r.faultedTuple.tuple).filter(t => t !== undefined);
    // this.setupResultTable(result, result.length);
    const errorsMessages: Record<string, string> = {};
    breakpointTriggerInfo.report.forEach(r => {
      const splitPath = r.actorPath.split('/');
      const workerName = splitPath[splitPath.length - 1];
      const workerText = 'Worker ' + workerName + ':                ';
      if (r.messages.toString().toLowerCase().includes('exception')) {
        errorsMessages[workerText] = r.messages.toString();
      }
    });
    this.errorMessages = errorsMessages;
  }

  private displayFault() {
    this.errorMessages = this.executeWorkflowService.getErrorMessages();
  }
}

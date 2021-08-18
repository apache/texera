import { Component } from '@angular/core';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { BreakpointTriggerInfo } from '../../../types/workflow-common.interface';
import { ExecutionState } from 'src/app/workspace/types/execute-workflow.interface';
import { WorkflowConsoleService } from '../../../service/workflow-console/workflow-console.service';

@Component({
  selector: 'texera-console-frame',
  templateUrl: './console-frame.component.html',
  styleUrls: ['./console-frame.component.scss']
})
export class ConsoleFrameComponent {
  // display error message:
  errorMessages: Readonly<Record<string, string>> | undefined;
  // display breakpoint
  breakpointTriggerInfo: BreakpointTriggerInfo | undefined;
  breakpointAction: boolean = false;

  // display print
  public consoleMessages: string[] = [];

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService,
    private workflowConsoleService: WorkflowConsoleService
  ) {
    this.renderConsole();
    this.registerAutoConsoleRerender();
  }

  registerAutoConsoleRerender() {
    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.previous.state === ExecutionState.Completed && event.current.state === ExecutionState.WaitingToRun) {
        this.clearConsole();
      } else if (event.current.state === ExecutionState.Failed) {
        this.renderConsole();
      } else {
        this.renderConsole();
      }
    });
  }

  public onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }


  private clearConsole() {
    this.consoleMessages = [];
    this.errorMessages = undefined;
  }

  private renderConsole() {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;
    const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
    this.errorMessages = this.executeWorkflowService.getErrorMessages();
    if (this.resultPanelOperatorID) {
      if (this.resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
        this.displayBreakpoint(breakpointTriggerInfo);
      } else {
        this.displayConsoleMessages();
      }
    }
  }

  private displayConsoleMessages() {
    this.consoleMessages = this.resultPanelOperatorID ?
      this.workflowConsoleService.consoleMessages.get(this.resultPanelOperatorID) || [] : [];
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
}

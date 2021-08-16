import { Component } from '@angular/core';
import { ExecuteWorkflowService } from '../../../service/execute-workflow/execute-workflow.service';
import { ResultPanelToggleService } from '../../../service/result-panel-toggle/result-panel-toggle.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { BreakpointTriggerInfo } from '../../../types/workflow-common.interface';
import { ExecutionState } from 'src/app/workspace/types/execute-workflow.interface';

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
    private workflowActionService: WorkflowActionService
  ) {
    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.previous.state === ExecutionState.Completed && event.current.state === ExecutionState.WaitingToRun) {
        this.consoleMessages = [];
      } else if (event.current.state === ExecutionState.Failed) {
        this.errorMessages = this.executeWorkflowService.getErrorMessages();
      } else {
        // update highlighted operator
        const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;
        const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
        const pythonPrintTriggerInfo = this.executeWorkflowService.getPythonPrintTriggerInfo();
        if (resultPanelOperatorID) {
          if (resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
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
          } else if (resultPanelOperatorID === pythonPrintTriggerInfo?.operatorID) {
            this.consoleMessages.push(pythonPrintTriggerInfo.message);
          }
        }
      }
    });
  }

  onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }
}

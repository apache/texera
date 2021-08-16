import { Component, OnInit } from '@angular/core';
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
export class ConsoleFrameComponent implements OnInit {
  // the highlighted operator ID for display result table / visualization / breakpoint
  public resultPanelOperatorID: string | undefined;
  // display error message:
  public errorMessages: Readonly<Record<string, string>> | undefined;
  // display breakpoint
  public breakpointTriggerInfo: BreakpointTriggerInfo | undefined;
  public breakpointAction: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private resultPanelToggleService: ResultPanelToggleService,
    private workflowActionService: WorkflowActionService
  ) { }

  ngOnInit(): void {
    const executionState = this.executeWorkflowService.getExecutionState();

    if (executionState.state === ExecutionState.Failed) {
      this.errorMessages = this.executeWorkflowService.getErrorMessages();
    } else {
      // update highlighted operator
      const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
      this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;
      const breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
      if (this.resultPanelOperatorID && this.resultPanelOperatorID === breakpointTriggerInfo?.operatorID) {
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

  }

  public onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }
}

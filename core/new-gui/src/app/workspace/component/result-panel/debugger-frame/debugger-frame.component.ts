import { Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { BreakpointTriggerInfo } from "../../../types/workflow-common.interface";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-debugger-frame",
  templateUrl: "./debugger-frame.component.html",
  styleUrls: ["./debugger-frame.component.scss"]
})
export class DebuggerFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  // display breakpoint
  breakpointTriggerInfo?: BreakpointTriggerInfo;
  breakpointAction: boolean = false;
  evaluateTrees: object[] = [];
  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderConsole();
  }

  renderConsole() {
    // try to fetch if we have breakpoint info
    this.breakpointTriggerInfo =
      this.executeWorkflowService.getBreakpointTriggerInfo();
    if (this.breakpointTriggerInfo) {
      this.breakpointAction = true;
    }
  }

  onClickSkipTuples(): void {
    this.executeWorkflowService.skipTuples();
    this.breakpointAction = false;
  }

  onClickRetry() {
    this.executeWorkflowService.retryExecution();
    this.breakpointAction = false;
  }

  onClickEvaluate() {
    this.workflowWebsocketService.send("PythonExpressionEvaluateRequest", {
      expression: "self"
    });
  }

  ngOnInit(): void {
    this.workflowWebsocketService
      .subscribeToEvent("PythonExpressionEvaluateResponse")
      .pipe(untilDestroyed(this))
      .subscribe((a) => console.log(a));
  }
}

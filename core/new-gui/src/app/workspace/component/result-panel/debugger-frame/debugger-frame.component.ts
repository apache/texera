import { Component, Input, OnChanges, SimpleChanges } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { BreakpointTriggerInfo } from "../../../types/workflow-common.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@Component({
  selector: "texera-debugger-frame",
  templateUrl: "./debugger-frame.component.html",
  styleUrls: ["./debugger-frame.component.scss"],
})
export class DebuggerFrameComponent implements OnChanges {
  @Input() operatorId?: string;
  // display breakpoint
  breakpointTriggerInfo?: BreakpointTriggerInfo;
  breakpointAction: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private notificationService: NotificationService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderConsole();
  }

  renderConsole() {
    // try to fetch if we have breakpoint info
    this.breakpointTriggerInfo = this.executeWorkflowService.getBreakpointTriggerInfo();
    if (this.breakpointTriggerInfo) {
      this.breakpointAction = true;
    }
  }

  onClickSkipTuples(): void {
    try {
      this.executeWorkflowService.skipTuples();
    } catch (e: any) {
      this.notificationService.error(e);
    }
    this.breakpointAction = false;
  }

  onClickRetry() {
    try {
      this.executeWorkflowService.retryExecution();
    } catch (e: any) {
      this.notificationService.error(e);
    }
    this.breakpointAction = false;
  }
}

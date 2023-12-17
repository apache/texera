import { Component, ElementRef, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { render } from "sass";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";

@UntilDestroy()
@Component({
  selector: "texera-error-frame",
  templateUrl: "./error-frame.component.html",
  styleUrls: ["./error-frame.component.scss"],
})
export class ErrorFrameComponent implements OnInit, OnChanges {
  @Input() operatorId?: string;
  // display error message:
  categoryToErrorMapping: ReadonlyMap<string, ReadonlyArray<WorkflowFatalError>> = new Map();

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    this.operatorId = changes.operatorId?.currentValue;
    this.renderError();
  }

  ngOnInit(): void {
    this.renderError();
  }

  onClickGotoButton(target: string) {
    this.workflowActionService.highlightOperators(false, target);
  }

  renderError(): void {
    let errorMessages = this.executeWorkflowService.getErrorMessages();
    if (this.operatorId) {
      errorMessages = errorMessages.filter(err => err.operatorId === this.operatorId);
    }
    this.categoryToErrorMapping = errorMessages.reduce((acc, obj) => {
      const key = obj.type.name;
      if (!acc.has(key)) {
        acc.set(key, []);
      }
      acc.get(key)!.push(obj);
      return acc;
    }, new Map<string, WorkflowFatalError[]>());
  }
}

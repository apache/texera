import { Component } from "@angular/core";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowResultService } from "src/app/workspace/service/workflow-result/workflow-result.service";
import { WorkflowResultExportService } from "src/app/workspace/service/workflow-result-export/workflow-result-export.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { ResultExportationComponent } from "../../../result-exportation/result-exportation.component";
import { ValidationWorkflowService } from "src/app/workspace/service/validation/validation-workflow.service";

@UntilDestroy()
@Component({
  selector: "texera-context-menu",
  templateUrl: "./context-menu.component.html",
  styleUrls: ["./context-menu.component.scss"],
})
export class ContextMenuComponent {
  public isWorkflowModifiable: boolean = false;

  constructor(
    public workflowActionService: WorkflowActionService,
    public operatorMenuService: OperatorMenuService,
    public workflowResultExportService: WorkflowResultExportService,
    private workflowResultService: WorkflowResultService,
    private modalService: NzModalService,
    private validationWorkflowService: ValidationWorkflowService
  ) {
    this.registerWorkflowModifiableChangedHandler();
  }

  public canExecuteOperator(): boolean {
    if (!this.hasExactlyOneOperatorSelected() || !this.isWorkflowModifiable) {
      return false;
    }

    const operatorID = this.getSelectedOperatorID();
    return this.isOperatorExecutable(operatorID);
  }

  private hasExactlyOneOperatorSelected(): boolean {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().length === 1;
  }

  private getSelectedOperatorID(): string {
    return this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
  }

  private isOperatorExecutable(operatorID: string): boolean {
    return (
      this.validationWorkflowService.validateOperator(operatorID).isValid &&
      !this.workflowActionService.getTexeraGraph().isOperatorDisabled(operatorID)
    );
  }

  public onCopy(): void {
    this.operatorMenuService.saveHighlightedElements();
  }

  public onPaste(): void {
    this.operatorMenuService.performPasteOperation();
  }

  public onCut(): void {
    this.onCopy();
    this.onDelete();
  }

  public onDelete(): void {
    const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const highlightedCommentBoxIDs = this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedCommentBoxIDs();
    this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs);
    highlightedCommentBoxIDs.forEach(highlightedCommentBoxID =>
      this.workflowActionService.deleteCommentBox(highlightedCommentBoxID)
    );
  }

  private registerWorkflowModifiableChangedHandler() {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(modifiable => (this.isWorkflowModifiable = modifiable));
  }

  /**
   * This is the handler for the execution result export button for only highlighted operators.
   *
   */
  public onClickExportHighlightedExecutionResult(exportType: string): void {
    this.modalService.create({
      nzTitle: "Export Highlighted Operators Result",
      nzContent: ResultExportationComponent,
      nzData: {
        exportType: exportType,
        workflowName: this.workflowActionService.getWorkflowMetadata()?.name,
        sourceTriggered: "context-menu",
      },
      nzFooter: null,
    });
  }
}

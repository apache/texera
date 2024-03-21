import { Component } from "@angular/core";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

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
    public operatorMenuService: OperatorMenuService
  ) {
    this.registerWorkflowModifiableChangedHandler();
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
    const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
    const highlightedCommentBoxIDs = this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedCommentBoxIDs();
    this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
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
}

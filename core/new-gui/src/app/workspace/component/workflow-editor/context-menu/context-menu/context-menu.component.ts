import { Component, OnInit } from "@angular/core";
import { OperatorMenuService } from "src/app/workspace/service/operator-menu/operator-menu.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";

@Component({
  selector: "context-menu",
  templateUrl: "./context-menu.component.html",
  styleUrls: ["./context-menu.component.scss"],
})
export class ContextMenuComponent implements OnInit {
  constructor(public workflowActionService: WorkflowActionService, public operatorMenu: OperatorMenuService) {}

  ngOnInit(): void {}

  public onCopy(): void {
    this.operatorMenu.saveHighlightedElements();
  }

  public onPaste(): void {
    this.operatorMenu.performPasteOperation();
  }

  public onCut(): void {
    this.onCopy();
    this.onDelete();
  }

  public onDelete(): void {
    const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
    this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
  }
}

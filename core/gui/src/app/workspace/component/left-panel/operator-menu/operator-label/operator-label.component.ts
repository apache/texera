import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { AfterContentInit, Component, Input } from "@angular/core";
import { OperatorSchema } from "../../../../types/operator-schema.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Point } from "../../../../types/workflow-common.interface";

@UntilDestroy()
@Component({
  selector: "texera-operator-label",
  templateUrl: "operator-label.component.html",
  styleUrls: ["operator-label.component.scss"],
})
export class OperatorLabelComponent implements AfterContentInit {
  @Input() operator?: OperatorSchema;
  public draggable = true;

  constructor(
    private dragDropService: DragDropService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngAfterContentInit(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        this.draggable = canModify;
      });
  }

  dragStarted() {
    if (this.draggable) {
      this.dragDropService.dragStarted(this.operator!.operatorType);
    }
  }

  dragDropped(dropPoint: Point) {
    this.dragDropService.dragDropped(dropPoint);
  }
}

import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { AfterContentInit, AfterViewInit, Component, Input } from "@angular/core";
import { OperatorSchema } from "../../../../types/operator-schema.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

/**
 * OperatorLabelComponent is one operator box in the operator panel.
 *
 * @author Zuozhi Wang
 */
@UntilDestroy()
@Component({
  selector: "texera-operator-label",
  templateUrl: "operator-label.component.html",
  styleUrls: ["operator-label.component.scss"],
})
export class OperatorLabelComponent implements AfterViewInit, AfterContentInit {
  @Input() operator?: OperatorSchema;
  @Input() fromSearchBox?: boolean;
  public operatorLabelID?: string;
  public draggable = true;

  constructor(private dragDropService: DragDropService, private workflowActionService: WorkflowActionService) {}

  ngAfterContentInit(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        this.draggable = canModify;
      });
    this.operatorLabelID = "operator-label-" + this.operator!.operatorType + this.fromSearchBox;
  }

  ngAfterViewInit() {
    this.dragDropService.registerOperatorLabelDrag(this.operatorLabelID!, this.operator!.operatorType);
  }
}

import { Component, Input } from "@angular/core";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { VisualizationFrameContentComponent } from "../../visualization-panel-content/visualization-frame-content.component";

/**
 * VisualizationFrameComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * When user click on button, this component will open VisualizationFrameContentComponent and display figure.
 * User could click close at the button of VisualizationFrameContentComponent to exit the visualization panel.
 */
@Component({
  selector: "texera-visualization-frame",
  templateUrl: "./visualization-frame.component.html",
  styleUrls: ["./visualization-frame.component.scss"],
})
export class VisualizationFrameComponent {
  @Input() operatorId?: string;
  modalRef?: NzModalRef;

  constructor(private modalService: NzModalService) {}

  onClickVisualize(): void {
    if (!this.operatorId) {
      return;
    }

    this.modalRef = this.modalService.create({
      nzTitle: "Visualization",
      nzStyle: { top: "20px" },
      nzWidth: 1100,
      nzFooter: null, // null indicates that the footer of the window would be hidden
      nzContent: VisualizationFrameContentComponent,
      nzComponentParams: {
        operatorId: this.operatorId,
      },
    });
  }
}

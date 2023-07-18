import { Component, Input } from "@angular/core";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { VisualizationFrameContentComponent } from "../../visualization-panel-content/visualization-frame-content.component";
import { ViewChild, TemplateRef } from "@angular/core";

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
  @ViewChild("modalTitleMax") modalTitleMax!: TemplateRef<{}>;
  @ViewChild("modalTitleMin") modalTitleMin!: TemplateRef<{}>;
  modalRef?: NzModalRef;

  constructor(private modalService: NzModalService) {}

  onClickVisualize(): void {
    if (!this.operatorId) {
      return;
    }

    this.modalRef = this.modalService.create({
      nzTitle: this.modalTitleMax,
      nzStyle: { top: "20px" },
      nzWidth: 1100,
      nzContent: VisualizationFrameContentComponent,
      nzBodyStyle: { height: "800px" },
      nzComponentParams: {
        operatorId: this.operatorId,
      },
    });
  }

  maximize(): void {
    // @ts-ignore
    this.modalRef.updateConfig({
      nzTitle: this.modalTitleMin,
      nzWidth: "100vw",
      nzStyle: { top: "0", bottom: "0", left: "0", right: "0", height: "100vh", width: "100vw" },
      nzBodyStyle: { width: "100vw", height: "100vh", padding: "0" },
      nzFooter: null,
    });
  }

  minimize(): void {
    // @ts-ignore
    this.modalRef.updateConfig({
      nzTitle: this.modalTitleMax,
      nzStyle: { top: "20px", width: "1100px", height: "800px" },
      nzWidth: 1100,
      nzContent: VisualizationFrameContentComponent,
      nzBodyStyle: { width: "1100px", height: "800px" },
      nzComponentParams: {
        operatorId: this.operatorId,
      },
    });
  }
}

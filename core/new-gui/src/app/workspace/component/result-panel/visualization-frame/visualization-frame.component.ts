import { Component, Input } from "@angular/core";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { VisualizationFrameContentComponent } from "../../visualization-panel-content/visualization-frame-content.component";
import { ViewChild, TemplateRef } from "@angular/core";
import { FullscreenExitOutline, FullscreenOutline } from "@ant-design/icons-angular/icons";
import { NZ_ICONS } from "ng-zorro-antd/icon";

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
  providers: [{ provide: NZ_ICONS, useValue: [FullscreenExitOutline, FullscreenOutline] }],
})
export class VisualizationFrameComponent {
  @Input() operatorId?: string;
  @ViewChild("modalTitle") modalTitle!: TemplateRef<{}>;
  modalRef?: NzModalRef;
  isFullscreen?: Boolean;

  constructor(private modalService: NzModalService) {}

  onClickVisualize(): void {
    if (!this.operatorId) {
      return;
    }
    this.isFullscreen = false;
    this.modalRef = this.modalService.create({
      nzTitle: this.modalTitle,
      nzStyle: { top: "20px", width: "70vw", height: "78vh" },
      nzContent: VisualizationFrameContentComponent,
      nzFooter: null,
      nzBodyStyle: { width: "70vw", height: "74vh" },
      nzComponentParams: {
        operatorId: this.operatorId,
      },
    });
  }

  toggleFullscreen(): void {
    this.isFullscreen = !this.isFullscreen;
    if (!this.isFullscreen) {
      // @ts-ignore
      this.modalRef.updateConfig({
        nzStyle: { top: "20px", width: "70vw", height: "78vh" },
        nzBodyStyle: { width: "70vw", height: "74vh" },
      });
    } else {
      // @ts-ignore
      this.modalRef.updateConfig({
        nzStyle: { top: "5px", bottom: "0", left: "0", right: "0", width: "100vw", height: "94vh" },
        nzBodyStyle: { width: "98vw", height: "92vh" },
      });
    }
  }
}

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
  @ViewChild("modalTitle") modalTitle: TemplateRef<{}>;
  modalRef?: NzModalRef;
  isFullscreen: Boolean = false;

  constructor(private modalService: NzModalService) {
    this.modalTitle = {} as TemplateRef<any>;
  }

  onClickVisualize(): void {
    if (!this.operatorId) {
      return;
    }
    this.modalRef = this.modalService.create({
      nzTitle: this.modalTitle,
      nzStyle: { top: "20px", width: "70vw", height: "78vh" },
      nzContent: VisualizationFrameContentComponent,
      nzFooter: null, // null indicates that the footer of the window would be hidden
      nzBodyStyle: { width: "70vw", height: "74vh" },
      nzData: {
        operatorId: this.operatorId,
      },
    });
    this.isFullscreen = false;
  }

  toggleFullscreen(): void {
    this.isFullscreen = !this.isFullscreen;
    if (!this.modalRef) {
      return;
    }
    if (!this.isFullscreen) {
      this.modalRef.updateConfig({
        nzStyle: { top: "20px", width: "70vw", height: "78vh" },
        nzBodyStyle: { width: "70vw", height: "74vh" },
      });
    } else {
      this.modalRef.updateConfig({
        nzStyle: { top: "5px", bottom: "0", left: "0", right: "0", width: "100vw", height: "94vh" },
        nzBodyStyle: { width: "98vw", height: "92vh" },
      });
    }
  }
}

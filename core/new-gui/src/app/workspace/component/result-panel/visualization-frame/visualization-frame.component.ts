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
  startDragHandler = (event: MouseEvent) => {
    this.startDrag(event);
  };

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
      nzComponentParams: {
        operatorId: this.operatorId,
      },
      nzClassName: 'draggable-modal',
    });

    const modalElement = document.querySelector('.ant-modal') as HTMLElement; // Cast to HTMLElement
    if (modalElement) {
      modalElement.addEventListener('mousedown', this.startDragHandler);
    }
  }
    
  // Implement the drag functionality
  private startDrag(event: MouseEvent): void {
    const modalElement = document.querySelector('.ant-modal') as HTMLElement;
    if (!modalElement) {
      return;
    }

    const initialX = event.clientX;
    const initialY = event.clientY;

    const onMouseMove = (e: MouseEvent) => {
      const deltaX = e.clientX - initialX;
      const deltaY = e.clientY - initialY;

      modalElement.style.transform = `translate(${deltaX}px, ${deltaY}px)`;
    };

    const onMouseUp = () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
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
      const modalElement = document.querySelector('.ant-modal') as HTMLElement; // Cast to HTMLElement
    if (modalElement) {
      modalElement.addEventListener('mousedown', this.startDragHandler);
    }
    } else {
      this.modalRef.updateConfig({
        nzStyle: { top: "50%",       // Vertically center the modal
        left: "0",      // Horizontally center the modal
        transform: "translate(0, -50%)", // Center using translate
        width: "100vw", height: "100vh" },
        nzBodyStyle: { width: "98vw", height: "94vh" },
      });
      const modalElement = document.querySelector('.ant-modal') as HTMLElement; // Cast to HTMLElement
    if (modalElement) {
      modalElement.removeEventListener('mousedown', this.startDragHandler);
    }
  }
  }
}

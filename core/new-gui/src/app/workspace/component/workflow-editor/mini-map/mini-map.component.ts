import { AfterViewInit, Component } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { MAIN_CANVAS } from "../workflow-editor.component";
import * as joint from "jointjs";

@UntilDestroy()
@Component({
  selector: "texera-mini-map",
  templateUrl: "mini-map.component.html",
  styleUrls: ["mini-map.component.scss"],
})
export class MiniMapComponent implements AfterViewInit {
  scale = 0;
  paper!: joint.dia.Paper;
  dragging = false;
  constructor(private workflowActionService: WorkflowActionService) {}
  ngAfterViewInit() {
    const map = document.getElementById("mini-map")!;
    this.scale = map.offsetWidth / (MAIN_CANVAS.xMax - MAIN_CANVAS.xMin);
    new joint.dia.Paper({
      el: map,
      model: this.workflowActionService.getJointGraphWrapper().jointGraph,
      background: { color: "#F6F6F6" },
      interactive: false,
      width: map.offsetWidth,
      height: map.offsetHeight,
    })
      .scale(this.scale)
      .translate(-MAIN_CANVAS.xMin * this.scale, -MAIN_CANVAS.yMin * this.scale);
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaperAttachedStream()
      .pipe(untilDestroyed(this))
      .subscribe(mainPaper => {
        this.paper = mainPaper;
        this.updateNavigator();
        mainPaper.on("translate", () => this.updateNavigator());
        mainPaper.on("scale", () => this.updateNavigator());
        mainPaper.on("resize", () => this.updateNavigator());
      });
  }

  onDrag(event: any) {
    this.paper.translate(
      this.paper.translate().tx + -event.event.movementX / this.scale,
      this.paper.translate().ty + -event.event.movementY / this.scale
    );
  }

  private updateNavigator(): void {
    if (!this.dragging) {
      const point = this.paper.pageToLocalPoint({ x: 0, y: 0 });
      const editor = document.getElementById("workflow-editor")!;
      const navigator = document.getElementById("mini-map-navigator")!;
      navigator.style.left = (point.x - MAIN_CANVAS.xMin) * this.scale + "px";
      navigator.style.top = (point.y - MAIN_CANVAS.yMin) * this.scale + "px";
      navigator.style.width = (editor.offsetWidth / this.paper.scale().sx) * this.scale + "px";
      navigator.style.height = (editor.offsetHeight / this.paper.scale().sy) * this.scale + "px";
    }
  }
}

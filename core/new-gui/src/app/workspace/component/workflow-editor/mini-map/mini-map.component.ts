import { AfterViewInit, Component } from "@angular/core";
import { fromEvent } from "rxjs";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { Point } from "../../../types/workflow-common.interface";
import { auditTime } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { MAIN_CANVAS_LIMIT } from "../workflow-editor-constants";
import { WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID } from "../workflow-editor.component";

@UntilDestroy()
@Component({
  selector: "texera-mini-map",
  templateUrl: "mini-map.component.html",
  styleUrls: ["mini-map.component.scss"],
})
export class MiniMapComponent implements AfterViewInit {
  mini_map!: HTMLElement;
  navigator!: HTMLElement;
  workflowEditor!: HTMLElement;
  scale = 0;

  constructor(private workflowActionService: WorkflowActionService) {}

  ngAfterViewInit() {
    this.mini_map = document.getElementById("mini-map")!;
    this.workflowEditor = document.getElementById(WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID)!;
    this.navigator = document.getElementById("mini-map-navigator")!;
    this.scale = this.mini_map.offsetWidth / (MAIN_CANVAS_LIMIT.xMax - MAIN_CANVAS_LIMIT.xMin);
    const miniMapPaper = this.workflowActionService.getJointGraphWrapper().attachMiniMapJointPaper({
      el: this.mini_map,
      gridSize: 45,
      background: { color: "#F6F6F6" },
      interactive: false,
      width: this.mini_map.offsetWidth,
      height: this.mini_map.offsetHeight,
    });

    miniMapPaper.translate(-MAIN_CANVAS_LIMIT.xMin * this.scale, -MAIN_CANVAS_LIMIT.yMin * this.scale);
    miniMapPaper.scale(this.scale);

    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaperAttachedStream()
      .pipe(untilDestroyed(this))
      .subscribe(mainPaper => {
        mainPaper.on("translate", () => this.updateNavigator());
        mainPaper.on("scale", () => this.updateNavigator());
      });

    let mouseDownPosition: Point | undefined;
    fromEvent<MouseEvent>(this.navigator!, "mousedown")
      .pipe(untilDestroyed(this))
      .subscribe(event => (mouseDownPosition = { x: event.screenX, y: event.screenY }));

    fromEvent(document, "mouseup")
      .pipe(untilDestroyed(this))
      .subscribe(() => (mouseDownPosition = undefined));

    fromEvent<MouseEvent>(document, "mousemove")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (mouseDownPosition) {
          const newCoordinate = { x: event.screenX, y: event.screenY };
          this.workflowActionService.getJointGraphWrapper().navigatorMoveDelta.next({
            deltaX: -(newCoordinate.x - mouseDownPosition.x) / this.scale,
            deltaY: -(newCoordinate.y - mouseDownPosition.y) / this.scale,
          });
          mouseDownPosition = newCoordinate;
        }
      });

    fromEvent(window, "resize")
      .pipe(auditTime(30))
      .pipe(untilDestroyed(this))
      .subscribe(() => this.updateNavigator());
  }

  private updateNavigator(): void {
    const mainPaperWrapper = this.workflowActionService.getJointGraphWrapper();
    const mainPaperScale = mainPaperWrapper.getMainJointPaper()?.scale()!;
    const mainPaperPoint = mainPaperWrapper.pageToJointLocalCoordinate({
      x: this.workflowEditor.offsetLeft,
      y: this.workflowEditor.offsetTop,
    });
    this.navigator.style.left =
      this.mini_map.offsetLeft + (mainPaperPoint.x - MAIN_CANVAS_LIMIT.xMin) * this.scale + "px";
    this.navigator.style.top =
      this.mini_map.offsetTop + (mainPaperPoint.y - MAIN_CANVAS_LIMIT.yMin) * this.scale + "px";
    this.navigator.style.width = (this.workflowEditor.offsetWidth / mainPaperScale.sx) * this.scale + "px";
    this.navigator.style.height = (this.workflowEditor.offsetHeight / mainPaperScale.sy) * this.scale + "px";
  }
}

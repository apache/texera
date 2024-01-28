import { AfterViewInit, Component } from "@angular/core";
import { fromEvent } from "rxjs";
import { auditTime, takeUntil } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { MAIN_CANVAS_LIMIT } from "../workflow-editor-constants";
import * as joint from "jointjs";

@UntilDestroy()
@Component({
  selector: "texera-mini-map",
  templateUrl: "mini-map.component.html",
  styleUrls: ["mini-map.component.scss"],
})
export class MiniMapComponent implements AfterViewInit {
  scale = 0;
  constructor(private workflowActionService: WorkflowActionService) {}
  ngAfterViewInit() {
    const map = document.getElementById("mini-map")!;
    this.scale = map.offsetWidth / (MAIN_CANVAS_LIMIT.xMax - MAIN_CANVAS_LIMIT.xMin);
    new joint.dia.Paper({
      el: map,
      model: this.workflowActionService.getJointGraphWrapper().jointGraph,
      background: { color: "#F6F6F6" },
      interactive: false,
      width: map.offsetWidth,
      height: map.offsetHeight,
    })
      .scale(this.scale)
      .translate(-MAIN_CANVAS_LIMIT.xMin * this.scale, -MAIN_CANVAS_LIMIT.yMin * this.scale);
    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaperAttachedStream()
      .pipe(untilDestroyed(this))
      .subscribe(mainPaper => {
        mainPaper.on("translate", () => this.updateNavigator());
        mainPaper.on("scale", () => this.updateNavigator());
      });
    fromEvent(document.getElementById("mini-map-navigator")!, "mousedown")
      .pipe(untilDestroyed(this))
      .subscribe(() =>
        fromEvent<MouseEvent>(document, "mousemove")
          .pipe(takeUntil(fromEvent(document, "mouseup")))
          .subscribe(event =>
            this.workflowActionService
              .getJointGraphWrapper()
              .navigatorMoveDelta.next({ deltaX: -event.movementX / this.scale, deltaY: -event.movementY / this.scale })
          )
      );
    fromEvent(window, "resize")
      .pipe(auditTime(30))
      .pipe(untilDestroyed(this))
      .subscribe(() => this.updateNavigator());
  }

  private updateNavigator(): void {
    const mainPaper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    const mainPaperPoint = mainPaper.pageToLocalPoint({ x: 0, y: 0 });
    const editor = document.getElementById("workflow-editor")!;
    const navigator = document.getElementById("mini-map-navigator")!;
    navigator.style.left = (mainPaperPoint.x - MAIN_CANVAS_LIMIT.xMin) * this.scale + "px";
    navigator.style.top = (mainPaperPoint.y - MAIN_CANVAS_LIMIT.yMin) * this.scale + "px";
    navigator.style.width = (editor.offsetWidth / mainPaper.scale().sx) * this.scale + "px";
    navigator.style.height = (editor.offsetHeight / mainPaper.scale().sy) * this.scale + "px";
  }
}

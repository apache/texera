import { AfterViewInit, Component } from "@angular/core";
import { fromEvent } from "rxjs";

// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from "jquery";
import * as joint from "jointjs";

import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { Point } from "../../../types/workflow-common.interface";
import { MAIN_CANVAS_LIMIT } from "../workflow-editor-constants";
import { WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID } from "../workflow-editor.component";
import { auditTime } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

/**
 * The mini map component is bound to a JointJS Paper. The mini map's paper uses the same graph/model
 *  as the main workflow (WorkflowEditorComponent's model)
 */
@UntilDestroy()
@Component({
  selector: "texera-mini-map",
  templateUrl: "mini-map.component.html",
  styleUrls: ["mini-map.component.scss"],
})
export class MiniMapComponent implements AfterViewInit {
  mini_map: JQuery<HTMLElement> = jQuery();
  workflowEditor: JQuery<HTMLElement> = jQuery();
  navigator: HTMLElement | undefined | null;

  scale = 0;
  width = 0;
  height = 0;

  private miniMapPaper: joint.dia.Paper | undefined;

  constructor(private workflowActionService: WorkflowActionService) {}

  ngAfterViewInit() {
    this.mini_map = jQuery("texera-mini-map");
    this.workflowEditor = jQuery("#" + WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID);
    this.navigator = document.getElementById("mini-map-navigator");

    this.width = this.mini_map.width()!;
    this.height = this.mini_map.height()!;
    this.scale = this.width / (MAIN_CANVAS_LIMIT.xMax - MAIN_CANVAS_LIMIT.xMin);
    this.miniMapPaper = this.workflowActionService.getJointGraphWrapper().attachMiniMapJointPaper({
      el: jQuery("#mini-map"),
      gridSize: 45,
      background: { color: "#F6F6F6" },
      interactive: false,
      width: this.width,
      height: this.height,
    });

    this.miniMapPaper.translate(-MAIN_CANVAS_LIMIT.xMin * this.scale, -MAIN_CANVAS_LIMIT.yMin * this.scale);
    this.miniMapPaper.scale(this.scale);

    this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaperAttachedStream()
      .pipe(untilDestroyed(this))
      .subscribe(mainPaper => {
        mainPaper.on("translate", () => this.updateNavigator());
        mainPaper.on("scale", () => this.updateNavigator());
      });
    this.handleMouseEvents();

    fromEvent(window, "resize")
      .pipe(auditTime(30))
      .pipe(untilDestroyed(this))
      .subscribe(() => this.updateNavigator());
  }

  public handleMouseEvents() {
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
  }

  public mainPaperToMiniMapPoint(point: Point): Point {
    return { x: (point.x - MAIN_CANVAS_LIMIT.xMin) * this.scale, y: (point.y - MAIN_CANVAS_LIMIT.yMin) * this.scale };
  }

  private updateNavigator(): void {
    // set navigator position in the component
    const mainPaperWrapper = this.workflowActionService.getJointGraphWrapper();
    const mainPaperPoint = mainPaperWrapper.pageToJointLocalCoordinate({
      x: this.workflowEditor.offset()!.left,
      y: this.workflowEditor.offset()!.top,
    } as Point);
    const miniMapPoint = this.mainPaperToMiniMapPoint(mainPaperPoint);
    this.navigator!.style.left = this.mini_map.offset()!.left + miniMapPoint.x + "px";
    this.navigator!.style.top = this.mini_map.offset()!.top + miniMapPoint.y + "px";

    const mainPaperScale = mainPaperWrapper.getMainJointPaper()?.scale()!;
    this.navigator!.style.width = (this.workflowEditor.width()! / mainPaperScale.sx) * this.scale + "px";
    this.navigator!.style.height = (this.workflowEditor.height()! / mainPaperScale.sy) * this.scale + "px";
  }
}

import { Component, AfterViewInit, Input } from '@angular/core';
import { Observable } from 'rxjs/Observable';

// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from 'jquery';
import * as joint from 'jointjs';

import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { Point } from '../../../types/workflow-common.interface';
import { MAIN_CANVAS_LIMIT } from '../workflow-editor-constants';

/**
 * MiniMapComponent is the componenet that contains the mini-map of the workflow editor component.
 *  This component is used for navigating on the workflow editor paper.
 *
 * The mini map component is bound to a JointJS Paper. The mini map's paper uses the same graph/model
 *  as the main workflow (WorkflowEditorComponent's model), making it so that the map will always have
 *  the same operators and links as the main workflow.
 *
 * @author Cynthia Wang
 * @author Henry Chen
 */
@Component({
  selector: 'texera-mini-map',
  templateUrl: './mini-map.component.html',
  styleUrls: ['./mini-map.component.scss']
})
export class MiniMapComponent implements AfterViewInit {

  // the DOM element ID of map. It can be used by jQuery and jointJS to find the DOM element
  // in the HTML template, the div element ID is set using this variable
  public readonly MINI_MAP_JOINTJS_MAP_WRAPPER_ID = 'texera-mini-map-editor-jointjs-wrapper-id';
  public readonly MINI_MAP_JOINTJS_MAP_ID = 'texera-mini-map-editor-jointjs-body-id';
  public readonly MINI_MAP_NAVIGATOR_ID = 'mini-map-navigator-id';
  public readonly MINI_MAP_GRID_SIZE = 45;

  public MINI_MAP_ZOOM_SCALE = 0.12;
  public MINI_MAP_SIZE = {
    width: (MAIN_CANVAS_LIMIT.xMax - MAIN_CANVAS_LIMIT.xMin) * this.MINI_MAP_ZOOM_SCALE,
    height: (MAIN_CANVAS_LIMIT.yMax - MAIN_CANVAS_LIMIT.yMin) * this.MINI_MAP_ZOOM_SCALE
  };


  @Input()
  public mainPaperWrapperElementID: string | undefined;
  public show: boolean = true;

  private mouseDownPosition: Point | undefined;
  private miniMapPaper: joint.dia.Paper | undefined;


  constructor(private workflowActionService: WorkflowActionService) { }

  ngAfterViewInit() {
    this.initializeMapPaper();
    this.handleMouseEvents();
    this.handleWindowResize();
  }

  public handleMouseEvents() {

    const navigatorElement = document.getElementById(this.MINI_MAP_NAVIGATOR_ID);
    if (navigatorElement == null) {
      throw new Error('minimap: cannot find navigator element');
    }

    Observable.fromEvent<MouseEvent>(navigatorElement, 'mousedown')
      .subscribe(event => {
        const x = event.screenX;
        const y = event.screenY;
        if (x !== undefined && y !== undefined) {
          this.mouseDownPosition = { x, y };
        }
      });

    Observable.fromEvent(document, 'mouseup')
      .subscribe(() => {
        this.mouseDownPosition = undefined;
      });

    const mousePanEvent = Observable.fromEvent<MouseEvent>(document, 'mousemove')
      .subscribe(event => {
        if (this.mouseDownPosition) {
          const newCoordinate = { x: event.screenX, y: event.screenY };
          const panDelta = {
            deltaX: - (newCoordinate.x - this.mouseDownPosition.x) / this.MINI_MAP_ZOOM_SCALE,
            deltaY: - (newCoordinate.y - this.mouseDownPosition.y) / this.MINI_MAP_ZOOM_SCALE
          };
          this.mouseDownPosition = newCoordinate;

          this.workflowActionService.getJointGraphWrapper().navigatorMoveDelta.next(panDelta);
        }
      });
  }

  /**
   * This function is used to initialize the minimap paper by passing
   *  the same paper from the workspace editor.
   *
   * @param workflowPaper original JointJS paper from workspace editor
   */
  public initializeMapPaper(): void {

    const miniMapPaperOptions: joint.dia.Paper.Options = {
      el: jQuery(`#${this.MINI_MAP_JOINTJS_MAP_ID}`),
      gridSize: this.MINI_MAP_GRID_SIZE,
      background: { color: '#F7F6F6' },
      interactive: false,
      width: this.MINI_MAP_SIZE.width,
      height: this.MINI_MAP_SIZE.height
    };
    this.miniMapPaper = this.workflowActionService.getJointGraphWrapper().attachMiniMapJointPaper(miniMapPaperOptions);

    const origin = this.mainPaperToMiniMapPoint({ x: 0, y: 0 });
    this.miniMapPaper.translate(origin.x, origin.y);
    this.miniMapPaper.scale(this.MINI_MAP_ZOOM_SCALE);


    this.workflowActionService.getJointGraphWrapper().getMainJointPaperAttachedStream().subscribe(mainPaper => {

      this.updateNavigatorOffset();
      this.updateNavigatorDimension();

      mainPaper.on('translate', () => {
        this.updateNavigatorOffset();
      });
      mainPaper.on('scale', (event: any) => {
        this.updateNavigatorOffset();
        this.updateNavigatorDimension();
      });
    });

  }

  public mainPaperToMiniMapPoint(point: Point): Point {
    // calculate the distance from (x, y) to the main canvas border
    const xOffset = point.x - MAIN_CANVAS_LIMIT.xMin;
    const yOffset = point.y - MAIN_CANVAS_LIMIT.yMin;

    // calculate how much distance it should be on the mini map
    const x = xOffset * this.MINI_MAP_ZOOM_SCALE;
    const y = yOffset * this.MINI_MAP_ZOOM_SCALE;

    return { x, y };
  }


  /**
   * When window is resized, recalculate navigatorOffset, reset mini-map's dimensions,
   *  recompute navigator dimension, and reset mini-map origin offset (introduce
   *  a delay to limit only one event every 30ms)
   */
  private handleWindowResize(): void {
    Observable.fromEvent(window, 'resize').auditTime(30).subscribe(
      () => {
        this.updateNavigatorDimension();
      }
    );
  }

  private updateNavigatorOffset(): void {
    // set navigator position in the component
    const mainPaperPoint = this.workflowActionService.getJointGraphWrapper()
      .pageToJointLocalCoordinate(this.getMainPaperWrapperElementOffset());
    const miniMapPoint = this.mainPaperToMiniMapPoint(mainPaperPoint);
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).css({ left: miniMapPoint.x + 'px', top: miniMapPoint.y + 'px' });
  }

  /**
   * This method sets the dimension of the navigator based on the browser size.
   */
  private updateNavigatorDimension(): void {
    const { width: mainPaperWidth, height: mainPaperHeight } = this.getOriginalWrapperElementSize();
    const mainPaperScale = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.scale() ?? { sx: 1, sy: 1 };

    // set navigator dimension size, mainPaperDimension * MINI_MAP_ZOOM_SCALE is the
    //  main paper's size in the mini-map
    const width = (mainPaperWidth / mainPaperScale.sx) * this.MINI_MAP_ZOOM_SCALE;
    const height = (mainPaperHeight / mainPaperScale.sy) * this.MINI_MAP_ZOOM_SCALE;
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).width(width);
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).height(height);
  }

  private getMainPaperWrapperElementOffset(): Point {
    if (!this.mainPaperWrapperElementID) {
      throw new Error('main paper wrapper element ID is unknown');
    }
    const offset = jQuery('#' + this.mainPaperWrapperElementID).offset();
    if (offset === undefined) {
      throw new Error('fail to get Workflow Editor wrapper element offset');
    }
    return { x: offset.left, y: offset.top };
  }

  /**
   * This method gets the original paper wrapper size.
   */
  private getOriginalWrapperElementSize(): { width: number, height: number } {
    if (!this.mainPaperWrapperElementID) {
      throw new Error('main paper wrapper element ID is unknown');
    }

    let width = jQuery('#' + this.mainPaperWrapperElementID).width();
    let height = jQuery('#' + this.mainPaperWrapperElementID).height();

    // when testing, width and height will be undefined, this gives default value
    //  according to css grids
    width = width === undefined ? window.innerWidth * 0.70 : width;
    height = height === undefined ? window.innerHeight - 56 - 25 : height;

    return { width, height };
  }

}

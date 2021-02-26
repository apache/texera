import { Component, AfterViewInit, Input } from '@angular/core';
import { Observable } from 'rxjs/Observable';

// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from 'jquery';
import * as joint from 'jointjs';

import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';
import { Point } from '../../../types/workflow-common.interface';
import { MINI_MAP_SIZE, MAIN_CANVAS_LIMIT, MINI_MAP_ZOOM_SCALE } from '../workflow-editor-constants';

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
  public readonly WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID = 'texera-workflow-editor-jointjs-wrapper-id';
  public readonly MINI_MAP_NAVIGATOR_ID = 'mini-map-navigator-id';


  private mouseDownPosition: Point | undefined;


  private readonly MINI_MAP_GRID_SIZE = 45;
  private miniMapPaper: joint.dia.Paper | undefined;

  // private ifMouseDown: boolean = false;
  // private panOffset: Point = { x: 0, y: 0 };
  // private navigatorElementOffset: Point = { x: 0, y: 0 };
  // private navigatorInitialOffset: Point = { x: 0, y: 0 };
  // private navigatorCoordinate: Point = { x: 0, y: 0 };
  // private navigatorOriginalWidth = 0;
  // private navigatorOriginalHeight = 0;
  // private navigatorTop = 0;
  // private navigatorLeft = 0;



  constructor(private workflowActionService: WorkflowActionService) { }

  ngAfterViewInit() {
    this.initializeMapPaper();
    this.setMiniMapNavigatorDimension();
    this.handleMouseEvents();
    this.handleWindowResize();
    // this.handleMinimapTranslate();
    this.handlePaperZoom();
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
            deltaX: - (newCoordinate.x - this.mouseDownPosition.x) / MINI_MAP_ZOOM_SCALE,
            deltaY: - (newCoordinate.y - this.mouseDownPosition.y) / MINI_MAP_ZOOM_SCALE
          };
          this.mouseDownPosition = newCoordinate;

          this.workflowActionService.getJointGraphWrapper().navigatorMoveDelta.next(panDelta);
        }
      });
  }

  public getMiniMapPaper(): joint.dia.Paper {
    if (this.miniMapPaper === undefined) {
      throw new Error('JointJS Map paper is undefined');
    }
    return this.miniMapPaper;
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
      width: MINI_MAP_SIZE.width,
      height: MINI_MAP_SIZE.height
    };
    this.miniMapPaper = this.workflowActionService.getJointGraphWrapper().attachMinimapJointPaper(miniMapPaperOptions);

    const origin = this.canvasToMinimapPoint({ x: 0, y: 0 });
    this.miniMapPaper.translate(origin.x, origin.y);
    this.miniMapPaper.scale(MINI_MAP_ZOOM_SCALE);

    // this.navigatorElementOffset = this.getNavigatorElementOffset();
    // this.setMiniPaperOriginOffset();
    this.setMiniMapNavigatorInitialOffset();
    this.setMiniMapNavigatorDimension();
    // this.setMapPaperDimensions();



    this.workflowActionService.getJointGraphWrapper().mainCanvasOriginEvent.subscribe(event => {
      // set navigator position in the component
      const point = this.canvasToMinimapPoint({ x: -event.x, y: -event.y });
      jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).css({ left: point.x + 'px', top: point.y + 'px' });
    });

  }

  /**
   * Handles the panning event from the workflow editor and reflect translation changes
   *  on the mini-map paper. There will be 2 events from the main workflow paper
   *
   * 1. Paper panning event
   * 2. Paper pan offset restore default event
   *
   * Both events return a position in which the paper should translate to.
   */
  public handleMinimapTranslate(): void {
    const { width: mainPaperWidth, height: mainPaperHeight } = this.getOriginalWrapperElementSize();
    const { width: miniMapWidth, height: miniMapHeight } = this.getWrapperElementSize();
    const marginTop = (miniMapHeight - mainPaperHeight * MINI_MAP_ZOOM_SCALE) / 2;
    const marginLeft = (miniMapWidth - mainPaperWidth * MINI_MAP_ZOOM_SCALE) / 2;

    // Minimap Navigator Dimension
    const minX = 0;
    const minY = 0;
    const maxX = (miniMapWidth - mainPaperWidth * MINI_MAP_ZOOM_SCALE);
    const maxY = (miniMapHeight - mainPaperHeight * MINI_MAP_ZOOM_SCALE);
  }

  public canvasToMinimapPoint(point: Point): Point {
    // calculate the distance from (x, y) to the main canvas border
    const xOffset = point.x - MAIN_CANVAS_LIMIT.xMin;
    const yOffset = point.y - MAIN_CANVAS_LIMIT.yMin;

    // calculate how much distance it should be on the mini map
    const x = xOffset * MINI_MAP_ZOOM_SCALE;
    const y = yOffset * MINI_MAP_ZOOM_SCALE;

    return { x, y };
  }

  public mouseDown(event: MouseEvent) {
    console.log(event);
    // this.mouseDownPosition = true;
    this.mouseDownPosition = { x: event.screenX, y: event.screenY };
    // this.navigatorInitialOffset = {x: e.target.offsetLeft, y: e.target.offsetTop};
    // this.workflowActionService.getJointGraphWrapper().setMouseDownReminder(this.ifMouseDown);
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(newRatio => {
      this.setMiniMapNavigatorDimension();
      // this.getMiniMapPaper().scale(newRatio * MINI_MAP_ZOOM_SCALE, newRatio * MINI_MAP_ZOOM_SCALE);
      // const e = jQuery('#' + this.MINI_MAP_NAVIGATOR_ID);
      // const prevWidth = e.width() ?? 150;
      // const prevHeight = e.height() ?? 150;
      // const width = this.navigatorOriginalWidth * (1 / newRatio);
      // const height = this.navigatorOriginalHeight * (1 / newRatio);
      // e.css('width', width);
      // e.css('height', height);
      // e.css('top', e.position().top - (height - prevHeight) / 2 + 'px');
      // e.css('left', e.position().left - (width - prevWidth) / 2 + 'px');
    });
  }

  /**
   * When window is resized, recalculate navigatorOffset, reset mini-map's dimensions,
   *  recompute navigator dimension, and reset mini-map origin offset (introduce
   *  a delay to limit only one event every 30ms)
   */
  private handleWindowResize(): void {
    Observable.fromEvent(window, 'resize').auditTime(30).subscribe(
      () => {
        this.setMiniMapNavigatorDimension();
      }
    );
  }

  private setMiniMapNavigatorInitialOffset(): void {
      // set navigator position in the component
      const point = this.canvasToMinimapPoint({ x: 0, y: 0 });
      jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).css({ left: point.x + 'px', top: point.y + 'px' });
  }

  /**
   * This method sets the dimension of the navigator based on the browser size.
   */
  private setMiniMapNavigatorDimension(): void {
    const { width: mainPaperWidth, height: mainPaperHeight } = this.getOriginalWrapperElementSize();
    const zoomRatio = this.workflowActionService.getJointGraphWrapper().getZoomRatio();

    // set navigator dimension size, mainPaperDimension * MINI_MAP_ZOOM_SCALE is the
    //  main paper's size in the mini-map
    const width = (mainPaperWidth / zoomRatio) * MINI_MAP_ZOOM_SCALE;
    const height = (mainPaperHeight / zoomRatio) * MINI_MAP_ZOOM_SCALE;
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).width(width);
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).height(height);

  }


  /**
   * This method gets the original paper wrapper size.
   */
  private getOriginalWrapperElementSize(): { width: number, height: number } {
    let width = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).width();
    let height = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).height();

    // when testing, width and height will be undefined, this gives default value
    //  according to css grids
    width = width === undefined ? window.innerWidth * 0.70 : width;
    height = height === undefined ? window.innerHeight - 56 - 25 : height;

    return { width, height };
  }

  /**
   * This method gets the mini-map paper wrapper dimensions.
   */
  private getWrapperElementSize(): { width: number, height: number } {
    const e = jQuery('#' + this.MINI_MAP_JOINTJS_MAP_WRAPPER_ID);
    const width = e.width();
    const height = e.height();

    if (width === undefined || height === undefined) {
      throw new Error('fail to get mini-map wrapper element size');
    }
    return { width, height };
  }

  // /**
  //  * This method sets the mini-map paper width and height.
  //  */
  // private setMapPaperDimensions(): void {
  //   const size = this.getWrapperElementSize();
  //   this.getMiniMapPaper().setDimensions(size.width, size.height);
  // }

}

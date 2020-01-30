import { Component, AfterViewInit } from '@angular/core';
import { Observable } from 'rxjs/Observable';

// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from 'jquery';
import * as joint from 'jointjs';

import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { Point } from '../../types/workflow-common.interface';

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

  private readonly MINI_MAP_ZOOM_SCALE = 0.15;
  private readonly MINI_MAP_GRID_SIZE = 45;
  private miniMapPaper: joint.dia.Paper | undefined;

  private panOffset: Point = {x: 0, y: 0};
  private navigatorElementOffset: Point = {x: 0, y: 0};

  constructor(private workflowActionService: WorkflowActionService) { }

  ngAfterViewInit() {
    this.initializeMapPaper();
    this.handleWindowResize();
    this.handleMinimapTranslate();
    this.handlePaperZoom();
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
  private initializeMapPaper(): void {

    const miniMapPaperOptions: joint.dia.Paper.Options = {
      el: jQuery(`#${this.MINI_MAP_JOINTJS_MAP_ID}`),
      gridSize: this.MINI_MAP_GRID_SIZE,
      background: { color:  '#F7F6F6' },
      interactive: false
    };
    this.workflowActionService.attachJointPaper(miniMapPaperOptions);

    this.miniMapPaper =  new joint.dia.Paper(miniMapPaperOptions);
    this.miniMapPaper.scale(this.MINI_MAP_ZOOM_SCALE);

    this.navigatorElementOffset = this.getNavigatorElementOffset();
    this.setMiniPaperOriginOffset();
    this.setMiniMapNavigatorDimension();
    this.setMapPaperDimensions();
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
  private handleMinimapTranslate(): void {
    Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getPanPaperOffsetStream(),
      this.workflowActionService.getJointGraphWrapper().getRestorePaperOffsetStream()
    ).subscribe(newOffset => {
      this.panOffset = newOffset;
      this.getMiniMapPaper().translate(
        -this.navigatorElementOffset.x + newOffset.x * this.MINI_MAP_ZOOM_SCALE,
        -this.navigatorElementOffset.y + newOffset.y * this.MINI_MAP_ZOOM_SCALE
      );
      }
    );
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(newRatio => {
      this.getMiniMapPaper().scale(newRatio * this.MINI_MAP_ZOOM_SCALE, newRatio * this.MINI_MAP_ZOOM_SCALE);
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
        // get navigatorElementOffset here to prevent recomputing everytime
        this.navigatorElementOffset = this.getNavigatorElementOffset();
        this.setMapPaperDimensions();
        this.setMiniMapNavigatorDimension();
        this.setMiniPaperOriginOffset();
      }
    );
  }


  /**
   * Modifies the JointJS paper origin coordinates by shifting it to the left
   *  top (minus the x and y offset of the wrapper element and navigator margin)
   *  so that elements in JointJS paper have the same coordinates as the actual document.
   *  and we don't have to convert between JointJS coordinates and actual coordinates.
   *
   * panOffset is added to this translation to consider the situation that the paper
   *  has been panned by the user previously.
   *
   */
  private setMiniPaperOriginOffset(): void {
    this.getMiniMapPaper().translate(-this.navigatorElementOffset.x + this.panOffset.x  * this.MINI_MAP_ZOOM_SCALE,
      -this.navigatorElementOffset.y + this.panOffset.y  * this.MINI_MAP_ZOOM_SCALE);
  }

  /**
   * This method sets the dimension of the navigator based on the browser size.
   */
  private setMiniMapNavigatorDimension(): void {
    const { width: mainPaperWidth, height: mainPaperHeight } = this.getOriginalWrapperElementSize();
    const { width: miniMapWidth, height: miniMapHeight } = this.getWrapperElementSize();

    // set navigator dimension size, mainPaperDimension * this.MINI_MAP_ZOOM_SCALE is the
    //  main paper's size in the mini-map
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).width(mainPaperWidth * this.MINI_MAP_ZOOM_SCALE);
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).height(mainPaperHeight * this.MINI_MAP_ZOOM_SCALE);

    // set navigator position in the component
    const marginTop = (miniMapHeight - mainPaperHeight * this.MINI_MAP_ZOOM_SCALE) / 2;
    const marginLeft = (miniMapWidth - mainPaperWidth * this.MINI_MAP_ZOOM_SCALE) / 2;
    jQuery('#' + this.MINI_MAP_NAVIGATOR_ID).css({top: marginTop + 'px', left: marginLeft + 'px'});
  }

  /**
   * This method will calculate and return the top-left coordinate in the mini-map navigator.
   */
  private getNavigatorElementOffset(): { x: number, y: number } {
    // Gets the document offset coordinates of the original wrapper element's top-left corner.
    const offset = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).offset();

    // when testing, offset will be undefined, this gives default value
    //  according to css grids
    const offsetLeft = offset === undefined ? window.innerWidth * 0.14 : offset.left;
    const offsetTop = offset === undefined ? 56.0 : offset.top;


    // get the margin size of the navigator in the mini-map
    const { width: mainPaperWidth, height: mainPaperHeight } = this.getOriginalWrapperElementSize();
    const { width: miniMapWidth, height: miniMapHeight } = this.getWrapperElementSize();

    const marginTop = (miniMapHeight - mainPaperHeight * this.MINI_MAP_ZOOM_SCALE) / 2;
    const marginLeft = (miniMapWidth - mainPaperWidth * this.MINI_MAP_ZOOM_SCALE) / 2;

    return { x: (offsetLeft * this.MINI_MAP_ZOOM_SCALE) - marginLeft, y: (offsetTop * this.MINI_MAP_ZOOM_SCALE) - marginTop };
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

  /**
   * This method sets the mini-map paper width and height.
   */
  private setMapPaperDimensions(): void {
    const size = this.getWrapperElementSize();
    this.getMiniMapPaper().setDimensions(size.width, size.height);
  }

}

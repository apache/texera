import { Component, AfterViewInit } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import * as joint from 'jointjs';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';

/**
 * MiniMapComponent is the componenet that contains the mini-map of the workflow editor component.
 *  This component is used for navigating on the workflow editor paper.
 *
 * The mini map component is bound to a JointJS Paper. The mini map's paper uses the same graph/model
 *  as the main workflow (WorkflowEditorComponent's model), making it so that the map will always have
 *  the same operators and links as the main workflow.
 *
 * @author Cynthia Wang
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

  private readonly MINI_MAP_ZOOM_SCALE = 0.15;
  private readonly MINI_MAP_GRID_SIZE = 45;
  private miniMapPaper: joint.dia.Paper | undefined;

  constructor(private workflowActionService: WorkflowActionService) { }

  ngAfterViewInit() {
    this.initializeMapPaper();
    this.handleWindowResize();
    this.handleMinimapTranslate();
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
      el: document.getElementById(this.MINI_MAP_JOINTJS_MAP_ID),
      gridSize: this.MINI_MAP_GRID_SIZE,
      background: { color:  '#F7F6F6' },
      interactive: false
    };
    this.workflowActionService.attachJointPaper(miniMapPaperOptions);

    this.miniMapPaper =  new joint.dia.Paper(miniMapPaperOptions);
    this.miniMapPaper.scale(this.MINI_MAP_ZOOM_SCALE);
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
        this.getMiniMapPaper().translate(
          newOffset.x * this.MINI_MAP_ZOOM_SCALE,
          newOffset.y * this.MINI_MAP_ZOOM_SCALE
        );
      }
    );
  }

  /**
   * When window is resized, reset mini-map's dimensions (introduce
   *  a delay to limit only one event every 30ms)
   */
  private handleWindowResize(): void {
    Observable.fromEvent(window, 'resize').auditTime(30).subscribe(
      () => this.setMapPaperDimensions()
    );
  }

  private getWrapperElementSize(): { width: number, height: number } {
    const e = $('#' + this.MINI_MAP_JOINTJS_MAP_WRAPPER_ID);
    const width = e.width();
    const height = e.height();

    if (width === undefined || height === undefined) {
      throw new Error('fail to get mini-map wrapper element size');
    }
    return { width, height };
  }

  private setMapPaperDimensions(): void {
    const size = this.getWrapperElementSize();
    this.getMiniMapPaper().setDimensions(size.width, size.height);
  }

}

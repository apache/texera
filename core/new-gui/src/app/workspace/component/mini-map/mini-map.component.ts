import { Component, OnInit } from '@angular/core';
import { MiniMapService } from './../../service/workflow-graph/model/mini-map.service';
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
export class MiniMapComponent implements OnInit {
  // the DOM element ID of map. It can be used by jQuery and jointJS to find the DOM element
  // in the HTML template, the div element ID is set using this variable
  public readonly MINI_MAP_JOINTJS_MAP_WRAPPER_ID = 'texera-mini-map-editor-jointjs-wrapper-id';
  public readonly MINI_MAP_JOINTJS_MAP_ID = 'texera-mini-map-editor-jointjs-body-id';

  private readonly MINI_MAP_ZOOM_SCALE = 0.15;
  private readonly MINI_MAP_GRID_SIZE = 45;
  private miniMapPaper: joint.dia.Paper | undefined;

  constructor(private miniMapService: MiniMapService,
    private workflowActionService: WorkflowActionService) { }

  ngOnInit() {
    this.handleMiniMapInitialize();
    this.handleWindowResize();
    this.handlePaperPan();
    this.handlePaperRestoreDefaultOffset();
    this.handlePaperZoom();
  }

  public getMiniMapPaper(): joint.dia.Paper {
    if (this.miniMapPaper === undefined) {
      throw new Error('JointJS Map paper is undefined');
    }
    return this.miniMapPaper;
  }

  /**
   * Gets the WorkflowEditorComponent's paper from MiniMapService,
   *  and calls initializeMapPaper() to set initialize the mapPaper
   */

  /**
   * This function handles the initialization of the mini-map paper. It will
   *  listen to an event that notifies the initialization of the original jointJS paper,
   *  then get the same paper to initialize the mini-map used for navigation on workflow
   *  editor.
   */
  private handleMiniMapInitialize(): void {
    this.miniMapService.getMiniMapInitializeStream().subscribe( paper =>
      this.initializeMapPaper(paper)
    );
  }

  /**
   * This function is used to initialize the minimap paper by passing
   *  the same paper from the workspace editor.
   *
   * @param workflowPaper original JointJS paper from workspace editor
   */
  private initializeMapPaper(workflowPaper: joint.dia.Paper): void {
    if (workflowPaper === undefined) {
      throw new Error('Workflow Graph is undefined');
    }
    this.miniMapPaper =  new joint.dia.Paper({
      el: document.getElementById(this.MINI_MAP_JOINTJS_MAP_ID),
      model: workflowPaper.model, // binds the main workflow's graph/model to the map
      gridSize: this.MINI_MAP_GRID_SIZE,
      drawGrid: true,
      background: {
        color:  '#F7F6F6',
      },
      interactive: false
    });
    this.miniMapPaper.scale(this.MINI_MAP_ZOOM_SCALE);
    this.miniMapPaper.drawGrid({'color' : '#D8656A', 'thickness': 5 });
    this.setMapPaperDimensions();
  }

  /**
   * Handles the panning event from the workflow editor and reflect translation changes
   *  on the mini-map paper.
   */
  private handlePaperPan(): void {
    this.workflowActionService.getJointGraphWrapper().getPanPaperOffsetStream().subscribe(
      newOffset => {
        this.getMiniMapPaper().translate(
          newOffset.x * this.MINI_MAP_ZOOM_SCALE,
          newOffset.y * this.MINI_MAP_ZOOM_SCALE
        );
      }
    );
  }

  /**
   * Handles restore offset default event by translating jointJS paper
   *  back to original position
   */
  private handlePaperRestoreDefaultOffset(): void {
    this.workflowActionService.getJointGraphWrapper().getRestorePaperOffsetStream()
      .subscribe(() => this.getMiniMapPaper().translate(0, 0));
  }

  /**
   * Handles zoom events passed from navigation-component, which can be used to
   *  make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(newRatio => {
      this.getMiniMapPaper().scale(this.MINI_MAP_ZOOM_SCALE * newRatio, this.MINI_MAP_ZOOM_SCALE * newRatio);
    });
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
      throw new Error('fail to get MAP wrapper element size');
    }

    return { width, height };
  }

  private setMapPaperDimensions(): void {
    const size = this.getWrapperElementSize();
    this.getMiniMapPaper().setDimensions(size.width, size.height);
  }

}

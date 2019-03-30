import { Component, OnInit } from '@angular/core';
import { MiniMapService } from './../../service/workflow-graph/model/mini-map.service';
import { Observable } from 'rxjs/Observable';
import * as joint from 'jointjs';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';

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

  private miniMapScaleSize = 0.15;
  private miniMapGridSize = 45;
  private mapPaper: joint.dia.Paper | undefined;

  constructor(private miniMapService: MiniMapService,
    private resultPanelToggleService: ResultPanelToggleService) { }

  ngOnInit() {
    this.handleMiniMapInitialize();
    this.handleWindowResize();
  }

  public getMapPaper(): joint.dia.Paper {
    if (this.mapPaper === undefined) {
      throw new Error('JointJS Map paper is undefined');
    }
    return this.mapPaper;
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
    this.mapPaper =  new joint.dia.Paper({
      el: document.getElementById(this.MINI_MAP_JOINTJS_MAP_ID),
      model: workflowPaper.model, // binds the main workflow's graph/model to the map
      gridSize: this.miniMapGridSize,
      drawGrid: true,
      background: {
        color:  '#F7F6F6',
      },
      interactive: false
    });
    this.mapPaper.scale(this.miniMapScaleSize);
    this.mapPaper.drawGrid({'color' : '#D8656A', 'thickness': 3 });

    this.setMapPaperDimensions();
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
    this.getMapPaper().setDimensions(size.width, size.height);
  }

}

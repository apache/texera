import { Component, OnInit } from '@angular/core';
import { MiniMapService } from './../../service/workflow-graph/model/mini-map.service';
import { Observable } from 'rxjs/Observable';
import * as joint from 'jointjs';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';

/**
 * MiniMapComponent is the componenet for the mini map part of the UI.
 *
 * The mini map component is bound to a JointJS Paper. The mini map's paper uses the same graph/model
 * as the main workflow (WorkflowEditorComponent's model), making it so that the map will always have
 * the same operators and links as the main workflow.
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
    this.initializeGraph();
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
   * and calls initializeMapPaper() to set initialize the mapPaper
   */
  private initializeGraph(): void {
    this.miniMapService.getMiniMapInitializeStream().subscribe( paper => {
      this.initializeMapPaper(paper);
    } );
  }

  /**
   * Function is used by initializeGraph() and it sets the mapPaper's
   * properties.
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
        color:  '#efefef',
      },
      interactive: false
    });
    this.mapPaper.scale(this.miniMapScaleSize);
    this.mapPaper.drawGrid({'color' : '#D8656A', 'thickness': 3 });

    this.setMapPaperDimensions();
  }

  /**
   * When window is resized, reset map's dimensions
   */
  private handleWindowResize(): void {
    Observable.merge(
      Observable.fromEvent(window, 'resize').auditTime(1000),
      this.resultPanelToggleService.getToggleChangeStream().debounceTime(50)
      ).subscribe(
      () => {
        this.setMapPaperDimensions();
      }
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

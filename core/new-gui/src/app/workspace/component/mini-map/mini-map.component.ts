import { Component, OnInit } from '@angular/core';
import { MiniMapService } from './../../service/workflow-graph/model/mini-map.service';
import { Observable } from 'rxjs/Observable';
import * as joint from 'jointjs';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';

@Component({
  selector: 'texera-mini-map',
  templateUrl: './mini-map.component.html',
  styleUrls: ['./mini-map.component.scss']
})
export class MiniMapComponent implements OnInit {
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

  private initializeGraph(): void {
    this.miniMapService.getMiniMapInitializeStream().subscribe( paper => {
      this.initializeMapPaper(paper);
    } );
  }

  private initializeMapPaper(workflowPaper: joint.dia.Paper): void {
    if (workflowPaper === undefined) {
      throw new Error('Workflow Graph is undefined');
    }
    this.mapPaper =  new joint.dia.Paper({
      el: document.getElementById(this.MINI_MAP_JOINTJS_MAP_ID),
      model: workflowPaper.model,
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

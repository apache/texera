import { Component, OnInit } from '@angular/core';
import { MiniMapService } from './../../service/workflow-graph/model/mini-map.service';
import { Observable } from 'rxjs/Observable';
import * as joint from 'jointjs';

@Component({
  selector: 'texera-mini-map',
  templateUrl: './mini-map.component.html',
  styleUrls: ['./mini-map.component.scss']
})
export class MiniMapComponent implements OnInit {
  public readonly MINI_MAP_JOINTJS_MAP_WRAPPER_ID = 'texera-mini-map-editor-jointjs-wrapper-id';
  public readonly MINI_MAP_JOINTJS_MAP_ID = 'texera-mini-map-editor-jointjs-body-id';

  private workflowPaper: joint.dia.Paper | undefined;
  private mapPaper: joint.dia.Paper | undefined;
  constructor(private miniMapService: MiniMapService) { }

  ngOnInit() {
    this.initializeGraph();
    this.handleWindowResize();
  }

  public getWorkflowPaper(): joint.dia.Paper {
    if (this.workflowPaper === undefined) {
      throw new Error('JointJS Workflow paper is undefined');
    }
    return this.workflowPaper;
  }

  public getMapPaper(): joint.dia.Paper {
    if (this.mapPaper === undefined) {
      throw new Error('JointJS Workflow paper is undefined');
    }
    return this.mapPaper;
  }

  private initializeGraph(): void {
    this.miniMapService.getMiniMapInitializeStream().subscribe( paper => {
      this.initializeMapPaper(paper);
    } );
  }

  private initializeMapPaper(workflow_paper: joint.dia.Paper): void {
    if (workflow_paper === undefined) {
      throw new Error('Workflow Graph is undefined');
    }
    this.workflowPaper = workflow_paper;
    this.mapPaper =  new joint.dia.Paper({
      el: document.getElementById(this.MINI_MAP_JOINTJS_MAP_ID),
      model: workflow_paper.model,
      gridSize: 10,
      drawGrid: true,
      background: {
        color: '#f2f2f2',
      },
      interactive: false
    });
    this.mapPaper.scale(0.15);
    this.setMapPaperDimensions();
  }

  private handleWindowResize(): void {
    Observable.fromEvent(window, 'resize').auditTime(1000).subscribe(
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

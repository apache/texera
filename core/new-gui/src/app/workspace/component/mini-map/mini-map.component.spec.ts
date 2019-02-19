import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MiniMapComponent } from './mini-map.component';
import { MiniMapService } from '../../service/workflow-graph/model/mini-map.service';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';

import { marbles } from 'rxjs-marbles';
import { WorkflowEditorComponent } from '../workflow-editor/workflow-editor.component';

import * as joint from 'jointjs';

describe('MiniMapComponent', () => {
  let component: MiniMapComponent;
  let fixture: ComponentFixture<MiniMapComponent>;
  let miniMapService: MiniMapService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MiniMapComponent, WorkflowEditorComponent ],
      providers: [
        MiniMapService,
        ResultPanelToggleService
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    component = fixture.componentInstance;
    miniMapService = TestBed.get(MiniMapService);

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should register a new JointJS paper when mini-map initialize a new map paper', marbles((m) => {

    const mockMapPaper = new joint.dia.Paper({});
    m.hot('-e-').do(event => miniMapService.initializeMapPaper(mockMapPaper)).subscribe();

    miniMapService.getMiniMapInitializeStream().subscribe(
      () => {
        fixture.detectChanges();
        expect(component.getMapPaper()).toBeTruthy();
        expect(component.getMapPaper().model).toEqual(mockMapPaper.model);

        expect(component.getMapPaper().scale().sx).toEqual(0.15);
        expect(component.getMapPaper().scale().sy).toEqual(0.15);
      }
    );

  }));

  it('should should a graph with multiple cells in the mini-map', () => {

    const mockMapPaper = new joint.dia.Paper({});
    miniMapService.initializeMapPaper(mockMapPaper);

    fixture.detectChanges();

    const jointGraph = mockMapPaper.model;

    const operator1 = 'test_multiple_1_op_1';
    const operator2 = 'test_multiple_1_op_2';

    const element1 = new joint.shapes.basic.Rect({
      size: { width: 100, height: 50 },
      position: { x: 100, y: 400 }
    });
    element1.set('id', operator1);

    const element2 = new joint.shapes.basic.Rect({
      size: { width: 100, height: 50 },
      position: { x: 100, y: 400 }
    });
    element2.set('id', operator2);

    const link1 = new joint.dia.Link({
      source: { id: operator1 },
      target: { id: operator2 }
    });

    jointGraph.addCell(element1);
    jointGraph.addCell(element2);
    jointGraph.addCell(link1);

    // check the model is added correctly
    expect(jointGraph.getElements().find(el => el.id === operator1)).toBeTruthy();
    expect(jointGraph.getElements().find(el => el.id === operator2)).toBeTruthy();
    expect(jointGraph.getLinks().find(link => link.id === link1.id)).toBeTruthy();


    // check the view is updated correctly
    expect(component.getMapPaper().findViewByModel(element1.id)).toBeTruthy();
    expect(component.getMapPaper().findViewByModel(element2.id)).toBeTruthy();
    expect(component.getMapPaper().findViewByModel(link1.id)).toBeTruthy();
  });

});

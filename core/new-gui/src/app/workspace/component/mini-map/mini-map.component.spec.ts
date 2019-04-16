import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MiniMapComponent } from './mini-map.component';

import { marbles } from 'rxjs-marbles';
import { WorkflowEditorComponent } from '../workflow-editor/workflow-editor.component';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';

class StubHttpClient {
  constructor() { }

  public post(): Observable<string> { return Observable.of('a'); }
}

import * as joint from 'jointjs';

describe('MiniMapComponent', () => {
  let component: MiniMapComponent;
  let fixture: ComponentFixture<MiniMapComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MiniMapComponent, WorkflowEditorComponent ],
      providers: [
        WorkflowActionService,
        JointUIService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
        {provide: HttpClient, useClass: StubHttpClient}
      ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    component = fixture.componentInstance;

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });


  it('should should a graph with multiple cells in the mini-map', () => {

    const mockMapPaper = new joint.dia.Paper({});

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
    expect(component.getMiniMapPaper().findViewByModel(element1.id)).toBeTruthy();
    expect(component.getMiniMapPaper().findViewByModel(element2.id)).toBeTruthy();
    expect(component.getMiniMapPaper().findViewByModel(link1.id)).toBeTruthy();
  });

});

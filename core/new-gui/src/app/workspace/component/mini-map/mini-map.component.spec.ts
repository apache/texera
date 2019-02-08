import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MiniMapComponent } from './mini-map.component';
import { MiniMapService } from '../../service/workflow-graph/model/mini-map.service';
import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';

import { marbles } from 'rxjs-marbles';
import { WorkflowEditorComponent } from '../workflow-editor/workflow-editor.component';

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

  fit('should check if map is initialized', () => {
    miniMapService.getMiniMapInitializeStream().subscribe(
      () => {
        fixture.detectChanges();
        expect(component.getMapPaper()).toBeFalsy();
      }
    );
  });

});

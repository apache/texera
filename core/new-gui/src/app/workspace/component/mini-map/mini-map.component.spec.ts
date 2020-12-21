import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { MiniMapComponent } from './mini-map.component';

import { WorkflowEditorComponent } from '../workflow-editor/workflow-editor.component';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { Observable } from 'rxjs/Observable';

import { mockScanPredicate, mockPoint,
  mockResultPredicate, mockSentimentPredicate, mockScanResultLink } from '../../service/workflow-graph/model/mock-workflow-data';
import { environment } from './../../../../environments/environment';
import { WorkflowUtilService } from '../../service/workflow-graph/util/workflow-util.service';

describe('MiniMapComponent', () => {
  let component: MiniMapComponent;
  let fixture: ComponentFixture<MiniMapComponent>;
  let workflowActionService: WorkflowActionService;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MiniMapComponent, WorkflowEditorComponent ],
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
      ],
      imports: [ HttpClientTestingModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MiniMapComponent);
    component = fixture.componentInstance;

    workflowActionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a mini-map paper that is compatible to the main workflow paper', () => {
    // add operator operations
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);

    // check if add operator is compatible
    expect(component.getMiniMapPaper().model.getElements().length).toEqual(3);

    // add operator link operation
    workflowActionService.addLink(mockScanResultLink);

    // check if add link is compatible
    expect(component.getMiniMapPaper().model.getLinks().length).toEqual(1);

    // delete operator link operation
    workflowActionService.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    // check if delete link is compatible
    expect(component.getMiniMapPaper().model.getLinks().length).toEqual(0);

    // delete operator operation
    workflowActionService.deleteOperator(mockScanPredicate.operatorID);

    // check if delete operator is compatible
    expect(component.getMiniMapPaper().model.getElements().length).toEqual(2);
  });
});

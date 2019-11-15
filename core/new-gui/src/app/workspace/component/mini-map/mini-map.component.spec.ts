import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MiniMapComponent } from './mini-map.component';

import { WorkflowEditorComponent } from '../workflow-editor/workflow-editor.component';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';

import { mockScanPredicate, mockPoint,
  mockResultPredicate, mockSentimentPredicate, mockScanResultLink } from '../../service/workflow-graph/model/mock-workflow-data';

class StubHttpClient {
  constructor() { }

  public post(): Observable<string> { return Observable.of('a'); }
}

describe('MiniMapComponent', () => {
  let component: MiniMapComponent;
  let fixture: ComponentFixture<MiniMapComponent>;
  let workflowActionService: WorkflowActionService;

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

    workflowActionService = TestBed.get(WorkflowActionService);
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

    // ! each time a operator is added as an element
    // its corresponding tooltip is also added as an element,
    // though invisible most of the time
    // so number of elements should *2

    // check if add operator is compatible
    expect(component.getMiniMapPaper().model.getElements().length).toEqual(6);

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
    expect(component.getMiniMapPaper().model.getElements().length).toEqual(4);
  });

});

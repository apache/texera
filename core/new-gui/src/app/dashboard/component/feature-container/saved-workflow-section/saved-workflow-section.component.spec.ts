import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import {HttpClientTestingModule, HttpTestingController} from '@angular/common/http/testing';
import {ReactiveFormsModule} from "@angular/forms";
import { SavedWorkflowSectionComponent } from './saved-workflow-section.component';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';
import { MatDividerModule } from '@angular/material/divider';
import { MatListModule } from '@angular/material/list';
import { MatCardModule } from '@angular/material/card';
import { MatDialogModule } from '@angular/material/dialog';

import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';


import {Workflow, WorkflowContent} from '../../../../common/type/workflow';
import {jsonCast} from '../../../../common/util/storage';
import {HttpClient} from '@angular/common/http';

describe('SavedWorkflowSectionComponent', () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;
  let mockWorkflowPersistService: WorkflowPersistService;
  // tslint:disable-next-line:prefer-const
  let httpClient: HttpClient;
  // tslint:disable-next-line:prefer-const
  let httpTestingController: HttpTestingController;

  // tslint:disable-next-line:max-line-length
  const testContent = '{"operators":[{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9","operatorType":"Limit","operatorProperties":{"limit":2},"inputPorts":[{"portID":"input-0","displayName":""}],"outputPorts":[{"portID":"output-0","displayName":null}],"showAdvanced":false},{"operatorID":"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914","operatorType":"SimpleSink","operatorProperties":{},"inputPorts":[{"portID":"input-0","displayName":""}],"outputPorts":[],"showAdvanced":false},{"operatorID":"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50","operatorType":"MySQLSource","operatorProperties":{"port":"default","search":false,"progressive":false,"min":"auto","max":"auto","interval":1000000000,"host":"localhost"},"inputPorts":[],"outputPorts":[{"portID":"output-0","displayName":""}],"showAdvanced":false}],"operatorPositions":{"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9":{"x":200,"y":212},"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914":{"x":392,"y":218},"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50":{"x":36,"y":214}},"links":[{"linkID":"link-ea977a06-3ef5-4c80-b31a-4013cfb8321d","source":{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9","portID":"output-0"},"target":{"operatorID":"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914","portID":"input-0"}},{"linkID":"link-c94e24a6-2c77-40cf-ba22-1a7ffba64b7d","source":{"operatorID":"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50","portID":"output-0"},"target":{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9","portID":"input-0"}}],"groups":[],"breakpoints":{}}';
  const TestCase: Workflow[] = [
    {
      wid: 1,
      name: 'project 1',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 1,
      lastModifiedTime: 2,
    },
    {
      wid: 2,
      name: 'project 2',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 4,
    },
    {
      wid: 3,
      name: 'project 3',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 3,
    },
    {
      wid: 4,
      name: 'project 4',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 4,
      lastModifiedTime: 6,
    },
    {
      wid: 5,
      name: 'project 5',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 8,
    }
  ];

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [SavedWorkflowSectionComponent],
      providers: [
        WorkflowPersistService,
        NgbActiveModal,
        HttpClient
      ],
      imports: [MatDividerModule,
        MatListModule,
        MatCardModule,
        MatDialogModule,
        NgbModule,
        FormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        ReactiveFormsModule]
    }).compileComponents();
  }));

  beforeEach(() => {
    httpClient = TestBed.get(HttpClient);
    httpTestingController = TestBed.get(HttpTestingController);
    fixture = TestBed.createComponent(SavedWorkflowSectionComponent);
    mockWorkflowPersistService = TestBed.inject(WorkflowPersistService);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(mockWorkflowPersistService).toBeTruthy();
  });

  it('alphaSortTest increaseOrder', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.ascSort();
    const SortedCase = component.workflows.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 1', 'project 2', 'project 3', 'project 4', 'project 5']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.dscSort();
    const SortedCase = component.workflows.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 5', 'project 4', 'project 3', 'project 2', 'project 1']);
  });

  it('createDateSortTest', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.dateSort();
    const SortedCase = component.workflows.map(item => item.creationTime);
    expect(SortedCase)
      .toEqual([1, 3, 3, 3, 4]);
  });

  it('lastEditSortTest', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.lastSort();
    const SortedCase = component.workflows.map(item => item.lastModifiedTime);
    expect(SortedCase)
      .toEqual([2, 3, 4, 6, 8]);
  });


  // it('should call createWorkflow when click on duplicate button', () => {
  //   // tslint:disable-next-line:prefer-const
  //   let buttonState = fixture.debugElement.query(By.css('button.mat-focus-indicator.duplicate-workflow-btn.mat-button.mat-button-base'));
  //   expect(buttonState).toBeTruthy();
  //   spyOn(mockWorkflowPersistService, 'createWorkflow');
  //   buttonState.triggerEventHandler('click', null);
  //   expect(mockWorkflowPersistService.createWorkflow).toHaveBeenCalledTimes(1);
  // });


  /*
  * more tests of testing return value from pop-up components(windows)
  * should be removed to here
  */

});

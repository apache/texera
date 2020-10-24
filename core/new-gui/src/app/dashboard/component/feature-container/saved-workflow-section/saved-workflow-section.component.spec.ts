import {async, ComponentFixture, TestBed} from '@angular/core/testing';

import {SavedWorkflowSectionComponent} from './saved-workflow-section.component';

import {SavedWorkflowService} from '../../../service/saved-project/saved-workflow.service';
import {StubSavedProjectService} from '../../../service/saved-project/stub-saved-project.service';
import {MatDividerModule} from '@angular/material/divider';
import {MatListModule} from '@angular/material/list';
import {MatCardModule} from '@angular/material/card';
import {MatDialogModule} from '@angular/material/dialog';

import {NgbActiveModal, NgbModule} from '@ng-bootstrap/ng-bootstrap';
import {FormsModule} from '@angular/forms';

import {SavedWorkflow} from '../../../type/saved-workflow';
import {HttpClientModule} from '@angular/common/http';

describe('SavedProjectSectionComponent', () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;

  const TestCase: SavedWorkflow[] = [
    {
      id: '1',
      name: 'project 3',
      creationTime: '2017-10-25T12:34:50Z',
      lastModifiedTime: '2018-01-17T06:26:50Z',
    },
    {
      id: '2',
      name: 'project 2',
      creationTime: '2017-10-30T01:02:50Z',
      lastModifiedTime: '2018-01-14T22:56:50Z',
    },
    {
      id: '3',
      name: 'project 4',
      creationTime: '2018-01-01T01:01:01Z',
      lastModifiedTime: '2018-01-22T17:26:50Z',
    },
    {
      id: '4',
      name: 'project 1',
      creationTime: '2017-10-25T12:34:50Z',
      lastModifiedTime: '2018-01-17T06:26:50Z',
    },
    {
      id: '5',
      name: 'project 5',
      creationTime: '2017-10-30T01:02:50Z',
      lastModifiedTime: '2018-01-14T22:56:50Z',
    }
  ];

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [SavedWorkflowSectionComponent],
      providers: [
        {provide: SavedWorkflowService, useClass: StubSavedProjectService},
        NgbActiveModal
      ],
      imports: [MatDividerModule,
        MatListModule,
        MatCardModule,
        MatDialogModule,
        NgbModule,
        FormsModule,
        HttpClientModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SavedWorkflowSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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
      .toEqual(['2018-01-01T01:01:01Z', '2017-10-30T01:02:50Z', '2017-10-30T01:02:50Z', '2017-10-25T12:34:50Z', '2017-10-25T12:34:50Z']);
  });

  it('lastEditSortTest', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.lastSort();
    const SortedCase = component.workflows.map(item => item.lastModifiedTime);
    expect(SortedCase)
      .toEqual(['2018-01-22T17:26:50Z', '2018-01-17T06:26:50Z', '2018-01-17T06:26:50Z', '2018-01-14T22:56:50Z', '2018-01-14T22:56:50Z']);
  });

/*
* more tests of testing return value from pop-up components(windows)
* should be removed to here
*/

});

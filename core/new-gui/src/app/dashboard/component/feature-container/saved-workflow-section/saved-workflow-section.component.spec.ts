import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { SavedWorkflowSectionComponent } from './saved-workflow-section.component';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';
import { MatDividerModule } from '@angular/material/divider';
import { MatListModule } from '@angular/material/list';
import { MatCardModule } from '@angular/material/card';
import { MatDialogModule } from '@angular/material/dialog';

import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';


import { Workflow } from '../../../../common/type/workflow';

describe('SavedWorkflowSectionComponent', () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;

  // const TestCase: Workflow[] = [
  //   {
  //     wid: 1,
  //     name: 'project 1',
  //     content: '{}',
  //     creationTime: 1,
  //     lastModifiedTime: 2,
  //   },
  //   {
  //     wid: 2,
  //     name: 'project 2',
  //     content: '{}',
  //     creationTime: 3,
  //     lastModifiedTime: 4,
  //   },
  //   {
  //     wid: 3,
  //     name: 'project 3',
  //     content: '{}',
  //     creationTime: 3,
  //     lastModifiedTime: 3,
  //   },
  //   {
  //     wid: 4,
  //     name: 'project 4',
  //     content: '{}',
  //     creationTime: 4,
  //     lastModifiedTime: 6,
  //   },
  //   {
  //     wid: 5,
  //     name: 'project 5',
  //     content: '{}',
  //     creationTime: 3,
  //     lastModifiedTime: 8,
  //   }
  // ];

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [SavedWorkflowSectionComponent],
      providers: [
        WorkflowPersistService,
        NgbActiveModal
      ],
      imports: [MatDividerModule,
        MatListModule,
        MatCardModule,
        MatDialogModule,
        NgbModule,
        FormsModule,
        RouterTestingModule,
        HttpClientTestingModule]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SavedWorkflowSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // it('alphaSortTest increaseOrder', () => {
  //   component.workflows = [];
  //   component.workflows = component.workflows.concat(TestCase);
  //   component.ascSort();
  //   const SortedCase = component.workflows.map(item => item.name);
  //   expect(SortedCase)
  //     .toEqual(['project 1', 'project 2', 'project 3', 'project 4', 'project 5']);
  // });

  // it('alphaSortTest decreaseOrder', () => {
  //   component.workflows = [];
  //   component.workflows = component.workflows.concat(TestCase);
  //   component.dscSort();
  //   const SortedCase = component.workflows.map(item => item.name);
  //   expect(SortedCase)
  //     .toEqual(['project 5', 'project 4', 'project 3', 'project 2', 'project 1']);
  // });

  // it('createDateSortTest', () => {
  //   component.workflows = [];
  //   component.workflows = component.workflows.concat(TestCase);
  //   component.dateSort();
  //   const SortedCase = component.workflows.map(item => item.creationTime);
  //   expect(SortedCase)
  //     .toEqual([1, 3, 3, 3, 4]);
  // });

  // it('lastEditSortTest', () => {
  //   component.workflows = [];
  //   component.workflows = component.workflows.concat(TestCase);
  //   component.lastSort();
  //   const SortedCase = component.workflows.map(item => item.lastModifiedTime);
  //   expect(SortedCase)
  //     .toEqual([2, 3, 4, 6, 8]);
  // });

  /*
  * more tests of testing return value from pop-up components(windows)
  * should be removed to here
  */

});

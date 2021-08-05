import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MatDialogModule } from '@angular/material/dialog';

import { NgbActiveModal, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';

import { NgbdModalDeleteWorkflowComponent } from './ngbd-modal-delete-workflow.component';
import { DashboardWorkflowEntry } from '../../../../../common/type/dashboard-workflow-entry';
import { WorkflowContent } from '../../../../../common/type/workflow';
import { jsonCast } from '../../../../../common/util/storage';

describe('NgbdModalDeleteProjectComponent', () => {
  let component: NgbdModalDeleteWorkflowComponent;
  let fixture: ComponentFixture<NgbdModalDeleteWorkflowComponent>;

  let deleteComponent: NgbdModalDeleteWorkflowComponent;
  let deleteFixture: ComponentFixture<NgbdModalDeleteWorkflowComponent>;

  const targetWorkflowEntry: DashboardWorkflowEntry = {
    workflow: {
      name: 'workflow 1',
      wid: 4,
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 1,
      lastModifiedTime: 2,
    },
    isOwner: true,
    ownerName: 'Texera',
    accessLevel: 'Write'
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [NgbdModalDeleteWorkflowComponent],
      providers: [
        NgbActiveModal
      ],
      imports: [
        MatDialogModule,
        NgbModule,
        FormsModule,
        HttpClientModule]
    })
      .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalDeleteWorkflowComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('deleteProjectComponent deleteSavedProject return a result of true', () => {
    deleteFixture = TestBed.createComponent(NgbdModalDeleteWorkflowComponent);
    deleteComponent = deleteFixture.componentInstance;
    deleteComponent.dashboardWorkflowEntry = targetWorkflowEntry;

    spyOn(deleteComponent.activeModal, 'close');
    deleteComponent.deleteSavedWorkflowEntry();
    expect(deleteComponent.activeModal.close).toHaveBeenCalledWith(true);
  });
});

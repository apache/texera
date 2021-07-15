import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { SavedWorkflowSectionComponent } from './saved-workflow-section.component';
import { WorkflowPersistService } from '../../../../common/service/user/workflow-persist/workflow-persist.service';
import { MatDividerModule } from '@angular/material/divider';
import { MatListModule } from '@angular/material/list';
import { MatCardModule } from '@angular/material/card';
import { MatDialogModule } from '@angular/material/dialog';
import { NgbActiveModal, NgbModal, NgbModalRef, NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { NgbdModalShareAccessComponent } from './ngbd-modal-share-access/ngbd-modal-share-access.component';
import { Workflow, WorkflowContent } from '../../../../common/type/workflow';
import { jsonCast } from '../../../../common/util/storage';
import { HttpClient } from '@angular/common/http';
import { WorkflowGrantAccessService } from '../../../../common/service/user/workflow-access-control/workflow-grant-access.service';

describe('SavedWorkflowSectionComponent', () => {
  let component: SavedWorkflowSectionComponent;
  let fixture: ComponentFixture<SavedWorkflowSectionComponent>;
  let modalService: NgbModal;
  let modalRef: NgbModalRef;
  let mockWorkflowPersistService: WorkflowPersistService;
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;
  const testContent = ' {"operators":[],"operatorPositions":{},"links":[],"groups":[],"breakpoints":{}}';
  const TestWorkflow: Workflow = {
    wid: 1,
    name: 'workflow',
    content: jsonCast<WorkflowContent>(testContent),
    creationTime: 1,
    lastModifiedTime: 2,
  };
  const TestCase: Workflow[] = [
    {
      wid: 1,
      name: 'workflow 1',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 1,
      lastModifiedTime: 2,
    },
    {
      wid: 2,
      name: 'workflow 2',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 4,
    },
    {
      wid: 3,
      name: 'workflow 3',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 3,
    },
    {
      wid: 4,
      name: 'workflow 4',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 4,
      lastModifiedTime: 6,
    },
    {
      wid: 5,
      name: 'workflow 5',
      content: jsonCast<WorkflowContent>('{}'),
      creationTime: 3,
      lastModifiedTime: 8,
    }
  ];

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [SavedWorkflowSectionComponent,
        NgbdModalShareAccessComponent],
      providers: [
        WorkflowPersistService,
        NgbActiveModal,
        HttpClient,
        NgbActiveModal,
        WorkflowGrantAccessService
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
    modalService = TestBed.get(NgbModal);
    modalRef = modalService.open(NgbdModalShareAccessComponent);
    spyOn(modalService, 'open').and.returnValue(modalRef);
    spyOn(console, 'log').and.callThrough();
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
      .toEqual(['workflow 1', 'workflow 2', 'workflow 3', 'workflow 4', 'workflow 5']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.dscSort();
    const SortedCase = component.workflows.map(item => item.name);
    expect(SortedCase)
      .toEqual(['workflow 5', 'workflow 4', 'workflow 3', 'workflow 2', 'workflow 1']);
  });

  it('Modal Opened', () => {
    component.onClickOpenShareAccess(TestWorkflow);
    expect(modalService.open).toHaveBeenCalled();
  });

  it('Modal Opened, then Closed', () => {
    component.onClickOpenShareAccess(TestWorkflow);
    expect(modalService.open).toHaveBeenCalled();
    fixture.detectChanges();
    fixture.whenStable().then(() => {
      modalRef.dismiss();
    });
  });
  it('alphaSortTest increaseOrder', () => {
    component.workflows = [];
    component.workflows = component.workflows.concat(TestCase);
    component.ascSort();
    const SortedCase = component.workflows.map(item => item.name);
    expect(SortedCase)
      .toEqual(['workflow 1', 'workflow 2', 'workflow 3', 'workflow 4', 'workflow 5']);
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


  /*
  * more tests of testing return value from pop-up components(windows)
  * should be removed to here
  */

});

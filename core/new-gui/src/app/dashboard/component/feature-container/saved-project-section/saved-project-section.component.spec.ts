import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SavedProjectSectionComponent} from './saved-project-section.component';
import { NgbdModalAddProjectComponent} from './ngbd-modal-add-project/ngbd-modal-add-project.component';
import { NgbdModalDeleteProjectComponent } from './ngbd-modal-delete-project/ngbd-modal-delete-project.component';

import { SavedProjectService } from '../../../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../../../service/saved-project/stub-saved-project.service';
import {MatDividerModule} from '@angular/material/divider';
import {MatListModule} from '@angular/material/list';
import {MatCardModule} from '@angular/material/card';
import {MatDialogModule} from '@angular/material/dialog';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { SavedProject } from '../../../type/saved-project';

import * as c from './saved-project-section.component';
import { HttpClientModule } from '@angular/common/http';

describe('SavedProjectSectionComponent', () => {
  let component: SavedProjectSectionComponent;
  let fixture: ComponentFixture<SavedProjectSectionComponent>;

  let addcomponent: NgbdModalAddProjectComponent;
  let addfixture: ComponentFixture<NgbdModalAddProjectComponent>;

  let deletecomponent: NgbdModalDeleteProjectComponent;
  let deletefixture: ComponentFixture<NgbdModalDeleteProjectComponent>;

  const TestCase: SavedProject[] = [
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
      declarations: [ SavedProjectSectionComponent,
        NgbdModalAddProjectComponent,
        NgbdModalDeleteProjectComponent ],
      providers: [
        { provide: SavedProjectService, useClass: StubSavedProjectService },
        NgbActiveModal
      ],
      imports: [MatDividerModule,
        MatListModule,
        MatCardModule,
        MatDialogModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SavedProjectSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('alphaSortTest increaseOrder', () => {
    component.projects = [];
    component.projects = component.projects.concat(TestCase);
    component.ascSort();
    const SortedCase = component.projects.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 1', 'project 2', 'project 3', 'project 4', 'project 5']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.projects = [];
    component.projects = component.projects.concat(TestCase);
    component.dscSort();
    const SortedCase = component.projects.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 5', 'project 4', 'project 3', 'project 2', 'project 1']);
  });

  it('createDateSortTest', () => {
    component.projects = [];
    component.projects = component.projects.concat(TestCase);
    component.dateSort();
    const SortedCase = component.projects.map(item => item.creationTime);
    expect(SortedCase)
      .toEqual(['2018-01-01T01:01:01Z', '2017-10-30T01:02:50Z', '2017-10-30T01:02:50Z', '2017-10-25T12:34:50Z', '2017-10-25T12:34:50Z']);
  });

  it('lastEditSortTest', () => {
    component.projects = [];
    component.projects = component.projects.concat(TestCase);
    component.lastSort();
    const SortedCase = component.projects.map(item => item.lastModifiedTime);
    expect(SortedCase)
      .toEqual(['2018-01-22T17:26:50Z', '2018-01-17T06:26:50Z', '2018-01-17T06:26:50Z', '2018-01-14T22:56:50Z', '2018-01-14T22:56:50Z']);
  });

  it('addProjectComponent addProject should add new project', () => {
    addfixture = TestBed.createComponent(NgbdModalAddProjectComponent);
    addcomponent = addfixture.componentInstance;
    addfixture.detectChanges();

    let getResult: String = '';
    addcomponent.name = 'test';
    addcomponent.newProject.subscribe((out: any) => getResult = out);
    addcomponent.addProject();

    expect(getResult).toEqual('test');
  });

  it('deleteProjectComponent deleteSavedProject should delete project in list', () => {
    deletefixture = TestBed.createComponent(NgbdModalDeleteProjectComponent);
    deletecomponent = deletefixture.componentInstance;

    let getBool: Boolean = false;
    deletecomponent.project = {
      id: '4',
      name: 'project 1',
      creationTime: '2017-10-25T12:34:50Z',
      lastModifiedTime: '2018-01-17T06:26:50Z',
    };
    deletecomponent.deleteProject.subscribe((outb: any) => getBool = outb);
    deletecomponent.deleteSavedProject();

    expect(getBool).toEqual(true);
  });

});

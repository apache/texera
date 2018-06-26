import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SavedProjectSectionComponent } from './saved-project-section.component';
import { SavedProjectService } from '../../../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../../../service/saved-project/stub-saved-project.service';

import { SavedProject } from '../../../type/saved-project';

import * as c from './saved-project-section.component';

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

describe('SavedProjectSectionComponent', () => {
  let component: SavedProjectSectionComponent;
  let fixture: ComponentFixture<SavedProjectSectionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SavedProjectSectionComponent ],
      providers: [
        { provide: SavedProjectService, useClass: StubSavedProjectService }
      ]
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
    component.projects = TestCase;
    console.log(component.projects);
    component.ascSort();
    const SortedCase = component.projects.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 1', 'project 2', 'project 3', 'project 4', 'project 5']);
  });

  it('alphaSortTest decreaseOrder', () => {
    component.projects = TestCase;
    component.dscSort();
    const SortedCase = component.projects.map(item => item.name);
    expect(SortedCase)
      .toEqual(['project 5', 'project 4', 'project 3', 'project 2', 'project 1']);
  });

  it('createDateSortTest', () => {
    component.projects = TestCase;
    component.dateSort();
    const SortedCase = component.projects.map(item => item.creationTime);
    expect(SortedCase)
      .toEqual(['2018-01-01T01:01:01Z', '2017-10-30T01:02:50Z', '2017-10-30T01:02:50Z', '2017-10-25T12:34:50Z', '2017-10-25T12:34:50Z']);
  });

  it('lastEditSortTest', () => {
    component.projects = TestCase;
    component.lastSort();
    const SortedCase = component.projects.map(item => item.lastModifiedTime);
    expect(SortedCase)
      .toEqual(['2018-01-22T17:26:50Z', '2018-01-17T06:26:50Z', '2018-01-17T06:26:50Z', '2018-01-14T22:56:50Z', '2018-01-14T22:56:50Z']);
  });

});

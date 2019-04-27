import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import {MatDialogModule} from '@angular/material/dialog';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';

import { NgbdModalDeleteProjectComponent } from './ngbd-modal-delete-project.component';

describe('NgbdModalDeleteProjectComponent', () => {
  let component: NgbdModalDeleteProjectComponent;
  let fixture: ComponentFixture<NgbdModalDeleteProjectComponent>;

  let deletecomponent: NgbdModalDeleteProjectComponent;
  let deletefixture: ComponentFixture<NgbdModalDeleteProjectComponent>;

  const sampleProject = {
    id: '4',
    name: 'project 1',
    creationTime: '2017-10-25T12:34:50Z',
    lastModifiedTime: '2018-01-17T06:26:50Z',
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalDeleteProjectComponent ],
      providers: [
        NgbActiveModal
      ],
      imports: [
        MatDialogModule,
        NgbModule.forRoot(),
        FormsModule,
        HttpClientModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalDeleteProjectComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('deleteProjectComponent deleteSavedProject should delete project in list', () => {
    deletefixture = TestBed.createComponent(NgbdModalDeleteProjectComponent);
    deletecomponent = deletefixture.componentInstance;

    let getBool: Boolean = false;
    deletecomponent.project = sampleProject;
    deletecomponent.deleteProject.subscribe((outb: any) => getBool = outb);
    deletecomponent.deleteSavedProject();

    expect(getBool).toEqual(false);
  });
});

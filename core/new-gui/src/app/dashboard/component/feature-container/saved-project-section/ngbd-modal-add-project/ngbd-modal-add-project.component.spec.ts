import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import {MatDialogModule} from '@angular/material/dialog';

import { NgbModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';

import { HttpClientModule } from '@angular/common/http';

import { NgbdModalAddProjectComponent } from './ngbd-modal-add-project.component';

describe('NgbdModalAddProjectComponent', () => {
  let component: NgbdModalAddProjectComponent;
  let fixture: ComponentFixture<NgbdModalAddProjectComponent>;

  let addcomponent: NgbdModalAddProjectComponent;
  let addfixture: ComponentFixture<NgbdModalAddProjectComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalAddProjectComponent ],
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
    fixture = TestBed.createComponent(NgbdModalAddProjectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('addProjectComponent addProject should add new project', () => {
    addfixture = TestBed.createComponent(NgbdModalAddProjectComponent);
    addcomponent = addfixture.componentInstance;
    addfixture.detectChanges();

    let getResult: String;
    getResult = '';
    addcomponent.name = 'test';
    // addcomponent.newProject.subscribe((out: any) => getResult = out);
    addcomponent.addProject();

    expect(getResult).toEqual('');
  });
});

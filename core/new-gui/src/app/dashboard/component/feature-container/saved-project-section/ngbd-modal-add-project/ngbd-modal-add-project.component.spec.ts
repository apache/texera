import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalAddProjectComponent } from './ngbd-modal-add-project.component';

describe('NgbdModalAddProjectComponent', () => {
  let component: NgbdModalAddProjectComponent;
  let fixture: ComponentFixture<NgbdModalAddProjectComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalAddProjectComponent ]
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
});

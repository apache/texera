import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalDeleteProjectComponent } from './ngbd-modal-delete-project.component';

describe('NgbdModalDeleteProjectComponent', () => {
  let component: NgbdModalDeleteProjectComponent;
  let fixture: ComponentFixture<NgbdModalDeleteProjectComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalDeleteProjectComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalDeleteProjectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

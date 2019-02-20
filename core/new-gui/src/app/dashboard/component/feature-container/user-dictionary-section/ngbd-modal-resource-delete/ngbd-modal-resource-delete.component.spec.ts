import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalResourceDeleteComponent } from './ngbd-modal-resource-delete.component';

describe('NgbdModalResourceDeleteComponent', () => {
  let component: NgbdModalResourceDeleteComponent;
  let fixture: ComponentFixture<NgbdModalResourceDeleteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceDeleteComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceDeleteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

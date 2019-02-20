import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalResourceViewComponent } from './ngbd-modal-resource-view.component';

describe('NgbdModalResourceViewComponent', () => {
  let component: NgbdModalResourceViewComponent;
  let fixture: ComponentFixture<NgbdModalResourceViewComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalResourceViewComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalResourceViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

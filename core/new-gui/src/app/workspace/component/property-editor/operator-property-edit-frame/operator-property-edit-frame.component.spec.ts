import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OperatorPropertyEditFrameComponent } from './operator-property-edit-frame.component';

describe('FormlyFormFrameComponent', () => {
  let component: OperatorPropertyEditFrameComponent;
  let fixture: ComponentFixture<OperatorPropertyEditFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OperatorPropertyEditFrameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OperatorPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

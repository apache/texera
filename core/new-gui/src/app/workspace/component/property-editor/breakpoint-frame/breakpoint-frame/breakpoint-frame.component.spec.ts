import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BreakpointFrameComponent } from './breakpoint-frame.component';

describe('BreakpointFrameComponent', () => {
  let component: BreakpointFrameComponent;
  let fixture: ComponentFixture<BreakpointFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BreakpointFrameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BreakpointFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

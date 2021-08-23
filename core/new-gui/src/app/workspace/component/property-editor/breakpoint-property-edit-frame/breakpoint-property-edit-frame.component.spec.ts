import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BreakpointPropertyEditFrameComponent } from './breakpoint-property-edit-frame.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('BreakpointFrameComponent', () => {
  let component: BreakpointPropertyEditFrameComponent;
  let fixture: ComponentFixture<BreakpointPropertyEditFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BreakpointPropertyEditFrameComponent ],
      imports: [HttpClientTestingModule]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BreakpointPropertyEditFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

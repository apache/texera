import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ConsoleFrameComponent } from './console-frame.component';

describe('ConsoleFrameComponent', () => {
  let component: ConsoleFrameComponent;
  let fixture: ComponentFixture<ConsoleFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ConsoleFrameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ConsoleFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultTableFrameComponent } from './result-table-frame.component';

describe('ResultTableFrameComponent', () => {
  let component: ResultTableFrameComponent;
  let fixture: ComponentFixture<ResultTableFrameComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResultTableFrameComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ResultPanelToggleComponent } from './result-panel-toggle.component';

describe('ResultPanelToggleComponent', () => {
  let component: ResultPanelToggleComponent;
  let fixture: ComponentFixture<ResultPanelToggleComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ResultPanelToggleComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ResultPanelToggleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

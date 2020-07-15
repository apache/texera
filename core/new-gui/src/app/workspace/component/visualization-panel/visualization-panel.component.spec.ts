import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizationPanelComponent } from './visualization-panel.component';

describe('VisualizationPanelComponent', () => {
  let component: VisualizationPanelComponent;
  let fixture: ComponentFixture<VisualizationPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ VisualizationPanelComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

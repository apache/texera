import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizationPanelContentComponent } from './visualization-panel-content.component';

describe('VisualizationPanelContentComponent', () => {
  let component: VisualizationPanelContentComponent;
  let fixture: ComponentFixture<VisualizationPanelContentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ VisualizationPanelContentComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

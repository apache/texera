import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { VisualizationPanelContentComponent } from './visualization-panel-content.component';
import { MatDialogModule } from '@angular/material/dialog';
import { ChartType } from '../../types/visualization.interface';


describe('VisualizationPanelContentComponent', () => {
  let component: VisualizationPanelContentComponent;
  let fixture: ComponentFixture<VisualizationPanelContentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [MatDialogModule],
      declarations: [ VisualizationPanelContentComponent ],
      providers: []
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(VisualizationPanelContentComponent);
    component = fixture.componentInstance;
    component.data = [{'id': 1, 'data': 2}];
    component.chartType = ChartType.PIE;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should draw the figure', () => {
    spyOn(component, 'generateChart');
    component.ngOnChanges();
    fixture.detectChanges();
    expect(component.generateChart).toHaveBeenCalled();
  });

  it('should draw the wordcloud', () => {
    const testComponent = fixture.componentInstance;
    component.data = [{'word': 'foo', 'count': 120}, {'word': 'bar', 'count': 100}];
    component.chartType = ChartType.WORD_CLOUD;

    spyOn(testComponent, 'generateWordCloud');
    component.ngOnChanges();
    fixture.detectChanges();

    expect(testComponent.generateWordCloud).toHaveBeenCalled();
  });
});

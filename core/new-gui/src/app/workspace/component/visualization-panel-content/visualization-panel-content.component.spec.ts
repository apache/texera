import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import * as c3 from 'c3';
import { VisualizationPanelContentComponent } from './visualization-panel-content.component';
import { MatDialogModule,  MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ChartType, WordCloudTuple, DialogData } from '../../types/visualization.interface';
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
    component.data = {table: [['id', 'data'], [1, 2]], chartType: ChartType.PIE};
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should draw the figure', () => {
    spyOn(component, 'onClickGenerateChart');
    component.ngAfterViewInit();
    expect(component.onClickGenerateChart).toHaveBeenCalled();
  });

  it('should draw the wordcloud', () => {
    const testComponent = new VisualizationPanelContentComponent();
    testComponent.data = { table: [['word', 'count'], ['foo', 120], ['bar', 100]],
      chartType: ChartType.WORD_CLOUD };
    spyOn(testComponent, 'onClickGenerateWordCloud');
    testComponent.ngAfterViewInit();
    expect(testComponent.onClickGenerateWordCloud).toHaveBeenCalled();
  });
});

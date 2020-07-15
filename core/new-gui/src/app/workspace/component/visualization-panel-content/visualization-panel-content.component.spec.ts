import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import * as c3 from 'c3';
import { VisualizationPanelContentComponent } from './visualization-panel-content.component';
import { MatDialogModule,  MAT_DIALOG_DATA } from '@angular/material/dialog';

describe('VisualizationPanelContentComponent', () => {
  let component: VisualizationPanelContentComponent;
  let fixture: ComponentFixture<VisualizationPanelContentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [MatDialogModule],
      declarations: [ VisualizationPanelContentComponent ],
      providers: [
        {
          provide: MAT_DIALOG_DATA, useValue: {table: [['id', 'data'], [1, 2]], chartType: 'pie'}
        }]
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

  it('should draw the figure', () => {
    spyOn(c3, 'generate');
    component.onClickGenerateChart();
    expect(c3.generate).toHaveBeenCalled();
  });
});

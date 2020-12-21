import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { VisualizationPanelComponent } from './visualization-panel.component';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { ChartType } from '../../types/visualization.interface';

describe('VisualizationPanelComponent', () => {
  let component: VisualizationPanelComponent;
  let fixture: ComponentFixture<VisualizationPanelComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        NzModalModule,
        NzButtonModule
      ],
      declarations: [ VisualizationPanelComponent ],
      providers: []
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

  it('should have button', () => {
    const bannerElement: HTMLElement = fixture.nativeElement;
    const button = bannerElement.querySelector('button');
    expect(button).toBeTruthy();
  });

  it('should open dialog', () => {
    const createSpy = spyOn((component as any).modal, 'create');
    component.chartType = ChartType.PIE;
    component.onClickVisualize();
    expect(createSpy).toHaveBeenCalled();
  });
});

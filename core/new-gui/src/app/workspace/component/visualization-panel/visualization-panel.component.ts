import { Component, Input, } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';
import { ChartType } from '../../types/visualization.interface';
import { assertType } from '../../../common/util/assert';

/**
 * VisualizationPanelComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * It receives the data for visualization and chart type.
 * When user click on button, this component will open VisualzationPanelContentComponent and display figure.
 * User could click close at the button of VisualzationPanelContentComponent to exit the visualization panel.
 * @author Mingji Han
 */
@Component({
  selector: 'texera-visualization-panel',
  templateUrl: './visualization-panel.component.html',
  styleUrls: ['./visualization-panel.component.scss']
})
export class VisualizationPanelComponent {
  @Input() data: Object[];
  @Input() chartType: ChartType | null;


  constructor(private modal: NzModalService) {
    this.data = [];
    this.chartType = null;
  }

  onClickVisualize(): void {
    assertType<ChartType>(this.chartType);
    const dialogRef = this.modal.create({
      nzTitle: 'Visualization',
      nzWidth: 1100,
      nzContent: VisualizationPanelContentComponent,
      nzComponentParams: {
        data: {
          table: this.data,
          chartType: this.chartType,
        }
      }
    });

  }


}

import { Component, Input, } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';

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
  @Input() chartType: string;


  constructor(public dialog: MatDialog) {
    this.data = [];
    this.chartType = '';

  }

  onClickVisualize(): void {
    const dialogRef = this.dialog.open(VisualizationPanelContentComponent, {
      data: {
        table: this.data,
        chartType: this.chartType,
      }
    });

  }


}

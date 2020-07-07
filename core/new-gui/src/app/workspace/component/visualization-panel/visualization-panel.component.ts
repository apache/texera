import { Component, Input,  } from '@angular/core';
import {MatDialog} from '@angular/material/dialog';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component'
import { TableColumn } from '../../types/result-table.interface';

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
    this.chartType = "";

  }
  

  onClickVisualize(): void {
    const dialogRef = this.dialog.open(VisualizationPanelContentComponent, {
      data: {
        table: this.data,
        chartType: this.chartType,
      
      },
      height: '2000px',
      width: '2000px',
    });

  }


  
}

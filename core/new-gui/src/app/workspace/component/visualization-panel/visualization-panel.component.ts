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
  @Input() nameColumn: string;
  @Input() dataColumn: string;

  constructor(public dialog: MatDialog) {
    this.data = [];
    this.chartType = "";
    this.nameColumn = "";
    this.dataColumn = "";
   
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

    dialogRef.afterClosed().subscribe(result => {
      console.log(`Dialog result: ${result}`);
    });
  }


  
}

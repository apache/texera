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

  constructor(public dialog: MatDialog) {
    this.data = []

  }

  onClickVisualize(): void {
    const dialogRef = this.dialog.open(VisualizationPanelContentComponent, {
      data: {
        table: this.data,
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      console.log(`Dialog result: ${result}`);
    });
  }
  
}

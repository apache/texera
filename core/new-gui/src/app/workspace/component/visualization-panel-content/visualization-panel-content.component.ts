import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA} from '@angular/material/dialog';
import { TableColumn } from '../../types/result-table.interface';


interface ValueObject {
  table: object[]
}
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit {
  table: object[];
  columns: string[] | undefined;
  constructor(@Inject(MAT_DIALOG_DATA) public data: ValueObject) {
    this.table = data.table
  }

  ngOnInit() {
    this.columns =  Object.keys(this.table[0]).filter(x => x !== '_id')
    
  }

 
}

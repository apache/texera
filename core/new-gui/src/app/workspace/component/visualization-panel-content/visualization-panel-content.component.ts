import { Component, Inject, OnInit } from '@angular/core';
import { MAT_DIALOG_DATA} from '@angular/material/dialog';
import * as c3 from 'c3';
import {PrimitiveArray} from 'c3';
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
  map: Map<string, string[]>;
  selectedBarChartNameColumn: string;
  selectedBarChartDataColumn: string;
  selectedPieChartNameColumn: string;
  selectedPieChartDataColumn: string;
  constructor(@Inject(MAT_DIALOG_DATA) public data: ValueObject) {
    this.table = data.table;
    this.map = new Map<string, string[]>();
    this.selectedBarChartDataColumn = "";
    this.selectedBarChartNameColumn = "";
    this.selectedPieChartDataColumn = "";
    this.selectedPieChartNameColumn = "";
  }

  getKeys(map: Map<string, string[]>): string[] {
    return Array.from(map.keys());
}
  ngOnInit() {
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
   
    for (let column of this.columns) {
      
      let rows: string[] = []
      
      for (let row of this.table) {
        rows.push(String((row as any)[column]));
      }
     
      this.map.set(column, rows);
    }
  }

  async onClickGenerateChart(selectedChartNameColumn: string, selectedChartDataColumn: string, chartType : c3.ChartType, bindTo: string) {

    let dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    let count: number = 0;
    for (let name of this.map.get(selectedChartNameColumn)!) {
     
      let items:[string, ...PrimitiveArray] = [String(name)];
      items.push(Number(this.map.get(selectedChartDataColumn)![count++]));
      dataToDisplay.push(items)
     
    }
    console.log(dataToDisplay)
    c3.generate({
      data: {
          columns: dataToDisplay,
          type: chartType
      },
      bindto: bindTo
    });
  }

 
 
}

import { Component, Inject, OnInit, AfterViewInit } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';

interface DialogData {
  chartType: c3.ChartType;
  table: object[];
}
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit, AfterViewInit {
  table: object[];
  columns: string[] = [];
  columnMap: Map<string, string[]>;


  constructor(@Inject(MAT_DIALOG_DATA) public data: DialogData) {
    this.table = data.table;
    this.columnMap = new Map<string, string[]>();
  }

  getKeys(map: Map<string, string[]>): string[] {
    return Array.from(map.keys());
  }
  ngOnInit() {
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
    // store data in column format
    // key of columnMap is the column name
    // value of columnMap is the array of the column values in all rows
    for (const column of this.columns) {

      const rows: string[] = [];

      for (const row of this.table) {
        rows.push(String((row as any)[column]));
      }

      this.columnMap.set(column, rows);

    }

  }

  ngAfterViewInit() {
    this.onClickGenerateChart();
  }

  async onClickGenerateChart() {

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    let count = 0;
    // display the categories of data in the x-axis
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }
    for (const dataName of this.columnMap.get(this.columns[0])!) {
      // c3.js requires the first item of array is the data name
      // the first column of visualization operator result must be name column.
      // the remaining columns are data column.
      // the rest items are the data
      // for example :  [['tom', 90], ['jerry', 80], ['bob', 70]]

      const items: [string, ...PrimitiveArray] = [String(dataName)];

      for (let i = 1; i < this.columns.length; i++) {
        items.push(Number(this.columnMap.get(this.columns[i])![count]));
      }
      count++;
      dataToDisplay.push(items);

    }

    c3.generate({
      size: {
        height: 1080,
        width: 1920
      },
      data: {
        columns: dataToDisplay,
        type: this.data.chartType
      },
      axis: {
        x: {
          type: 'category',
          categories: category
        }
      },
      bindto: '#Chart'
    });
  }



}

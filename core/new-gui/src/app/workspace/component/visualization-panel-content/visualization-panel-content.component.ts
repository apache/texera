import { Component, Inject, OnInit, AfterViewInit } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';

interface DialogData {
  table: object[];
  chartType: c3.ChartType;
}
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit, AfterViewInit {
  table: object[];
  columns: string[] = [];


  constructor(@Inject(MAT_DIALOG_DATA) public data: DialogData) {
    this.table = data.table;
  }

  ngOnInit() {
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
  }

  ngAfterViewInit() {
    this.onClickGenerateChart();
  }

  async onClickGenerateChart() {

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
  //  let count = 1;
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    // c3.js requires the first element in the data array is the data name.
    // the remaining items are data.
    let firstRow = true;
    for (const row of this.table) {
      if (firstRow) {
        firstRow = false;
        continue;
      }
      const items: [string, ...PrimitiveArray] = [(row as any)[this.columns[0]]];
      for (let i = 1; i < this.columns.length; i++) {
        items.push(Number((row as any)[this.columns[i]]));
      }
      dataToDisplay.push(items);
    }

    c3.generate({
      size: {
        height: 600,
        width: 800
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

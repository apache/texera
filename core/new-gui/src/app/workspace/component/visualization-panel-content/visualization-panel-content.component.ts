import { Component, Inject, OnInit, AfterViewInit } from '@angular/core';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';

interface DialogData {
  table: object[];
  chartType: c3.ChartType;
}
/**
 * VisualizationPanelContentComponent displays the chart based on the chart type and data in table.
 *
 * It will convert the table into data format required by c3.js.
 * Then it passes the data and figure type to c3.js for rendering the figure.
 * @author Mingji Han
 */
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit, AfterViewInit {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly WIDTH = 800;
  public static readonly HEIGHT = 600;
  table: object[];
  columns: string[] = [];
  map: Map<string, string[]>;


  constructor(@Inject(MAT_DIALOG_DATA) public data: DialogData) {
    this.table = data.table;
    this.map = new Map<string, string[]>();
  }

  ngOnInit() {

    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');

    for (const column of this.columns) {

      const rows: string[] = [];

      for (const row of this.table) {
        rows.push(String((row as any)[column]));
      }

      this.map.set(column, rows);

    }


  }

  ngAfterViewInit() {
    this.onClickGenerateChart();
  }

  onClickGenerateChart() {

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    let count = 0;
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    for (const name of this.map.get(this.columns![0])!) {

      const items: [string, ...PrimitiveArray] = [String(name)];
      for (let i = 1; i < this.columns?.length; i++) {
        items.push(Number(this.map.get(this.columns![1])![count++]));
      }
      dataToDisplay.push(items);

    }

    c3.generate({
      size: {
        height: VisualizationPanelContentComponent.HEIGHT,
        width: VisualizationPanelContentComponent.WIDTH
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

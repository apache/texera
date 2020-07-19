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
 * Then it pass the data and figure type to c3.js for rendering the figure.
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

  constructor(@Inject(MAT_DIALOG_DATA) public data: DialogData) {
    this.table = data.table;
  }

  ngOnInit() {
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
  }

  ngAfterViewInit() {
    this.onClickGenerateChart();
  }

  onClickGenerateChart() {

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
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
      bindto: VisualizationPanelContentComponent.CHART_ID
    });
  }

}

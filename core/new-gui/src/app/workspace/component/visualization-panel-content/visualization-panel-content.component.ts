import { Component, Input, OnInit, AfterViewInit } from '@angular/core';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';
import * as WordCloud from 'wordcloud';
import { ChartType, WordCloudTuple, DialogData } from '../../types/visualization.interface';


/**
 * VisualizationPanelContentComponent displays the chart based on the chart type and data in table.
 *
 * It will convert the table into data format required by c3.js.
 * Then it passes the data and figure type to c3.js for rendering the figure.
 * @author Mingji Han, Xiaozhen Liu
 */
@Component({
  selector: 'texera-visualization-panel-content',
  templateUrl: './visualization-panel-content.component.html',
  styleUrls: ['./visualization-panel-content.component.scss']
})
export class VisualizationPanelContentComponent implements OnInit, AfterViewInit {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly WORD_CLOUD_ID = 'texera-word-cloud';
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 800;
  @Input()
  public data!: DialogData;
  private table: object[] = [];
  private columns: string[] = [];


  ngOnInit() {
    this.table = this.data.table;
    this.columns = Object.keys(this.table[0]).filter(x => x !== '_id');
  }

  ngAfterViewInit() {
    switch (this.data.chartType) {
      // correspond to TexeraWordCloud.java
      case ChartType.WORD_CLOUD: this.onClickGenerateWordCloud(); break;
      // correspond to TexeraBarChart.java
      case ChartType.BAR:
      case ChartType.STACKED_BAR:
      // correspond to TexeraPieChart.java
      case ChartType.PIE:
      case ChartType.DONUT:
      // correspond to TexeraLineChart.java
      case ChartType.LINE:
      case ChartType.SPLINE: this.onClickGenerateChart(); break;
    }
  }

  onClickGenerateWordCloud() {
    const dataToDisplay: object[] = [];
    // this.table.shift(); // In the old engine the first line is column names

    const wordCloudTuples: Array<WordCloudTuple> = new Array;
    this.table.forEach(element => {
      const tupleContent = (element as Array<object>);
      const wordCloudTuple: WordCloudTuple = {
        word: tupleContent[0] as unknown as string,
        count: tupleContent[1] as unknown as number,
      };
      wordCloudTuples.push(wordCloudTuple);
    });

    // const wordCloudTuples = this.table as ReadonlyArray<WordCloudTuple>; // Cannot successfuly cast for lack of schema
    for (const tuple of wordCloudTuples) {
      dataToDisplay.push([tuple.word, tuple.count]);
    }
    WordCloud(document.getElementById(VisualizationPanelContentComponent.WORD_CLOUD_ID) as HTMLElement,
           { list: dataToDisplay } );
  }

  onClickGenerateChart() {
    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    const columnCount = this.columns.length;

    for (const row of this.table) {
      const items: [string, ...PrimitiveArray] = [Object.values(row)[0]];
      for (let i = 1; i < columnCount; i++) {
        items.push(Number((Object.values(row)[i])));
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
        type: this.data.chartType as c3.ChartType
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

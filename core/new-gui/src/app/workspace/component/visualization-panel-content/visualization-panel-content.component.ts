import { AfterViewInit, Component, Input, OnDestroy } from '@angular/core';
import * as c3 from 'c3';
import { PrimitiveArray } from 'c3';
import * as d3 from 'd3';
import * as cloud from 'd3-cloud';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ResultObject } from '../../types/execute-workflow.interface';
import { ChartType, WordCloudTuple } from '../../types/visualization.interface';

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
export class VisualizationPanelContentComponent implements AfterViewInit, OnDestroy {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly WORD_CLOUD_ID = 'texera-word-cloud';
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 800;

  @Input()
  operatorID: string | undefined;

  data: object[] | undefined;
  chartType: ChartType | undefined;
  columns: string[] = [];

  private wordCloudElement: d3.Selection<SVGGElement, unknown, HTMLElement, any> | undefined;
  private c3ChartElement: c3.ChartAPI | undefined;

  constructor(
    private workflowStatusService: WorkflowStatusService
  ) {
  }

  ngAfterViewInit() {
    this.drawChart();
  }

  ngOnDestroy() {
    if (this.wordCloudElement) {
      this.wordCloudElement.remove();
    }
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
  }

  drawChart() {
    if (!this.operatorID) {
      return;
    }
    const result: ResultObject | undefined = this.workflowStatusService.getCurrentResult()[this.operatorID];
    if (!result) {
      return;
    }
    this.data = result.table as object[];
    this.chartType = result.chartType;

    switch (this.chartType) {
      // correspond to WordCloudSink.java
      case ChartType.WORD_CLOUD:
        this.generateWordCloud();
        break;
      // correspond to TexeraBarChart.java
      case ChartType.BAR:
      case ChartType.STACKED_BAR:
      // correspond to PieChartSink.java
      case ChartType.PIE:
      case ChartType.DONUT:
      // correspond to TexeraLineChart.java
      case ChartType.LINE:
      case ChartType.SPLINE:
        this.generateChart();
        break;
    }
  }

  generateWordCloud() {
    if (!this.data || !this.chartType) {
      return;
    }

    if (this.wordCloudElement === undefined) {
      this.wordCloudElement =
        d3.select(`#${VisualizationPanelContentComponent.WORD_CLOUD_ID}`)
          .append('svg')
          .attr('width', VisualizationPanelContentComponent.WIDTH)
          .attr('height', VisualizationPanelContentComponent.HEIGHT)
          .append('g')
          .attr('transform',
            'translate(' + VisualizationPanelContentComponent.WIDTH / 2 + ','
            + VisualizationPanelContentComponent.HEIGHT / 2 + ')');
    }

    const wordCloudTuples = this.data as ReadonlyArray<WordCloudTuple>;

    const drawWordCloud = (words: cloud.Word[]) => {
      if (!this.wordCloudElement) {
        return;
      }
      const d3Fill = d3.scaleOrdinal(d3.schemeCategory10);

      const wordCloudData = this.wordCloudElement.selectAll<d3.BaseType, cloud.Word>('g text').data(words, d => d.text ?? '');

      wordCloudData.enter()
        .append('text')
        .style('font-size', (d) => d.size ?? 0 + 'px')
        .style('fill', d => d3Fill(d.text ?? ''))
        .attr('font-family', 'Impact')
        .attr('text-anchor', 'middle')
        .attr('transform', (d) => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        // this text() call must be at the end or it won't work
        .text(d => d.text ?? '');

      // Entering and existing words
      wordCloudData.transition()
        .duration(600)
        .attr('font-family', 'Impact')
        .style('font-size', d => d.size + 'px')
        .attr('transform', d => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        .style('fill-opacity', 1);

      // Exiting words
      wordCloudData.exit()
        .transition()
        .duration(200)
        .attr('font-family', 'Impact')
        .style('fill-opacity', 1e-6)
        .attr('font-size', 1)
        .remove();
    };

    const minCount = Math.min(...wordCloudTuples.map(t => t.count));
    const maxCount = Math.max(...wordCloudTuples.map(t => t.count));

    const minFontSize = 50;
    const maxFontSize = 150;

    const d3Scale = d3.scaleLinear();
    // const d3Scale = d3.scaleSqrt();
    // const d3Scale = d3.scaleLog();

    d3Scale.domain([minCount, maxCount]).range([minFontSize, maxFontSize]);

    const layout = cloud()
      .size([VisualizationPanelContentComponent.WIDTH, VisualizationPanelContentComponent.HEIGHT])
      .words(wordCloudTuples.map(t => ({text: t.word, size: d3Scale(t.count)})))
      .text(d => d.text ?? '')
      .padding(5)
      .rotate(() => 0)
      .font('Impact')
      .fontSize(d => d.size ?? 0)
      .random(() => 1)
      .on('end', drawWordCloud);

    layout.start();
  }

  generateChart() {
    if (!this.data || !this.chartType) {
      return;
    }

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    const category: string[] = [];
    for (let i = 1; i < this.columns?.length; i++) {
      category.push(this.columns[i]);
    }

    const columnCount = this.columns.length;

    for (const row of this.data) {
      const items: [string, ...PrimitiveArray] = [Object.values(row)[0]];
      for (let i = 1; i < columnCount; i++) {
        items.push(Number((Object.values(row)[i])));
      }
      dataToDisplay.push(items);
    }

    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationPanelContentComponent.HEIGHT,
        width: VisualizationPanelContentComponent.WIDTH
      },
      data: {
        columns: dataToDisplay,
        type: this.chartType as c3.ChartType
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

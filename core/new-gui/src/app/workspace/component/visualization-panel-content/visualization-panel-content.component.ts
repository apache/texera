import { AfterContentInit, Component, Input, OnDestroy } from '@angular/core';
import * as c3 from 'c3';
import { Primitive, PrimitiveArray } from 'c3';
import * as d3 from 'd3';
import * as cloud from 'd3-cloud';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ResultObject } from '../../types/execute-workflow.interface';
import { ChartType, WordCloudTuple } from '../../types/visualization.interface';
import { Subscription, Subject, Observable } from 'rxjs';
import { environment } from 'src/environments/environment';
import * as mapboxgl from 'mapbox-gl';
import { MapboxLayer } from '@deck.gl/mapbox';
import { ScatterplotLayer } from '@deck.gl/layers';
import { ScatterplotLayerProps } from '@deck.gl/layers/scatterplot-layer/scatterplot-layer';
import { DomSanitizer } from '@angular/platform-browser';

(mapboxgl as any).accessToken = environment.mapbox.accessToken;

export const wordCloudScaleOptions = ['linear', 'square root', 'logarithmic'] as const;
type WordCloudControlsType = {
  scale: typeof wordCloudScaleOptions[number]
};

// TODO: The current design doesn't decouple the visualization types into different modules
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
export class VisualizationPanelContentComponent implements AfterContentInit, OnDestroy {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = '#texera-result-chart-content';
  public static readonly MAP_CONTAINER = 'texera-result-map-container';

  // width and height of the canvas in px
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 600;

  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;
  public static readonly WORD_CLOUD_CONTROL_UPDATE_INTERVAL_MS = 50;

  private static readonly props: ScatterplotLayerProps<any> = {
    opacity: 0.8,
    filled: true,
    radiusScale: 100,
    radiusMinPixels: 1,
    radiusMaxPixels: 25,
    getPosition: (d: { xColumn: number; yColumn: number; }) => [d.xColumn, d.yColumn],
    getFillColor: [57, 73, 171]
  };

  wordCloudScaleOptions = wordCloudScaleOptions; // make this a class variable so template can access it
  // word cloud related controls
  wordCloudControls: WordCloudControlsType = {
    scale: 'linear',
  };

  wordCloudControlUpdateObservable = new Subject<WordCloudControlsType>();

  htmlData: any = '';

  @Input()
  operatorID: string | undefined;
  displayHTML: boolean = false; // variable to decide whether to display the container to display the HTML container(iFrame)
  displayWordCloud: boolean = false; // variable to decide whether to display the container for worldcloud visualization
  displayMap: boolean = true; // variable to decide whether to hide/unhide the map
  data: object[] | undefined;
  chartType: ChartType | undefined;
  columns: string[] = [];

  private wordCloudElement: d3.Selection<SVGGElement, unknown, HTMLElement, any> | undefined;
  private c3ChartElement: c3.ChartAPI | undefined;
  private map: mapboxgl.Map | undefined;

  private updateSubscription: Subscription | undefined;

  constructor(
    private workflowStatusService: WorkflowStatusService,
    private sanitizer: DomSanitizer
  ) {
  }

  ngAfterContentInit() {
    // attempt to draw chart immediately
    this.drawChart();

    // setup an event lister that re-draws the chart content every (n) milliseconds
    // auditTime makes sure the first re-draw happens after (n) milliseconds has elapsed
    const resultUpdate = this.workflowStatusService.getResultUpdateStream()
      .auditTime(VisualizationPanelContentComponent.UPDATE_INTERVAL_MS);
    const controlUpdate = this.wordCloudControlUpdateObservable
      .debounceTime(VisualizationPanelContentComponent.WORD_CLOUD_CONTROL_UPDATE_INTERVAL_MS);

    this.updateSubscription = Observable.merge(resultUpdate, controlUpdate).subscribe(() => {
      this.drawChart();
    });
  }

  ngOnDestroy() {
    if (this.wordCloudElement) {
      this.wordCloudElement.remove();
    }
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    if (this.map) {
      this.map.remove();
    }
    if (this.updateSubscription) {
      this.updateSubscription.unsubscribe();
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
    if (!this.data || !this.chartType) {
      return;
    }
    if (this.data?.length < 1) {
      return;
    }
    this.displayHTML = false;
    this.displayWordCloud = false;
    this.displayMap = true;
    switch (this.chartType) {
      // correspond to WordCloudSink.java
      case ChartType.WORD_CLOUD:
        this.displayWordCloud = true;
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
      case ChartType.SPATIAL_SCATTERPLOT:
        this.displayMap = false;
        this.generateSpatialScatterplot();
        break;
      case ChartType.SIMPLE_SCATTERPLOT:
        this.generateSimpleScatterplot();
        break;
      case ChartType.HTML_VIZ:
        this.displayHTML = true;
        this.generateHTML();
        break;
    }
  }

  generateSimpleScatterplot() {
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    const result = this.data as Array<Record<string, Primitive>>;
    const xLabel: string = Object.keys(result[0])[0];
    const yLabel: string = Object.keys(result[0])[1];

    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationPanelContentComponent.HEIGHT,
        width: VisualizationPanelContentComponent.WIDTH
      },
      data: {
        json: result,
        keys: {
          x: xLabel,
          value: [yLabel]
        },
        type: this.chartType as c3.ChartType
      },
      axis: {
        x: {
          label: xLabel,
          tick: {
            fit: true
          }
        },
        y: {
          label: yLabel
        }
      },
      bindto: VisualizationPanelContentComponent.CHART_ID
    });
  }

  generateSpatialScatterplot() {
    if (this.map === undefined) {
      this.initMap();
    }
    /* after the map is defined and the base
    style is loaded, we add a layer of the data points */
    this.map?.on('styledata', () => {
      this.addNeworReplaceExistingLayer();
    });
  }

  initMap() {
    /* mapbox object with default configuration */
    this.map = new mapboxgl.Map({
      container: VisualizationPanelContentComponent.MAP_CONTAINER,
      style: 'mapbox://styles/mapbox/light-v9',
      center: [-96.35, 39.5],
      zoom: 3,
      maxZoom: 17,
      minZoom: 0
    });
  }

  addNeworReplaceExistingLayer() {
    if (!this.map) {
      return;
    }
    if (this.map?.getLayer('scatter')) {
      this.map?.removeLayer('scatter');
    }

    const clusterLayer = new MapboxLayer({
      id: 'scatter',
      type: ScatterplotLayer,
      data: this.data,
      pickable: true,
    });
    clusterLayer.setProps(VisualizationPanelContentComponent.props);
    this.map.addLayer(clusterLayer);
  }


  updateWordCloudScale(scale: typeof wordCloudScaleOptions[number]) {
    if (this.wordCloudControls.scale !== scale) {
      this.wordCloudControls.scale = scale;
      this.wordCloudControlUpdateObservable.next(this.wordCloudControls);
    }
  }

  generateWordCloud() {
    if (!this.data || !this.chartType) {
      return;
    }

    if (this.wordCloudElement === undefined) {
      this.wordCloudElement =
        d3.select(VisualizationPanelContentComponent.CHART_ID)
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
        .duration(300)
        .attr('font-family', 'Impact')
        .style('font-size', d => d.size + 'px')
        .attr('transform', d => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')')
        .style('fill-opacity', 1);

      // Exiting words
      wordCloudData.exit()
        .transition()
        .duration(100)
        .attr('font-family', 'Impact')
        .style('fill-opacity', 1e-6)
        .attr('font-size', 1)
        .remove();
    };

    const minCount = Math.min(...wordCloudTuples.map(t => t.count));
    const maxCount = Math.max(...wordCloudTuples.map(t => t.count));

    const minFontSize = 50;
    const maxFontSize = 150;

    const getScale: () => d3.ScaleContinuousNumeric<number, number> = () => {
      switch (this.wordCloudControls.scale) {
        case 'linear':
          return d3.scaleLinear();
        case 'logarithmic':
          return d3.scaleLog();
        case 'square root':
          return d3.scaleSqrt();
      }
    };
    const d3Scale = getScale();
    d3Scale.domain([minCount, maxCount]).range([minFontSize, maxFontSize]);

    const layout = cloud()
      .size([VisualizationPanelContentComponent.WIDTH, VisualizationPanelContentComponent.HEIGHT])
      .words(wordCloudTuples.map(t => ({ text: t.word, size: d3Scale(t.count) })))
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

  generateHTML() {
    if (!this.data) {
      return;
    }
    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(Object(this.data[0])['html-content']); // this line bypasses angular security
  }
}

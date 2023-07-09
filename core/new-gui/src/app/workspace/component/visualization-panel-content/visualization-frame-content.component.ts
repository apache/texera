import { AfterContentInit, Component, Input, OnDestroy, OnInit } from "@angular/core";
import * as c3 from "c3";
import { Primitive, PrimitiveArray } from "c3";
import * as d3 from "d3";
import * as cloud from "d3-cloud";
import { ChartType, WordCloudTuple } from "../../types/visualization.interface";
import { merge, Subject } from "rxjs";
import { environment } from "src/environments/environment";
import { MapboxLayer } from "@deck.gl/mapbox/typed";
import { ScatterplotLayer, ScatterplotLayerProps } from "@deck.gl/layers/typed";
import { DomSanitizer } from "@angular/platform-browser";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { auditTime, debounceTime } from "rxjs/operators";
import { untilDestroyed, UntilDestroy } from "@ngneat/until-destroy";
import * as mapboxgl from "mapbox-gl";
import { isDefined } from "../../../common/util/predicate";

(mapboxgl as any).accessToken = environment.mapbox.accessToken;

export const wordCloudScaleOptions = ["linear", "square root", "logarithmic"] as const;
type WordCloudControlsType = {
  scale: typeof wordCloudScaleOptions[number];
};

// TODO: The current design doesn't decouple the visualization types into different modules
/**
 * VisualizationFrameContentComponent displays the chart based on the chart type and data in table.
 * It receives the data for visualization and chart type and converts the table into data format
 * required by c3.js.
 * Then it passes the data and figure type to c3.js for rendering the figure.
 */
@UntilDestroy()
@Component({
  selector: "texera-visualization-panel-content",
  templateUrl: "./visualization-frame-content.component.html",
  styleUrls: ["./visualization-frame-content.component.scss"],
})
export class VisualizationFrameContentComponent implements OnInit, AfterContentInit, OnDestroy {
  // this readonly variable must be the same as HTML element ID for visualization
  public static readonly CHART_ID = "#texera-result-chart-content";
  public static readonly MAP_CONTAINER = "texera-result-map-container";

  // width and height of the canvas in px
  public static readonly WIDTH = 1000;
  public static readonly HEIGHT = 600;

  // progressive visualization update and redraw interval in milliseconds
  public static readonly UPDATE_INTERVAL_MS = 2000;
  public static readonly WORD_CLOUD_CONTROL_UPDATE_INTERVAL_MS = 50;

  wordCloudScaleOptions = wordCloudScaleOptions; // make this a class variable so template can access it
  // word cloud related controls
  wordCloudControls: WordCloudControlsType = {
    scale: "linear",
  };

  wordCloudControlUpdateObservable = new Subject<WordCloudControlsType>();

  htmlData: any = "";

  @Input()
  operatorId?: string;
  displayHTML: boolean = false; // variable to decide whether to display the container to display the HTML container(iFrame)
  displayWordCloud: boolean = false; // variable to decide whether to display the container for world cloud visualization
  displayMap: boolean = true; // variable to decide whether to hide/un-hide the map
  data: ReadonlyArray<object> = [];
  chartType?: ChartType;
  columns: string[] = [];
  /* Mapbox doesn't allow drawing points on the map if the style is not rendered,
   * hence we keep a flag to check if the style is loaded */
  isMapStyleRendered: boolean = false;

  private wordCloudElement?: d3.Selection<SVGGElement, unknown, HTMLElement, any>;
  private c3ChartElement?: c3.ChartAPI;
  private map?: mapboxgl.Map;

  constructor(private workflowResultService: WorkflowResultService, private sanitizer: DomSanitizer) {}

  ngOnInit() {
    this.initMap();
    this.map?.on("styledata", () => {
      this.isMapStyleRendered = true;
    });
  }

  ngAfterContentInit() {
    // attempt to draw chart immediately
    this.drawChart();

    // setup an event lister that re-draws the chart content every (n) milliseconds
    // auditTime makes sure the first re-draw happens after (n) milliseconds has elapsed
    const resultUpdate = this.workflowResultService
      .getResultUpdateStream()
      .pipe(auditTime(VisualizationFrameContentComponent.UPDATE_INTERVAL_MS));
    const controlUpdate = this.wordCloudControlUpdateObservable.pipe(
      debounceTime(VisualizationFrameContentComponent.WORD_CLOUD_CONTROL_UPDATE_INTERVAL_MS)
    );
    merge(resultUpdate, controlUpdate)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
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
  }

  drawChart() {
    if (!this.operatorId) {
      return;
    }
    const operatorResultService = this.workflowResultService.getResultService(this.operatorId);
    if (!operatorResultService) {
      return;
    }
    this.data = operatorResultService.getCurrentResultSnapshot() ?? [];
    this.chartType = operatorResultService.getChartType();
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
        this.generateSpatialScatterPlot();
        break;
      case ChartType.SIMPLE_SCATTERPLOT:
        this.generateSimpleScatterPlot();
        break;
      case ChartType.HTML_VIZ:
        this.displayHTML = true;
        this.generateHTML();
        break;
    }
  }

  generateSimpleScatterPlot() {
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    const result = this.data as Array<Record<string, Primitive>>;
    const xLabel: string = Object.keys(result[0])[0];
    const yLabel: string = Object.keys(result[0])[1];

    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationFrameContentComponent.HEIGHT,
        width: VisualizationFrameContentComponent.WIDTH,
      },
      data: {
        json: result,
        keys: {
          x: xLabel,
          value: [yLabel],
        },
        type: this.chartType as c3.ChartType,
      },
      axis: {
        x: {
          label: xLabel,
          tick: {
            fit: true,
          },
        },
        y: {
          label: yLabel,
        },
      },
      bindto: VisualizationFrameContentComponent.CHART_ID,
    });
  }

  generateSpatialScatterPlot() {
    /* after the map style is loaded, we add a layer of the data points */
    if (!this.isMapStyleRendered) {
      this.map?.on("styledata", () => {
        this.addNewOrReplaceExistingLayer();
      });
    } else {
      this.addNewOrReplaceExistingLayer();
    }
  }

  initMap() {
    /* mapbox object with default configuration */
    this.map = new mapboxgl.Map({
      container: VisualizationFrameContentComponent.MAP_CONTAINER,
      style: "mapbox://styles/mapbox/light-v9",
      center: [-96.35, 39.5],
      zoom: 3,
      maxZoom: 17,
      minZoom: 0,
    });
  }

  addNewOrReplaceExistingLayer() {
    if (!isDefined(this.map)) {
      return;
    }
    if (this.map.getLayer("scatter")) {
      this.map.removeLayer("scatter");
    }

    this.map.addLayer(
      new MapboxLayer({
        type: ScatterplotLayer,
        id: "scatter",
        data: this.data,
        getPosition: (d: { xColumn: number; yColumn: number }) => [d.xColumn, d.yColumn],
        getFillColor: [57, 73, 171],
        opacity: 0.8,
        filled: true,
        radiusScale: 100,
        radiusMinPixels: 1,
        radiusMaxPixels: 25,
        pickable: true,
      } as ScatterplotLayerProps)
    );
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
      this.wordCloudElement = d3
        .select(VisualizationFrameContentComponent.CHART_ID)
        .append("svg")
        .attr("width", VisualizationFrameContentComponent.WIDTH)
        .attr("height", VisualizationFrameContentComponent.HEIGHT)
        .append("g")
        .attr(
          "transform",
          "translate(" +
            VisualizationFrameContentComponent.WIDTH / 2 +
            "," +
            VisualizationFrameContentComponent.HEIGHT / 2 +
            ")"
        );
    }

    const wordCloudTuples = this.data as ReadonlyArray<WordCloudTuple>;

    const drawWordCloud = (words: cloud.Word[]) => {
      if (!this.wordCloudElement) {
        return;
      }
      const d3Fill = d3.scaleOrdinal(d3.schemeCategory10);

      const wordCloudData = this.wordCloudElement
        .selectAll<d3.BaseType, cloud.Word>("g text")
        .data(words, d => d.text ?? "");

      wordCloudData
        .enter()
        .append("text")
        .style("font-size", d => d.size ?? 0 + "px")
        .style("fill", d => d3Fill(d.text ?? ""))
        .attr("font-family", "Impact")
        .attr("text-anchor", "middle")
        .attr("transform", d => "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")")
        // this text() call must be at the end or it won't work
        .text(d => d.text ?? "");

      // Entering and existing words
      wordCloudData
        .transition()
        .duration(300)
        .attr("font-family", "Impact")
        .style("font-size", d => d.size + "px")
        .attr("transform", d => "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")")
        .style("fill-opacity", 1);

      // Exiting words
      wordCloudData
        .exit()
        .transition()
        .duration(100)
        .attr("font-family", "Impact")
        .style("fill-opacity", 1e-6)
        .attr("font-size", 1)
        .remove();
    };

    const minCount = Math.min(...wordCloudTuples.map(t => t.count));
    const maxCount = Math.max(...wordCloudTuples.map(t => t.count));

    const minFontSize = 50;
    const maxFontSize = 150;

    const getScale: () => d3.ScaleContinuousNumeric<number, number> = () => {
      switch (this.wordCloudControls.scale) {
        case "linear":
          return d3.scaleLinear();
        case "logarithmic":
          return d3.scaleLog();
        case "square root":
          return d3.scaleSqrt();
      }
    };
    const d3Scale = getScale();
    d3Scale.domain([minCount, maxCount]).range([minFontSize, maxFontSize]);

    const layout = cloud()
      .size([VisualizationFrameContentComponent.WIDTH, VisualizationFrameContentComponent.HEIGHT])
      .words(wordCloudTuples.map(t => ({ text: t.word, size: d3Scale(t.count) })))
      .text(d => d.text ?? "")
      .padding(5)
      .rotate(() => 0)
      .font("Impact")
      .fontSize(d => d.size ?? 0)
      .random(() => 1)
      .on("end", drawWordCloud);

    layout.start();
  }

  generateChart() {
    if (!this.data || !this.chartType) {
      return;
    }

    const dataToDisplay: Array<[string, ...PrimitiveArray]> = [];
    const category: string[] = [];

    const result = this.data as Array<Record<string, Primitive>>;

    // category for x-axis
    for (let i = 1; i < Object.values(result[0]).length; i++) {
      category.push(String(Object.keys(result[0])[i]));
    }
    const columnCount = category.length;

    // data
    for (const row of result) {
      var items: [string, ...PrimitiveArray] = [String(Object.values(row)[0])];
      for (let i = 1; i < columnCount + 1; i++) {
        items.push(Number(Object.values(row)[i]));
      }
      dataToDisplay.push(items);
    }
    // generate chart
    if (this.c3ChartElement) {
      this.c3ChartElement.destroy();
    }
    this.c3ChartElement = c3.generate({
      size: {
        height: VisualizationFrameContentComponent.HEIGHT,
        width: VisualizationFrameContentComponent.WIDTH,
      },
      data: {
        columns: dataToDisplay,
        type: this.chartType as c3.ChartType,
      },
      axis: {
        x: {
          type: "category",
          categories: category,
        },
      },
      bindto: VisualizationFrameContentComponent.CHART_ID,
    });
  }

  generateHTML() {
    if (!this.data) {
      return;
    }
    this.htmlData = this.sanitizer.bypassSecurityTrustHtml(Object(this.data[0])["html-content"]); // this line bypasses angular security
  }
}

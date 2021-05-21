import { Component, Input, OnChanges } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ResultObject } from '../../types/execute-workflow.interface';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';
import {ChartType} from "../../types/visualization.interface";

/**
 * VisualizationPanelComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * It receives the data for visualization and chart type.
 * When user click on button, this component will open VisualizationPanelContentComponent and display figure.
 * User could click close at the button of VisualizationPanelContentComponent to exit the visualization panel.
 * @author Mingji Han
 */
@Component({
  selector: 'texera-visualization-panel',
  templateUrl: './visualization-panel.component.html',
  styleUrls: ['./visualization-panel.component.scss']
})
export class VisualizationPanelComponent implements OnChanges {

  @Input() operatorID: string | undefined;
  displayVisualizationPanel: boolean = false;
  modalRef: NzModalRef | undefined;
  data: object[] | undefined;
  chartType: ChartType | undefined;
  constructor(
    private modalService: NzModalService,
    private workflowStatusService: WorkflowStatusService
  ) {
    this.workflowStatusService.getResultUpdateStream().subscribe(event => {
      this.updateDisplayVisualizationPanel();
    });
  }

  ngOnChanges() {
    this.updateDisplayVisualizationPanel();
  }

  updateDisplayVisualizationPanel() {
    if (!this.operatorID) {
      this.displayVisualizationPanel = false;
      return;
    }
    const result: ResultObject | undefined = this.workflowStatusService.getCurrentResult()[this.operatorID];
    this.displayVisualizationPanel = result?.chartType !== undefined;
  }

  onClickVisualize(): void {
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
      // correspond to TexeraBarChart.java
      case ChartType.BAR:
      case ChartType.STACKED_BAR:
      // correspond to PieChartSink.java
      case ChartType.PIE:
      case ChartType.DONUT:
      // correspond to TexeraLineChart.java
      case ChartType.LINE:
      case ChartType.SPLINE:
        this.generateSeparateComponent();
        break;
      case ChartType.HTML_VIZ:
        this.generateHTMLContent();
        break;
    }
  }

  generateHTMLContent() {
    if (! this.data) {
      return;
    }
    console.log(this.data[0]['HTML_content']);
    // @ts-ignore
    this.modalRef = this.modalService.create({
      nzTitle: 'Visualization',
      nzWidth: 1100,
      nzFooter: null,
      nzContent: this.data[0]['HTML_content'],
      nzComponentParams: {
        operatorID: this.operatorID
      }
    });
  }
  generateSeparateComponent() {
    this.modalRef = this.modalService.create({
      nzTitle: 'Visualization',
      nzWidth: 1100,
      nzContent: VisualizationPanelContentComponent,
      nzComponentParams: {
        operatorID: this.operatorID
      }
    });
  }
}

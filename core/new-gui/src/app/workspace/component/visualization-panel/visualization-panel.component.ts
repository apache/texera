import { Component, Input, OnChanges } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';
import { WorkflowResultService } from '../../service/workflow-result/workflow-result.service';

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

  constructor(
    private modalService: NzModalService,
    private workflowResultService: WorkflowResultService
  ) {
    this.workflowResultService.getResultUpdateStream().subscribe(event => {
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
    const opratorResultService = this.workflowResultService.getResultService(this.operatorID);
    if (! opratorResultService) {
      return;
    }
    const chartType = opratorResultService.getChartType();
    this.displayVisualizationPanel = chartType !== undefined && chartType !== null;
  }

  onClickVisualize(): void {
    if (!this.operatorID) {
      return;
    }

    this.modalRef = this.modalService.create({
      nzTitle: 'Visualization',
      nzStyle: {top: '20px'},
      nzWidth: 1100,
      nzFooter: null, // null indicates that the footer of the window would be hidden
      nzContent: VisualizationPanelContentComponent,
      nzComponentParams: {
        operatorID: this.operatorID
      }
    });
  }

}

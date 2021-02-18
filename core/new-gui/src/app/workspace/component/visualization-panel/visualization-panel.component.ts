import { Component, Input, OnChanges } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { ResultObject } from '../../types/execute-workflow.interface';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';

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

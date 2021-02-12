import { Component, Input, OnChanges, } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { VisualizationPanelContentComponent } from '../visualization-panel-content/visualization-panel-content.component';
import { ChartType } from '../../types/visualization.interface';
import { assertType } from '../../../common/util/assert';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';

/**
 * VisualizationPanelComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * It receives the data for visualization and chart type.
 * When user click on button, this component will open VisualzationPanelContentComponent and display figure.
 * User could click close at the button of VisualzationPanelContentComponent to exit the visualization panel.
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


  constructor(
    private modal: NzModalService,
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
    if (! this.operatorID) {
      this.displayVisualizationPanel = false;
      return;
    }
    this.displayVisualizationPanel = this.workflowStatusService.getCurrentResult()[this.operatorID]?.chartType !== undefined;
  }

  onClickVisualize(): void {
    const dialogRef = this.modal.create({
      nzTitle: 'Visualization',
      nzWidth: 1100,
      nzContent: VisualizationPanelContentComponent,
      nzComponentParams: {
        operatorID: this.operatorID
      }
    });

  }


}

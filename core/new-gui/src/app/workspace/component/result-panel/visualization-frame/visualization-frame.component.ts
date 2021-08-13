import { Component } from '@angular/core';
import { NzModalRef, NzModalService } from 'ng-zorro-antd/modal';
import { VisualizationFrameContentComponent } from '../../visualization-panel-content/visualization-frame-content.component';
import { WorkflowResultService } from '../../../service/workflow-result/workflow-result.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service';

/**
 * VisualizationFrameComponent displays the button for visualization in ResultPanel when the result type is chart.
 *
 * It receives the data for visualization and chart type.
 * When user click on button, this component will open VisualizationPanelContentComponent and display figure.
 * User could click close at the button of VisualizationPanelContentComponent to exit the visualization panel.
 * @author Mingji Han
 */
@Component({
  selector: 'texera-visualization-panel',
  templateUrl: './visualization-frame.component.html',
  styleUrls: ['./visualization-frame.component.scss']
})
export class VisualizationFrameComponent {

  resultPanelOperatorID: string | undefined;
  modalRef: NzModalRef | undefined;

  constructor(
    private modalService: NzModalService,
    private workflowResultService: WorkflowResultService,
    private workflowActionService: WorkflowActionService
  ) {
    this.updateDisplayVisualizationPanel();
    this.workflowResultService.getResultUpdateStream().subscribe(event => {
      this.updateDisplayVisualizationPanel();
    });
  }

  updateDisplayVisualizationPanel() {
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    this.resultPanelOperatorID = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

  }

  onClickVisualize(): void {
    if (!this.resultPanelOperatorID) {
      return;
    }

    this.modalRef = this.modalService.create({
      nzTitle: 'Visualization',
      nzStyle: {top: '20px'},
      nzWidth: 1100,
      nzFooter: null, // null indicates that the footer of the window would be hidden
      nzContent: VisualizationFrameContentComponent,
      nzComponentParams: {
        operatorID: this.resultPanelOperatorID
      }
    });
  }

}

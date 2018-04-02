import { Component, AfterViewInit } from '@angular/core';

import * as joint from 'jointjs';
import { OperatorViewElementService } from '../../service/operator-view-element/operator-view-element.service';

@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements AfterViewInit {

  public readonly WORKFLOW_EDITOR_ELEMENT_ID = 'texera-workflow-editor-body-id';

  private paper: joint.dia.Paper = null;
  private graph: joint.dia.Graph = new joint.dia.Graph();

  constructor(
    private operatorViewElementService: OperatorViewElementService
  ) { }

  ngAfterViewInit() {
    this.createJointjsPaper();

    this.graph.addCell(
      this.operatorViewElementService.getJointjsOperatorElement(
        'ScanSource',
        'operator1',
        100, 100
      )
    );

    this.graph.addCell(
      this.operatorViewElementService.getJointjsOperatorElement(
        'ViewResults',
        'operator2',
        500, 100
      )
    );

    const link = this.operatorViewElementService.getJointjsLinkElement(
      { operatorID: 'operator1', portID: 'out0' },
      { operatorID: 'operator2', portID: 'in0' }
    );


    this.graph.addCell(link);

  }

  /**
 * Creates a JointJS Paper object, which is the JointJS view object responsible for
 *  rendering the workflow cells and handle UI events.
 *
 * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
 */
  private createJointjsPaper(): joint.dia.Paper {

    const paper = new joint.dia.Paper({
      el: $('#' + this.WORKFLOW_EDITOR_ELEMENT_ID),
      model: this.graph,
      height: $('#' + this.WORKFLOW_EDITOR_ELEMENT_ID).height(),
      width: $('#' + this.WORKFLOW_EDITOR_ELEMENT_ID).width(),
      gridSize: 1,
      snapLinks: true,
      linkPinning: false,
      validateConnection: validateOperatorConnection,
      interactive: { vertexAdd: false },
      defaultLink: this.operatorViewElementService.getDefaultLinkElement(),
      preventDefaultBlankAction: false,
      preventContextMenu: false,
    });

    return paper;
  }

}

/**
* This function is provided to JointJS to disable some invalid connections on the UI.
* If the connection is invalid, users are not able to connect the links on the UI.
*
* https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateConnection
*
* @param sourceView
* @param sourceMagnet
* @param targetView
* @param targetMagnet
*/
function validateOperatorConnection(sourceView: joint.dia.CellView, sourceMagnet: SVGElement,
  targetView: joint.dia.CellView, targetMagnet: SVGElement): boolean {
  // user cannot draw connection starting from the input port (left side)
  if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') { return false; }

  // user cannot connect to the output port (right side)
  if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') { return false; }

  return sourceView.id !== targetView.id;
}




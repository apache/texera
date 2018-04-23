import { WorkflowUtilService } from './../../service/workflow-graph/util/workflow-util.service';
import { WorkflowModelActionService } from './../../service/workflow-graph/model/workflow-model-action.service';
import { JointModelService } from './../../service/workflow-graph/model/jointjs-model.service';
import { Component, AfterViewInit } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';

/**
 * WorkflowEditorComponent is the componenet for the main workflow editor part of the UI.
 *
 * This componenet is binded with the JointJS paper. JointJS handles the operations of the main workflow.
 * The JointJS UI events are wrapped into observables and exposed to other components / services.
 *
 * See JointJS documentation for the list of events that can be captured on the JointJS paper view.
 * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.events
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
*/
@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements AfterViewInit {

  // the DOM element ID of the main editor. It can be used by jQuery and jointJS to find the DOM element
  // in the HTML template, the div element ID is set using this variable
  public readonly WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID = 'texera-workflow-editor-jointjs-wrapper-id';
  public readonly WORKFLOW_EDITOR_JOINTJS_ID = 'texera-workflow-editor-jointjs-body-id';

  public paper: joint.dia.Paper = null;
  public graph: joint.dia.Graph = null;

  constructor(
    private jointUIService: JointUIService,
    private jointModelService: JointModelService,
    private workflowModelActionService: WorkflowModelActionService,
    private workflowUtilService: WorkflowUtilService
  ) {
    this.graph = jointModelService.jointGraph;
  }

  ngAfterViewInit() {

    this.createJointjsPaper();

    // add a 500ms delay for joint-ui.service to fetch the operator metaData
    // this code is temporary and will be deleted in future PRs when drag
    // and drop is implemented

    Observable.of([]).delay(500).subscribe(
      emptyData => {
        // add some dummy operators and links to show that JointJS works
        this.workflowModelActionService.addOperator(
          this.workflowUtilService.getNewOperatorPredicate('ScanSource'),
          { x: 300, y: 250 }
        );

        this.workflowModelActionService.addOperator(
          this.workflowUtilService.getNewOperatorPredicate('ViewResults'),
          { x: 500, y: 200 }
        );

        this.workflowModelActionService.addOperator(
          this.workflowUtilService.getNewOperatorPredicate('ViewResults'),
          { x: 500, y: 300 }
        );

      }
    );
  }

  /**
   * Creates a JointJS Paper object, which is the JointJS view object responsible for
   *  rendering the workflow cells and handle UI events.
   *
   * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
   */
  private createJointjsPaper(): void {

    const paper = new joint.dia.Paper({
      // bind the DOM element
      el: $('#' + this.WORKFLOW_EDITOR_JOINTJS_ID),
      // bind the jointjs graph model
      model: this.graph,
      // set the width and height of the paper to be the width height of the parent wrapper element
      width: this.getWrapperElementSize().width,
      height: this.getWrapperElementSize().height,
      // set grid size to 1px (smallest grid)
      gridSize: 1,
      // enable jointjs feature that automatically snaps a link to the closest port with a radius of 30px
      snapLinks: { radius: 30 },
      // disable jointjs default action that can make a link not connect to an operator
      linkPinning: false,
      // provide a validation to determine if two ports could be connected (only output connect to input is allowed)
      validateConnection: validateOperatorConnection,
      // disable jointjs default action of adding vertexes to the link
      interactive: { vertexAdd: false },
      // set a default link element used by jointjs when user creates a link on UI
      defaultLink: this.jointUIService.getDefaultLinkElement(),
      // disable jointjs default action that stops propagate click events on jointjs paper
      preventDefaultBlankAction: false,
      // disable jointjs default action that prevents normal right click menu showing up on jointjs paper
      preventContextMenu: false,
    });

    this.paper = paper;
    this.jointModelService.registerJointPaper(paper);
  }

  /**
   * get the width and height of the parent wrapper element
   */
  private getWrapperElementSize(): { width: number, height: number } {
    return {
      width: $('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).width(),
      height: $('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).height()
    };
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




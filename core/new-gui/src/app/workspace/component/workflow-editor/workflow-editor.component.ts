import { DragDropService } from './../../service/drag-drop/drag-drop.service';

import { JointUIService } from './../../service/joint-ui/joint-ui.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { Component, AfterViewInit } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import * as joint from 'jointjs';

// argument type of callback event on a JointJS Paper
// which is a 4-element tuple:
// 1. the JointJS View (CellView) of the event
// 2. the corresponding original JQuery Event
// 3. x coordinate, 4. y coordinate
type JointPaperEvent = [joint.dia.CellView, JQuery.Event, number, number];

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

  private paper: joint.dia.Paper | undefined;

  constructor(
    private workflowActionService: WorkflowActionService,
    private dragDropService: DragDropService,
  ) {
  }

  public getJointPaper(): joint.dia.Paper {
    if (this.paper === undefined) {
      throw new Error('JointJS paper is undefined');
    }
    return this.paper;
  }

  ngAfterViewInit() {

    this.initializeJointPaper();

    this.handleWindowResize();
    this.handleViewDeleteOperator();
    this.handleCellHighlight();

    this.dragDropService.registerWorkflowEditorDrop(this.WORKFLOW_EDITOR_JOINTJS_ID);

  }

  private initializeJointPaper(): void {
    // get the custom paper options
    let jointPaperOptions = WorkflowEditorComponent.getJointPaperOptions();
    // attach the JointJS graph (model) to the paper (view)
    jointPaperOptions = this.workflowActionService.attachJointPaper(jointPaperOptions);
    // attach the DOM element to the paper
    jointPaperOptions.el = $(`#${this.WORKFLOW_EDITOR_JOINTJS_ID}`);
    // create the JointJS paper
    this.paper = new joint.dia.Paper(jointPaperOptions);

    this.setJointPaperOriginOffset();
    this.setJointPaperDimensions();
  }

  private handleWindowResize(): void {
    // when the window is resized (limit to at most one event every 1000ms)
    Observable.fromEvent(window, 'resize').auditTime(1000).subscribe(
      () => {
        // reset the origin cooredinates
        this.setJointPaperOriginOffset();
        // resize the JointJS paper dimensions
        this.setJointPaperDimensions();
      }
    );
  }

  private handleCellHighlight(): void {
    this.handleHighlightMouseInput();
    this.handleOperatorHightlightEvent();
  }


  /**
   * Handles user mouse down events to trigger logically highlight and unhighlight an operator
   */
  private handleHighlightMouseInput(): void {
    // on user mouse clicks a operator cell, highlight that operator
    Observable.fromEvent<JointPaperEvent>(this.getJointPaper(), 'cell:pointerclick')
      .map(value => value[0])
      .filter(cellView => cellView.model.isElement())
      .subscribe(cellView => this.workflowActionService.getJointGraphWrapper().highlightOperator(cellView.model.id.toString()));

    /**
     * One possible way to unhighlight an operator when user clicks on the blank area,
     *  and bind `blank:pointerdown` event to unhighlight the operator.
     * However, in real life, randomly clicking the blank area happens a lot,
     *  and users are forced to click the operator again to highlight it,
     *  which would make the UI not user-friendly
     */
  }

  private handleOperatorHightlightEvent(): void {
    // handle logical operator highlight / unhighlight events to let JointJS
    //  use our own custom highlighter
    const highlightOptions = {
      name: 'stroke',
      options: {
        attrs: {
          'stroke-width': 1,
          stroke: '#afafaf'
        }
      }
    };

    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .subscribe(value => this.getJointPaper().findViewByModel(value.operatorID).highlight(
        'rect', { highlighter: highlightOptions }
      ));

    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .subscribe(value => this.getJointPaper().findViewByModel(value.operatorID).unhighlight(
        'rect', { highlighter: highlightOptions }
      ));
  }

  /**
   * Modifies the JointJS paper origin coordinates
   *  by shifting it to the left top (minus the x and y offset of the wrapper element)
   * So that elements in JointJS paper have the same coordinates as the actual document.
   *  and we don't have to convert between JointJS coordinates and actual coordinates.
   *
   * Note: attribute `origin` and function `setOrigin` are deprecated and won't work
   *  function `translate` does the same thing
   */
  private setJointPaperOriginOffset(): void {
    const elementOffset = this.getWrapperElementOffset();
    this.getJointPaper().translate(-elementOffset.x, -elementOffset.y);
  }

  /**
   * Sets the size of the JointJS paper to be the exact size of its wrapper element.
   */
  private setJointPaperDimensions(): void {
    const elementSize = this.getWrapperElementSize();
    this.getJointPaper().setDimensions(elementSize.width, elementSize.height);
  }

  /**
   * Handles the event where the Delete button is clicked for an Operator,
   *  and call workflowAction to delete the corresponding operator.
   *
   * JointJS doesn't have delete button built-in with an operator element,
   *  the delete button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `element:delete`.
   * The name of this callback event is registered in `JointUIService.getCustomOperatorStyleAttrs`
   */
  private handleViewDeleteOperator(): void {
    // bind the delete button event to call the delete operator function in joint model action
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'element:delete')
      .map(value => value[0])
      .subscribe(
        elementView => {
          this.workflowActionService.deleteOperator(elementView.model.id.toString());
        }
      );
  }

  /**
   * Gets the width and height of the parent wrapper element
   */
  private getWrapperElementSize(): { width: number, height: number } {
    const width = $('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).width();
    const height = $('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).height();

    if (width === undefined || height === undefined) {
      throw new Error('fail to get Workflow Editor wrapper element size');
    }

    return { width, height };
  }

  /**
   * Gets the document offset coordinates of the wrapper element's top-left corner.
   */
  private getWrapperElementOffset(): { x: number, y: number } {
    const offset = $('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).offset();
    if (offset === undefined) {
      throw new Error('fail to get Workflow Editor wrapper element offset');
    }
    return { x: offset.left, y: offset.top };
  }

  /**
   * Gets our customize options for the JointJS Paper object, which is the JointJS view object responsible for
   *  rendering the workflow cells and handle UI events.
   * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
   */
  private static getJointPaperOptions(): joint.dia.Paper.Options {

    const jointPaperOptions: joint.dia.Paper.Options = {

      // set grid size to 1px (smallest grid)
      gridSize: 1,
      // enable jointjs feature that automatically snaps a link to the closest port with a radius of 30px
      snapLinks: { radius: 30 },
      // disable jointjs default action that can make a link not connect to an operator
      linkPinning: false,
      // provide a validation to determine if two ports could be connected (only output connect to input is allowed)
      validateConnection: validateOperatorConnection,
      // provide a validation to determine if the port where link starts from is an out port
      validateMagnet: validateOperatorMagnet,
      // disable jointjs default action of adding vertexes to the link
      interactive: { vertexAdd: false },
      // set a default link element used by jointjs when user creates a link on UI
      defaultLink: JointUIService.getDefaultLinkCell(),
      // disable jointjs default action that stops propagate click events on jointjs paper
      preventDefaultBlankAction: false,
      // disable jointjs default action that prevents normal right click menu showing up on jointjs paper
      preventContextMenu: false,
    };

    return jointPaperOptions;
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

/**
* This function is provided to JointJS to disallow links starting from an in port.
*
* https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateMagnet
*
* @param cellView
* @param magnet
*/
function validateOperatorMagnet(cellView: joint.dia.CellView, magnet: SVGElement): boolean {
  if (magnet && magnet.getAttribute('port-group') === 'out') {
    return true;
  }
  return false;
}




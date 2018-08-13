import { Component, OnInit, AfterViewInit, AfterContentInit } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

declare var $: JQueryStatic;
import * as joint from 'jointjs';

import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';
import { WorkflowJointGraphService } from '../../service/workflow-graph/model/workflow-joint-graph.service';
import { WorkflowViewEventService } from '../../service/workflow-graph/view-event/workflow-view-event.service';


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
  public readonly WORKFLOW_EDITOR_ID: string = 'texera-workflow-editor-body-id';
  // the jointJS paper
  private paper: joint.dia.Paper = null;

  private onResizedSubject = new Subject<Object>();
  onResized$ = this.onResizedSubject.asObservable();

  constructor(
    private workflowJointGraphService: WorkflowJointGraphService,
    private workflowViewEventService: WorkflowViewEventService,
    private operatorDragDropService: OperatorDragDropService) {
      this.onResized$.subscribe(
        value => {
          const height = $('.texera-workflow-editor-grid-container').height();
          const width = $('.texera-workflow-editor-grid-container').width();
          this.paper.setDimensions(width, height);
        }
      );
  }

  /**
   * Creates the JointJS paper object,
   *  binds it with the DOM element,
   *  and let other services know the paper object
   *  *after* the view is initialized.
   */
  ngAfterViewInit() {
    // create the jointJS paper with custom configurations
    this.paper = this.createJointjsPaper();

    // register the paper to the model
    this.workflowJointGraphService.registerWorkflowPaper(this.paper);

    // register the DOM element ID of this compoenent to drag and drop service
    this.operatorDragDropService.registerWorkflowEditorDrop(this.WORKFLOW_EDITOR_ID);

    // handle jointJS events
    this.bindJointPaperEvents();

  }

  /**
   * Creates a JointJS Paper object, which is the JointJS view object responsible for
   *  rendering the workflow cells and handle UI events.
   *
   * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
   */
  private createJointjsPaper(): joint.dia.Paper {
    // console.log('height: ' + $('#' + this.WORKFLOW_EDITOR_ID).height());
    // console.log('width: ' + $('#' + this.WORKFLOW_EDITOR_ID).width());

    const paper = new joint.dia.Paper({
      el: $('#' + this.WORKFLOW_EDITOR_ID),
      model: this.workflowJointGraphService.uiGraph,
      height: $('#' + this.WORKFLOW_EDITOR_ID).height(),
      width: $('#' + this.WORKFLOW_EDITOR_ID).width(),
      gridSize: 1,
      snapLinks: true,
      linkPinning: false,
      validateConnection: this.validateOperatorConnection,
      preventDefaultBlankAction: false,
      preventContextMenu: false
    });

    return paper;
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
  private validateOperatorConnection(sourceView: joint.dia.CellView, sourceMagnet: SVGElement,
    targetView: joint.dia.CellView, targetMagnet: SVGElement): boolean {
    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') { return false; }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') { return false; }

    return sourceView.id !== targetView.id;
  }

  /**
   * Binds the JointJS Paper events to functions that push the event to the corresponding Subject:
   * The following events happening in JointJS paper are binded:
   *  - mouse pointer down in a Cell
   *  - mouse pointer down in the blank area
   *  - the delete "x" button of an operator is clicked
   *
   */
  private bindJointPaperEvents(): void {
    this.paper.on('cell:pointerdown', (cellView, evt, x, y) => this.handleCellPointerDown(cellView, evt, x, y));

    this.paper.on('element:delete', (cellView, evt, x, y) => this.handleElementDelete(cellView, evt, x, y));

    this.paper.on('blank:pointerdown', (evt: Event, x: number, y: number) => {
      this.workflowViewEventService.pointerDownOnBlankInEditor.next({ 'event': evt, 'x': x, 'y': y });
    });

    const current = this;
    $(window).resize(function() {
      current.onResizedSubject.next();
    });

  }


  /**
   * Handle JointJS paper cell:pointerdown event.
   * Check if the cell is an operator, and push the operator ID of the clicked operator to the corresponding subject.
   * Ignore if the cell is a link (for now, it might be used in the future).
   */
  private handleCellPointerDown(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    // in jointJS, a cell can either be an operator(joint.dia.Element) or a link
    if (cellView.model.isElement()) {
      // an operator cell is pointed down
      // push the operator ID to the subject
      this.workflowViewEventService.operatorSelectedInEditor.next({ operatorID: cellView.model.id.toString() });
    }
  }

  /**
   * Handle JointJS paper element:delete event.
   *
   * element:delete event is not in origin JointJS paper event API.
   * This event is made possible by setting the
   *  '.delete-button' event to 'element:delete' in OperatorUIElementService class.
   *
   */
  private handleElementDelete(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    this.workflowViewEventService.deleteOperatorClickedInEditor.next({ operatorID: cellView.model.id.toString() });
  }

}

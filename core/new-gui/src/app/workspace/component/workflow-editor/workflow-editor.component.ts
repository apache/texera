import { ValidationWorkflowService } from './../../service/validation/validation-workflow.service';
import { DragDropService } from './../../service/drag-drop/drag-drop.service';
import { JointUIService, linkPathStrokeColor } from './../../service/joint-ui/joint-ui.service';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from './../../service/workflow-graph/util/workflow-util.service';
import { Component, AfterViewInit, ElementRef } from '@angular/core';
import { Observable } from 'rxjs/Observable';

import '../../../common/rxjs-operators';
// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from 'jquery';
import * as joint from 'jointjs';

import { ResultPanelToggleService } from '../../service/result-panel-toggle/result-panel-toggle.service';
import { Point, OperatorPredicate, OperatorLink } from '../../types/workflow-common.interface';
import { JointGraphWrapper } from '../../service/workflow-graph/model/joint-graph-wrapper';
import { WorkflowStatusService } from '../../service/workflow-status/workflow-status.service';
import { environment } from './../../../../environments/environment';
import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { ExecutionState, OperatorStatistics, OperatorState } from '../../types/execute-workflow.interface';
import { DynamicSchemaService } from '../../service/dynamic-schema/dynamic-schema.service';
import { Group, LinkInfo, OperatorInfo } from '../../service/workflow-graph/model/operator-group';
import { assertType } from 'src/app/common/util/assert';
import { max, min } from 'lodash';


// argument type of callback event on a JointJS Paper
// which is a 4-element tuple:
// 1. the JointJS View (CellView) of the event
// 2. the corresponding original JQuery Event
// 3. x coordinate, 4. y coordinate
type JointPaperEvent = [joint.dia.CellView, JQuery.Event, number, number];

// argument type of callback event on a JointJS Paper only for blank:pointerdown event
type JointPointerDownEvent = [JQuery.Event, number, number];

// This type represents the copied operator and its information:
// - operator: the copied operator itself, and its properties, etc.
// - position: the position of the copied operator on the workflow graph
// - pastedOperators: a list of operators that are created out of the original operator,
//   including the operator itself.
type CopiedOperator = {
  operator: OperatorPredicate,
  position: Point,
  layer: number,
  pastedOperatorIDs: string[]
};

type CopiedGroup = {
  group: Group,
  position: Point,
  pastedGroupIDs: string[]
};

// jointjs interactive options for enabling and disabling interactivity
// https://resources.jointjs.com/docs/jointjs/v3.2/joint.html#dia.Paper.prototype.options.interactive
const defaultInteractiveOption = { vertexAdd: false, labelMove: false };
const disableInteractiveOption = {
  linkMove: false, labelMove: false, arrowheadMove: false, vertexMove: false, vertexAdd: false, vertexRemove: false
};

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

  public readonly COPY_OFFSET = 20;

  private paper: joint.dia.Paper | undefined;
  private interactive: boolean = true;

  private ifMouseDown: boolean = false;
  private mouseDown: Point | undefined;
  private panOffset: Point = { x: 0, y: 0 };
  private translateLimitX: number[] = [];
  private translateLimitY: number[] = [];

  // dictionary of {operatorID, CopiedOperator} pairs
  private copiedOperators = new Map<string, CopiedOperator>(); // References to operators that will be copied
  private copiedGroups = new Map<string, CopiedGroup>(); // NOT REFERENCES, stores this.copyGroup() copies because groups aren't constant

  constructor(
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private dragDropService: DragDropService,
    private elementRef: ElementRef,
    private resultPanelToggleService: ResultPanelToggleService,
    private validationWorkflowService: ValidationWorkflowService,
    private jointUIService: JointUIService,
    private workflowStatusService: WorkflowStatusService,
    private workflowUtilService: WorkflowUtilService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {

    // bind validation functions to the same scope as component
    // https://stackoverflow.com/questions/38245450/angular2-components-this-is-undefined-when-executing-callback-function
    this.validateOperatorConnection = this.validateOperatorConnection.bind(this);
    this.validateOperatorMagnet = this.validateOperatorMagnet.bind(this);
  }

  public getJointPaper(): joint.dia.Paper {
    if (this.paper === undefined) {
      throw new Error('JointJS paper is undefined');
    }

    return this.paper;
  }

  ngAfterViewInit() {

    this.initializeJointPaper();
    this.handleDisableJointPaperInteractiveness();
    this.handleOperatorValidation();
    this.handlePaperRestoreDefaultOffset();
    this.handlePaperZoom();
    this.handleWindowResize();
    this.handleViewDeleteOperator();
    this.handleCellHighlight();
    this.handleViewDeleteLink();
    this.handleViewCollapseGroup();
    this.handleViewExpandGroup();
    this.handlePaperPan();
    this.handleGroupResize();

    if (environment.executionStatusEnabled) {
      this.handleOperatorStatisticsUpdate();
    }

    this.handlePaperMouseZoom();
    this.handleOperatorSuggestionHighlightEvent();
    this.dragDropService.registerWorkflowEditorDrop(this.WORKFLOW_EDITOR_JOINTJS_ID);

    this.handleElementDelete();
    this.handleElementSelectAll();
    this.handleElementCopy();
    this.handleOperatorCut();
    this.handleOperatorPaste();

    this.handleLinkCursorHover();
    if (environment.linkBreakpointEnabled) {
      this.handleLinkBreakpoint();
    }
  }


  private initializeJointPaper(): void {
    // get the custom paper options
    let jointPaperOptions = this.getJointPaperOptions();
    // attach the JointJS graph (model) to the paper (view)
    jointPaperOptions = this.workflowActionService.attachJointPaper(jointPaperOptions);
    // attach the DOM element to the paper
    jointPaperOptions.el = jQuery(`#${this.WORKFLOW_EDITOR_JOINTJS_ID}`);
    // create the JointJS paper
    this.paper = new joint.dia.Paper(jointPaperOptions);

    this.setJointPaperOriginOffset();
    this.setJointPaperDimensions();
  }

  private handleDisableJointPaperInteractiveness(): void {
    this.workflowActionService.getWorkflowModificationEnabledStream().subscribe(enabled => {
      if (enabled) {
        this.interactive = true;
        this.getJointPaper().setInteractivity(defaultInteractiveOption);
      } else {
        this.interactive = false;
        this.getJointPaper().setInteractivity(disableInteractiveOption);
      }
    });
  }

  /**
   * This method subscribe to workflowStatusService's status stream
   * for Each processStatus that has been emited
   *    1. enable operatorStatusTooltipDisplay because tooltip will not be empty
   *    2. for each operator in current texeraGraph:
   *        - find its Statistics in processStatus, thrown an error if not found
   *        - generate its corresponding tooltip's id
   *        - pass the tooltip id and Statistics to jointUIService
   *          the specific tooltip content will be updated
   *          - if operator is in a group, save statistics in group's operatorInfo
   *    3. Whenever a group is expanded
   *        - for each operatorInfo, display statistics if there are some saved.
   */
  private handleOperatorStatisticsUpdate(): void {
    this.workflowStatusService.getStatusUpdateStream().subscribe(status => {
      Object.keys(status).forEach(operatorID => {
        if (!this.workflowActionService.getTexeraGraph().hasOperator(operatorID)) {
          throw new Error(`operator ${operatorID} does not exist`);
        }
        if (this.executeWorkflowService.getExecutionState().state === ExecutionState.Recovering) {
          status[operatorID] = {
            ...status[operatorID],
            operatorState: OperatorState.Recovering,
          };
        }
        // if operator is part of a group, find it
        const parentGroup = this.workflowActionService.getOperatorGroup().getGroupByOperator(operatorID);

        // if operator is not in a group or in a group that isn't collapsed, it is okay to draw statistics on it
        if (!parentGroup || !parentGroup.collapsed) {
          this.jointUIService.changeOperatorStatistics(this.getJointPaper(), operatorID, status[operatorID]);
        }

        // if operator is in a group, write statistics to the group's operatorInfo
        // so that it can be restored if the group is collapsed and expanded.
        if (parentGroup) {
          const operatorInfo = parentGroup.operators.get(operatorID);
          assertType<OperatorInfo>(operatorInfo);
          operatorInfo.statistics = status[operatorID];
        }
      });
    });

    // listen for group expanding, and redraw operator statistics if they exist
    this.workflowActionService.getOperatorGroup().getGroupExpandStream().subscribe(group => {
      group.operators.forEach((operatorInfo, operatorID) => {
        if (operatorInfo.statistics) {
          this.jointUIService.changeOperatorStatistics(this.getJointPaper(), operatorID, operatorInfo.statistics);
        }
      });
    });

    this.executeWorkflowService.getExecutionStateStream().subscribe(event => {
      if (event.previous.state === ExecutionState.Recovering) {
        let operatorState: OperatorState;
        if (event.current.state === ExecutionState.Paused) {
          operatorState = OperatorState.Paused;
        } else if (event.current.state === ExecutionState.Completed) {
          operatorState = OperatorState.Completed;
        } else if (event.current.state === ExecutionState.Running) {
          operatorState = OperatorState.Running;
        } else {
          throw new Error('unknown state transition from recovering state: ' + event.current.state);
        }
        this.workflowActionService.getTexeraGraph().getAllOperators().forEach(op => {
          this.jointUIService.changeOperatorState(this.getJointPaper(), op.operatorID, operatorState);
        });
      }
    });
  }

  /**
   * Handles restore offset default event by translating jointJS paper
   *  back to original position
   */
  private handlePaperRestoreDefaultOffset(): void {
    this.workflowActionService.getJointGraphWrapper().getRestorePaperOffsetStream()
      .subscribe(newOffset => {
        this.panOffset = newOffset;
        this.getJointPaper().translate(
          (- this.getWrapperElementOffset().x + newOffset.x),
          (- this.getWrapperElementOffset().y + newOffset.y)
        );
      });
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.workflowActionService.getJointGraphWrapper().getWorkflowEditorZoomStream().subscribe(newRatio => {
      this.getJointPaper().scale(newRatio, newRatio);
    });
  }

  /**
   * Handles zoom events when user slides the mouse wheel.
   *
   * The first filter will removes all the mousewheel events that are undefined
   * The second filter will remove all the mousewheel events that are
   *  from different components
   *
   * From the mousewheel event:
   *  1. when delta Y is negative, the wheel is scrolling down, so
   *      the jointJS paper will zoom in.
   *  2. when delta Y is positive, the wheel is scrolling up, so the
   *      jointJS paper will zoom out.
   */
  private handlePaperMouseZoom(): void {
    Observable.fromEvent<WheelEvent>(document, 'mousewheel')
      .filter(event => event !== undefined)
      .filter(event => this.elementRef.nativeElement.contains(event.target))
      .forEach(event => {
        if (event.metaKey || event.ctrlKey) {
          if (event.deltaY < 0) {
            // if zoom ratio already at minimum, do not zoom out.
            if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMin()) {
              return;
            }
            this.workflowActionService.getJointGraphWrapper()
              .setZoomProperty(this.workflowActionService.getJointGraphWrapper().getZoomRatio() - JointGraphWrapper.ZOOM_MOUSEWHEEL_DIFF);
          } else {
            // if zoom ratio already at maximum, do not zoom in.
            if (this.workflowActionService.getJointGraphWrapper().isZoomRatioMax()) {
              return;
            }
            this.workflowActionService.getJointGraphWrapper()
              .setZoomProperty(this.workflowActionService.getJointGraphWrapper().getZoomRatio() + JointGraphWrapper.ZOOM_MOUSEWHEEL_DIFF);
          }
        }
      });
  }

  /**
   * This method gets all operators' position and
   * gets the limits of translating.
   */
  private getTranslateLimit(): { minX: number, maxX: number, minY: number, maxY: number } {
    // reset the position array
    this.translateLimitX = [];
    this.translateLimitY = [];
    // get all operators' positions
    this.workflowActionService.getTexeraGraph().getAllOperators()
      .filter(
        op =>
          !this.workflowActionService.getOperatorGroup().getGroupByOperator(op.operatorID) &&
          !this.workflowActionService.getOperatorGroup().getGroupByOperator(op.operatorID)?.collapsed
      )
      .forEach(op => {
        const position = this.workflowActionService.getJointGraphWrapper().getElementPosition(op.operatorID);
        if (!this.translateLimitX.includes(position.x)) {
          this.translateLimitX.push(position.x);
        }
        if (!this.translateLimitY.includes(position.y)) {
          this.translateLimitY.push(position.y);
        }
      });
    // if no operator, set the default limit
    const minX = min(this.translateLimitX) ?? 700;
    const maxX = max(this.translateLimitX) ?? 700;
    const minY = min(this.translateLimitY) ?? 300;
    const maxY = max(this.translateLimitY) ?? 300;
    return { minX, maxX, minY, maxY };
  }

  /**
   * This method checks whether the operator is out of bound.
   */
  private checkBouding(limitx: number[], limity: number[], translateLimit: any): void {
    const elementSize = this.getWrapperElementSize();
    // check if operator out of right bound after WrapperElement changes its size
    if (this.getJointPaper().translate().tx + translateLimit.minX + 65 > elementSize.width) {
      this.getJointPaper().translate(
        limitx[0] - 1, this.getJointPaper().translate().ty
      );
      this.panOffset = {
        x: this.getWrapperElementOffset().x + limitx[0] - 1,
        y: this.getWrapperElementOffset().y + this.getJointPaper().translate().ty
      };
      // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
      this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
    }
    // check left bound
    if (this.getJointPaper().translate().tx < -translateLimit.maxX) {
      this.getJointPaper().translate(
        -translateLimit.maxX, this.getJointPaper().translate().ty
      );
      this.panOffset = {
        x: this.getWrapperElementOffset().x + (-translateLimit.maxX),
        y: this.getWrapperElementOffset().y + this.getJointPaper().translate().ty
      };
      // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
      this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
    }
    // check lower bound
    if (this.getJointPaper().translate().ty + translateLimit.minY + 70 > elementSize.height) {
      this.getJointPaper().translate(
        this.getJointPaper().translate().tx, limity[0] - 1
      );
      this.panOffset = {
        x: this.getWrapperElementOffset().x + this.getJointPaper().translate().tx,
        y: this.getWrapperElementOffset().y + limity[0] - 1
      };
      // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
      this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
    }
    // check upper bound
    if (this.getJointPaper().translate().ty < -translateLimit.maxY) {
      this.getJointPaper().translate(
        this.getJointPaper().translate().tx, -translateLimit.maxY
      );
      this.panOffset = {
        x: this.getWrapperElementOffset().x + this.getJointPaper().translate().tx,
        y: this.getWrapperElementOffset().y + (-translateLimit.maxY)
      };
      // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
      this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
    }
  }

  /**
   * This method handles user mouse drag events to pan JointJS paper.
   *
   * This method will listen to 4 events to implement the pan feature
   *   1. pointerdown event in the JointJS paper to start panning
   *   2. mousemove event on the document to change the offset of the paper
   *   3. pointerup event in the JointJS paper to stop panning
   *   4. mousewheel event on the document to start panning
   */
  private handlePaperPan(): void {

    Observable.fromEvent<WheelEvent>(document, 'mousewheel')
      .filter(event => event !== undefined)
      .filter(event => this.elementRef.nativeElement.contains(event.target))
      .forEach(event => {
        // calculate the limit of translate
        const translateLimit = this.getTranslateLimit();
        const elementSize = this.getWrapperElementSize();
        const limitx = [elementSize.width - 65 - translateLimit.minX, -translateLimit.maxX];
        const limity = [elementSize.height - 70 - translateLimit.minY, -translateLimit.maxY];
        this.checkBouding(limitx, limity, translateLimit);

        // do paper movement
        const translatex = this.getJointPaper().translate().tx - event.deltaX;
        const translatey = this.getJointPaper().translate().ty - event.deltaY;
        const conditionx = translatex > limitx[1] && translatex < limitx[0];
        const conditiony = translatey > limity[1] && translatey < limity[0];

        // set panOffset
        this.panOffset = {
          x: this.getWrapperElementOffset().x + this.getJointPaper().translate().tx,
          y: this.getWrapperElementOffset().y + this.getJointPaper().translate().ty
        };
        if (conditionx && conditiony) {
          this.getJointPaper().translate(
            translatex, translatey
          );
          // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
          this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
        }
      });

    // pointer down event to start the panning, this will record the original paper offset
    Observable.fromEvent<JointPointerDownEvent>(this.getJointPaper(), 'blank:pointerdown')
      .subscribe(
        coordinate => {
          this.mouseDown = { x: coordinate[1], y: coordinate[2] };
          this.ifMouseDown = true;
        }
      );

    /* mousemove event to move paper, this will calculate the new coordinate based on the
     *  starting coordinate, the mousemove offset, and the current zoom ratio.
     *  To move the paper based on the new coordinate, this will translate the paper by calling
     *  the JointJS method .translate() to move paper's offset.
     */

    Observable.fromEvent<MouseEvent>(document, 'mousemove')
      .filter(() => this.ifMouseDown === true)
      .filter(() => this.mouseDown !== undefined)
      .forEach(coordinate => {

        if (this.mouseDown === undefined) {
          throw new Error('Error: Mouse down is undefined after the filter');
        }

        // calculate the pan offset between user click on the mouse and then release the mouse, including zooming value.
        this.panOffset = {
          x: coordinate.x - this.mouseDown.x * this.workflowActionService.getJointGraphWrapper().getZoomRatio(),
          y: coordinate.y - this.mouseDown.y * this.workflowActionService.getJointGraphWrapper().getZoomRatio()
        };
        // calculate the limit of translate
        const translateLimit = this.getTranslateLimit();
        const elementSize = this.getWrapperElementSize();
        const limitx = [elementSize.width - 65 - translateLimit.minX, -translateLimit.maxX];
        const limity = [elementSize.height - 70 - translateLimit.minY, -translateLimit.maxY];
        this.checkBouding(limitx, limity, translateLimit);
        // do paper movement.
        const translatex = - this.getWrapperElementOffset().x + this.panOffset.x;
        const translatey = - this.getWrapperElementOffset().y + this.panOffset.y;
        const conditionx = translatex > limitx[1] && translatex < limitx[0];
        const conditiony = translatey > limity[1] && translatey < limity[0];
        if (conditionx && conditiony) {
          this.getJointPaper().translate(
            (- this.getWrapperElementOffset().x + this.panOffset.x),
            (- this.getWrapperElementOffset().y + this.panOffset.y)
          );
          // pass offset to the joint graph wrapper to make operator be at the right location during drag-and-drop.
          this.workflowActionService.getJointGraphWrapper().setPanningOffset(this.panOffset);
        }
      });

    // This observable captures the drop event to stop the panning
    Observable.fromEvent<JointPaperEvent>(this.getJointPaper(), 'blank:pointerup')
      .subscribe(() => this.ifMouseDown = false);
  }

  /**
   * This is the handler for window resize event
   * When the window is resized, trigger an event to set papaer offset and dimension
   *  and limit the event to at most one every 30ms.
   *
   * When user open the result panel and resize, the paper will resize to the size relative
   *  to the result panel, therefore we also need to listen to the event from opening
   *  and closing of the result panel.
   */
  private handleWindowResize(): void {
    // when the window is resized (limit to at most one event every 30ms).
    Observable.merge(
      Observable.fromEvent(window, 'resize').auditTime(30),
      this.resultPanelToggleService.getToggleChangeStream().auditTime(30)
    ).subscribe(
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
    this.handleElementHightlightEvent();
  }


  /**
   * Handles user mouse down events to trigger logically highlight and unhighlight an operator or group.
   * If user clicks the operator/group while pressing the shift key, multiselect mode is turned on.
   * When pressing the shift key, user can unhighlight a highlighted operator/group by clicking on it.
   * User can also unhighlight all operators and groups by clicking on the blank area of the graph.
   */
  private handleHighlightMouseInput(): void {
    // on user mouse clicks an operator/group cell, highlight that operator/group
    // operator status tooltips should never be highlighted
    Observable.fromEvent<JointPaperEvent>(this.getJointPaper(), 'cell:pointerdown')
      // event[0] is the JointJS CellView; event[1] is the original JQuery Event
      .filter(event => event[0].model.isElement())
      .filter(event => this.workflowActionService.getTexeraGraph().hasOperator(event[0].model.id.toString()) ||
        this.workflowActionService.getOperatorGroup().hasGroup(event[0].model.id.toString()))
      .subscribe(event => {
        // multiselect mode on if holding shift
        this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(<boolean>event[1].shiftKey);

        const elementID = event[0].model.id.toString();
        const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();

        if (event[1].shiftKey) { // if in multiselect toggle highlights on click
          if (highlightedOperatorIDs.includes(elementID)) {
            this.workflowActionService.getJointGraphWrapper().unhighlightOperators(elementID);
          } else if (highlightedGroupIDs.includes(elementID)) {
            this.workflowActionService.getJointGraphWrapper().unhighlightGroups(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightOperators(elementID);
          } else if (this.workflowActionService.getOperatorGroup().hasGroup(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightGroups(elementID);
          }
        } else { // else only highlight a single operator or group
          if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightOperators(elementID);
          } else if (this.workflowActionService.getOperatorGroup().hasGroup(elementID)) {
            this.workflowActionService.getJointGraphWrapper().highlightGroups(elementID);
          }
        }
      });

    // on user mouse clicks on blank area, unhighlight all operators and groups
    Observable.fromEvent<JointPaperEvent>(this.getJointPaper(), 'blank:pointerdown')
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        const highlightedLinkIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs();
        this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...highlightedOperatorIDs);
        this.workflowActionService.getJointGraphWrapper().unhighlightGroups(...highlightedGroupIDs);
        this.workflowActionService.getJointGraphWrapper().unhighlightLinks(...highlightedLinkIDs);
      });
  }

  private handleElementHightlightEvent(): void {
    // handle logical operator and group highlight / unhighlight events to let JointJS
    //  use our own custom highlighter
    const highlightOptions = {
      name: 'stroke',
      options: {
        attrs: {
          'stroke-width': 2,
          stroke: '#4A95FF'
        }
      }
    };

    // highlight on OperatorHighlightStream or GroupHighlightStream
    Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupHighlightStream()
    ).subscribe(elementIDs => elementIDs.forEach(elementID =>
      this.getJointPaper().findViewByModel(elementID).highlight(
        'rect', { highlighter: highlightOptions }
      )));

    // unhighlight on OperatorUnhighlightStream or GroupUnhighlightStream
    Observable.merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointGroupUnhighlightStream()
    ).subscribe(elementIDs => elementIDs.forEach(elementID =>
      this.getJointPaper().findViewByModel(elementID).unhighlight(
        'rect', { highlighter: highlightOptions }
      )));
  }

  private handleOperatorSuggestionHighlightEvent(): void {
    const highlightOptions = {
      name: 'stroke',
      options: {
        attrs: {
          'stroke-width': 5,
          stroke: '#551A8B70'
        }
      }
    };

    this.dragDropService.getOperatorSuggestionHighlightStream()
      .subscribe(value => this.getJointPaper().findViewByModel(value).highlight('rect',
        { highlighter: highlightOptions }
      ));

    this.dragDropService.getOperatorSuggestionUnhighlightStream()
      .subscribe(value => this.getJointPaper().findViewByModel(value).unhighlight('rect',
        { highlighter: highlightOptions }
      ));
  }


  /**
   * Modifies the JointJS paper origin coordinates
   *  by shifting it to the left top (minus the x and y offset of the wrapper element)
   * So that elements in JointJS paper have the same coordinates as the actual document.
   *  and we don't have to convert between JointJS coordinates and actual coordinates.
   *
   * panOffset is added to this translation to consider the situation that the paper
   *  has been panned by the user previously.
   *
   * Note: attribute `origin` and function `setOrigin` are deprecated and won't work
   *  function `translate` does the same thing
   */
  private setJointPaperOriginOffset(): void {
    const elementOffset = this.getWrapperElementOffset();
    this.getJointPaper().translate(-elementOffset.x + this.panOffset.x, -elementOffset.y + this.panOffset.y);
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
      .filter(value => this.interactive)
      .map(value => value[0])
      .subscribe(
        elementView => {
          this.workflowActionService.deleteOperator(elementView.model.id.toString());
        }
      );
  }

  /**
   * Handles the event where the Delete button is clicked for a Link,
   *  and call workflowAction to delete the corresponding link.
   *
   * We handle link deletion on our own by defining a custom markup.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `tool:remove`.
   */
  private handleViewDeleteLink(): void {
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'tool:remove')
      .filter(value => this.interactive)
      .map(value => value[0])
      .subscribe(elementView => {
        this.workflowActionService.deleteLinkWithID(elementView.model.id.toString());
      }
      );
  }

  /**
   * Handles the event where the Collapse button is clicked for a Group,
   *  and call groupOperator to collapse the corresponding group.
   *
   * JointJS doesn't have collapse button built-in with a group element,
   *  the collapse button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for collapse a group,
   *  we need to handle the callback event `element:collapse`.
   * The name of this callback event is registered in `JointUIService.getCustomGroupStyleAttrs`
   */
  private handleViewCollapseGroup(): void {
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'element:collapse')
      .map(value => value[0])
      .subscribe(
        elementView => {
          const groupID = elementView.model.id.toString();
          this.workflowActionService.collapseGroups(groupID);
        }
      );
  }

  /**
   * Handles the event where the Expand button is clicked for a Group,
   *  and call groupOperator to expand the corresponding group.
   *
   * JointJS doesn't have expand button built-in with a group element,
   *  the expand button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for expand a group,
   *  we need to handle the callback event `element:expand`.
   * The name of this callback event is registered in `JointUIService.getCustomGroupStyleAttrs`
   */
  private handleViewExpandGroup(): void {
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'element:expand')
      .map(value => value[0])
      .subscribe(
        elementView => {
          const groupID = elementView.model.id.toString();
          this.workflowActionService.expandGroups(groupID);
        }
      );
  }

  /**
   * if the operator is valid , the border of the box will be default
   */
  private handleOperatorValidation(): void {
    this.validationWorkflowService.getOperatorValidationStream()
      .subscribe(value => {
        if (!this.workflowActionService.getOperatorGroup().getGroupByOperator(value.operatorID)?.collapsed) {
          this.jointUIService.changeOperatorColor(this.getJointPaper(), value.operatorID, value.validation.isValid);
        }
      });
  }

  /**
   * Handles events that cause a group's size to change (collapse, expand, or
   * resize), and hides or repositions the group's collapse/expand button.
   *
   * Since the collapse button's position is relative to a group's width,
   * resizing the group will cause the button to be out of place.
   */
  private handleGroupResize(): void {
    this.workflowActionService.getOperatorGroup().getGroupCollapseStream().subscribe(group => {
      this.jointUIService.hideGroupCollapseButton(this.getJointPaper(), group.groupID);
    });

    this.workflowActionService.getOperatorGroup().getGroupExpandStream().subscribe(group => {
      this.jointUIService.hideGroupExpandButton(this.getJointPaper(), group.groupID);
    });

    this.workflowActionService.getOperatorGroup().getGroupResizeStream().subscribe(value => {
      this.jointUIService.repositionGroupCollapseButton(this.getJointPaper(), value.groupID, value.width);
    });
  }

  /**
   * Gets the width and height of the parent wrapper element
   */
  private getWrapperElementSize(): { width: number, height: number } {
    const width = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).width();
    const height = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).height();

    if (width === undefined || height === undefined) {
      throw new Error('fail to get Workflow Editor wrapper element size');
    }

    return { width, height };
  }

  /**
   * Gets the document offset coordinates of the wrapper element's top-left corner.
   */
  private getWrapperElementOffset(): { x: number, y: number } {
    const offset = jQuery('#' + this.WORKFLOW_EDITOR_JOINTJS_WRAPPER_ID).offset();
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
  private getJointPaperOptions(): joint.dia.Paper.Options {

    const jointPaperOptions: joint.dia.Paper.Options = {
      // enable jointjs feature that automatically snaps a link to the closest port with a radius of 30px
      snapLinks: { radius: 40 },
      // disable jointjs default action that can make a link not connect to an operator
      linkPinning: false,
      // provide a validation to determine if two ports could be connected (only output connect to input is allowed)
      validateConnection: this.validateOperatorConnection,
      // provide a validation to determine if the port where link starts from is an out port
      validateMagnet: this.validateOperatorMagnet,
      // marks all the available magnets or elements when a link is dragged
      markAvailable: true,
      // disable jointjs default action of adding vertexes to the link
      interactive: defaultInteractiveOption,
      // set a default link element used by jointjs when user creates a link on UI
      defaultLink: JointUIService.getDefaultLinkCell(),
      // disable jointjs default action that stops propagate click events on jointjs paper
      preventDefaultBlankAction: false,
      // disable jointjs default action that prevents normal right click menu showing up on jointjs paper
      preventContextMenu: false,
      // draw dots in the background of the paper
      drawGrid: { name: 'fixedDot', args: { color: 'black', scaleFactor: 8, thickness: 1.2 } },
      // set grid size
      gridSize: 2,
    };

    return jointPaperOptions;
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
  private validateOperatorConnection(sourceView: joint.dia.CellView, sourceMagnet: SVGElement | undefined,
    targetView: joint.dia.CellView, targetMagnet: SVGElement | undefined): boolean {

    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') { return false; }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') { return false; }

    const sourceCellID = sourceView.model.id.toString();
    const sourcePortID = sourceMagnet?.getAttribute('port');
    const targetCellID = targetView.model.id.toString();
    const targetPortID = targetMagnet?.getAttribute('port');

    // cannot connect to itself
    if (sourceCellID === targetCellID) {
      return false;
    }

    // must connect to ports
    if (!sourcePortID || !targetPortID) {
      return false;
    }

    // must connect to operators
    if (!this.workflowActionService.getTexeraGraph().hasOperator(sourceCellID)
      || !this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)) {
      return false;
    }


    // find all the links that are connected to the target port, except the link we are checking now
    const connectedLinksToTargetPort = this.workflowActionService.getTexeraGraph().getAllLinks()
      // connect to the same target operator and port
      .filter(link => link.target.operatorID === targetCellID && link.target.portID === targetPortID)
      // but not the link we are checking right now, jointJS sometimes invoke the function on already connected link
      .filter(link => !(link.source.operatorID === sourceCellID && link.source.portID === sourcePortID));


    let allowMultiInput = false;
    if (this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)) {
      const portIndex = this.workflowActionService.getTexeraGraph().getOperator(targetCellID)
        .inputPorts.findIndex(p => p.portID === targetPortID);
      if (portIndex > 0) {
        const portInfo = this.dynamicSchemaService.getDynamicSchema(targetCellID).additionalMetadata.inputPorts[portIndex];
        allowMultiInput = portInfo.allowMultiInputs ?? false;
      }
    }

    if (connectedLinksToTargetPort.length > 0 && !allowMultiInput) {
      return false;
    }

    return true;
  }


  /**
  * This function is provided to JointJS to disallow links starting from an in port.
  *
  * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateMagnet
  *
  * @param cellView
  * @param magnet
  */
  private validateOperatorMagnet(cellView: joint.dia.CellView, magnet: SVGElement): boolean {
    if (magnet && magnet.getAttribute('port-group') === 'out') {
      return true;
    }
    return false;
  }

  /**
   * Deletes currently highlighted operators and groups when user presses the delete key.
   * When the delete key is clicked to INPUT or TEXTAREA fields, operator should not be deleted
   */
  private handleElementDelete(): void {
    Observable.fromEvent<KeyboardEvent>(document, 'keydown')
      .filter(event => this.interactive)
      .filter(event => (<HTMLElement>event.target).nodeName !== 'INPUT')
      .filter(event => (<HTMLElement>event.target).nodeName !== 'TEXTAREA')
      .filter(event => event.key === 'Backspace' || event.key === 'Delete')
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
      });
  }

  /**
   * Highlight all operators and groups on the graph when user presses command/ctrl + A.
   */
  private handleElementSelectAll(): void {
    Observable.fromEvent<KeyboardEvent>(document, 'keydown')
      .filter(event => (<HTMLElement>event.target).nodeName !== 'INPUT')
      .filter(event => (event.metaKey || event.ctrlKey) && event.key === 'a')
      .subscribe(event => {
        event.preventDefault();
        const allOperators = this.workflowActionService.getTexeraGraph().getAllOperators().map(operator => operator.operatorID)
          .filter(operatorID => !this.workflowActionService.getOperatorGroup().getGroupByOperator(operatorID)?.collapsed);
        const allGroups = this.workflowActionService.getOperatorGroup().getAllGroups().map(group => group.groupID);
        this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(allOperators.length + allGroups.length > 1);
        this.workflowActionService.getJointGraphWrapper().highlightOperators(...allOperators);
        this.workflowActionService.getJointGraphWrapper().highlightGroups(...allGroups);
      });
  }

  /**
   * Caches the currently highlighted operators' info when user
   * triggers the copy event (i.e. presses command/ctrl + c on
   * keyboard or selects copy option from the browser menu).
   */
  private handleElementCopy(): void {
    Observable.fromEvent<ClipboardEvent>(document, 'copy')
      .filter(event => (<HTMLElement>event.target).nodeName !== 'INPUT')
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        if (highlightedOperatorIDs.length > 0 || highlightedGroupIDs.length > 0) {
          this.clearCopiedElements();
          this.saveHighlighedElements();
        }
      });
  }

  /**
   * Caches the currently highlighted operators' info and deletes it
   * when user triggers the cut event (i.e. presses command/ctrl + x
   * on keyboard or selects cut option from the browser menu).
   */
  private handleOperatorCut(): void {
    Observable.fromEvent<ClipboardEvent>(document, 'cut')
      .filter(event => this.interactive)
      .filter(event => (<HTMLElement>event.target).nodeName !== 'INPUT')
      .subscribe(() => {
        const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
        const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();
        if (highlightedOperatorIDs.length > 0 || highlightedGroupIDs.length > 0) {
          this.clearCopiedElements();
          this.saveHighlighedElements();
          this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs, [], highlightedGroupIDs);
        }
      });
  }

  /**
   * clears copiedOperators and copiedGroups
   */
  private clearCopiedElements(): void {
    this.copiedOperators.clear();
    this.copiedGroups.clear();
  }

  /**
   * saves highlighted elements to copiedOperators and copiedGroups
   */
  private saveHighlighedElements(): void {

    const highlightedOperatorIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const highlightedGroupIDs = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedGroupIDs();

    highlightedOperatorIDs.forEach(operatorID => this.saveOperatorInfo(operatorID, false));
    highlightedGroupIDs.forEach(groupID => {
      this.saveGroupInfo(groupID);

      const copiedGroup = this.workflowActionService.getOperatorGroup().getGroup(groupID);
      assertType<Group>(copiedGroup);
      // do no copy operators that would be copied along with their groups (to avoid double counting)
      copiedGroup.operators.forEach((operatorInfo, operatorID) => this.deleteOperatorInfo(operatorID));
    });
  }

  /**
   * Utility function to cache the operator's info.
   * @param operatorID
   * @param includeOperator
   */
  private saveOperatorInfo(operatorID: string, includeOperator: boolean): void {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    if (operator) {
      const position = this.workflowActionService.getJointGraphWrapper().getElementPosition(operatorID);
      const layer = this.workflowActionService.getJointGraphWrapper().getCellLayer(operatorID);
      const pastedOperators = includeOperator ? [operatorID] : [];
      this.copiedOperators.set(operatorID, { operator, position, layer, pastedOperatorIDs: pastedOperators });
    }
  }

  /**
   * removes operator from copiedOperators
   */
  private deleteOperatorInfo(operatorID: string) {
    this.copiedOperators.delete(operatorID);
  }

  /**
   * saves copy of group to copiedGroups
   */
  private saveGroupInfo(groupID: string): void {
    this.copiedGroups.set(groupID,
      {
        group: this.copyGroup(this.workflowActionService.getOperatorGroup().getGroup(groupID)),
        position: this.workflowActionService.getJointGraphWrapper().getElementPosition(groupID),
        pastedGroupIDs: []
      }
    );
  }

  /**
   * Pastes the cached operators onto the workflow graph and highlights them
   * when user triggers the paste event (i.e. presses command/ctrl + v on
   * keyboard or selects paste option from the browser menu).
   */
  private handleOperatorPaste(): void {
    Observable.fromEvent<ClipboardEvent>(document, 'paste')
      .filter(event => this.interactive)
      .filter(event => (<HTMLElement>event.target).nodeName !== 'INPUT')
      .subscribe(() => {
        // if there is something to paste
        if (this.copiedOperators.size > 0 || this.copiedGroups.size > 0) {
          const operatorsAndPositions: { op: OperatorPredicate, pos: Point }[] = [];
          const links: OperatorLink[] = [];
          const groups: Group[] = [];
          const positions: Point[] = [];

          // sort operators by layer
          this.copiedOperators = new Map<string, CopiedOperator>(Array.from(this.copiedOperators)
            .sort((first, second) => first[1].layer - second[1].layer));

          // make copies of each operator, and calculate their positions when pasted
          this.copiedOperators.forEach((copiedOperator, operatorID) => {
            const newOperator = this.copyOperator(copiedOperator.operator);
            const newOperatorPosition = this.calcOperatorPosition(newOperator.operatorID, operatorID, positions);
            operatorsAndPositions.push({ op: newOperator, pos: newOperatorPosition });
            positions.push(newOperatorPosition);
          });

          // make copies of each group, push each group's internal operators and calculated positions to operatorsAndPositions
          this.copiedGroups.forEach((copiedGroup, groupID) => {
            const newGroup = this.copyGroup(copiedGroup.group);

            const oldPosition = copiedGroup.position;
            const newPosition = this.calcGroupPosition(newGroup.groupID, groupID, positions);
            positions.push(newPosition);

            // delta between old position and new to apply to the copied group's operators
            const delta = {
              x: newPosition.x - oldPosition.x,
              y: newPosition.y - oldPosition.y
            };

            newGroup.operators.forEach((operatorInfo, operatorID) => {
              operatorInfo.position.x += delta.x;
              operatorInfo.position.y += delta.x;

              operatorsAndPositions.push({
                op: operatorInfo.operator,
                pos: operatorInfo.position
              });
            });

            // add links from group to list of all links to be added
            newGroup.links.forEach((linkInfo, operatorID) => {
              links.push(linkInfo.link);
            });

            // add group to list of all groups to be added
            groups.push(newGroup);
          });

          // actually add all operators, links, groups to the workflow
          this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, links, groups, new Map());
        }
      });
  }

  /**
   * Utility function to create a new operator that contains same
   * info as the copied operator.
   * @param operator
   */
  private copyOperator(operator: OperatorPredicate): OperatorPredicate {
    const operatorID = operator.operatorType + '-' + this.workflowUtilService.getRandomUUID();
    const operatorType = operator.operatorType;
    const operatorProperties = operator.operatorProperties;
    const inputPorts = operator.inputPorts;
    const outputPorts = operator.outputPorts;
    const showAdvanced = operator.showAdvanced;
    return { operatorID, operatorType, operatorProperties, inputPorts, outputPorts, showAdvanced };
  }

  private copyGroup(group: Group) {

    // copying a group involves copying all the operators and links inside it (inlinks and outlinks not copied)

    const operatorMap = new Map<string, string>();

    const operators = new Map(Array.from(group.operators.values()).map(operatorInfo => {
      const newOperatorInfo = {
        operator: this.copyOperator(operatorInfo.operator),
        position: {
          x: operatorInfo.position.x,
          y: operatorInfo.position.y,
        },
        layer: operatorInfo.layer,
      };

      operatorMap.set(operatorInfo.operator.operatorID, newOperatorInfo.operator.operatorID);
      return [newOperatorInfo.operator.operatorID, newOperatorInfo];
    }));

    const links = new Map<string, LinkInfo>(Array.from(group.links.values()).map(linkInfo => {
      const sourceID = operatorMap.get(linkInfo.link.source.operatorID);
      const targetID = operatorMap.get(linkInfo.link.target.operatorID);
      assertType<string>(sourceID);
      assertType<string>(targetID);

      const newLinkInfo = {
        link: {
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: {
            operatorID: sourceID,
            portID: linkInfo.link.source.portID,
          },
          target: {
            operatorID: targetID,
            portID: linkInfo.link.target.portID
          }
        },
        layer: linkInfo.layer
      };

      return [newLinkInfo.link.linkID, newLinkInfo];
    }));

    // this is a Group, but not casted as one to avoid readonly restrictions
    // probably should refactor group to be a class with a proper constructor, since future refactors to the group template could break this
    return {
      groupID: this.workflowUtilService.getGroupRandomUUID(),
      operators: operators,
      links: links,
      inLinks: [],
      outLinks: [],
      collapsed: group.collapsed
    };
  }

  /**
   * Utility function to calculate the position to paste the operator.
   * If a previously pasted operator is moved or deleted, the operator will be
   * pasted to the emptied position. Otherwise, it will be pasted to a position
   * that's non-overlapping and calculated according to the copy operator offset.
   * @param newOperatorID
   * @param copiedOperatorID
   * @param positions
   */
  private calcOperatorPosition(newOperatorID: string, copiedOperatorID: string, positions: Point[]): Point {
    let i, position;
    const copiedOperator = this.copiedOperators.get(copiedOperatorID);
    if (!copiedOperator) {
      throw Error(`Internal error: cannot find ${copiedOperatorID} in copied operators`);
    }
    const pastedOperators = copiedOperator.pastedOperatorIDs;
    for (i = 0; i < pastedOperators.length; ++i) {
      position = {
        x: copiedOperator.position.x + i * this.COPY_OFFSET,
        y: copiedOperator.position.y + i * this.COPY_OFFSET
      };
      if (!positions.includes(position) && (!this.workflowActionService.getTexeraGraph().hasOperator(pastedOperators[i]) ||
        this.workflowActionService.getOperatorGroup().getOperatorPositionByGroup(pastedOperators[i]).x !== position.x ||
        this.workflowActionService.getOperatorGroup().getOperatorPositionByGroup(pastedOperators[i]).y !== position.y)) {
        pastedOperators[i] = newOperatorID;
        return this.getNonOverlappingPosition(position, positions);
      }
    }
    pastedOperators.push(newOperatorID);
    position = {
      x: copiedOperator.position.x + i * this.COPY_OFFSET,
      y: copiedOperator.position.y + i * this.COPY_OFFSET
    };
    return this.getNonOverlappingPosition(position, positions);
  }

  /**
   * Utility function to calculate the position to paste the group.
   * If a previously pasted group is moved or deleted, the operator will be
   * pasted to the emptied position. Otherwise, it will be pasted to a position
   * that's non-overlapping and calculated according to the copy operator offset.
   * @param newGroupID
   * @param copiedGroupID
   * @param positions
   */
  private calcGroupPosition(newGroupID: string, copiedGroupID: string, positions: Point[]): Point {
    let i, position;
    const copiedGroup = this.copiedGroups.get(copiedGroupID);
    if (!copiedGroup) {
      throw Error(`Internal error: cannot find ${copiedGroupID} in copied groups`);
    }
    const copiedGroupPosition = copiedGroup.position;
    const pastedGroups = copiedGroup.pastedGroupIDs;

    for (i = 0; i < pastedGroups.length; ++i) {
      position = {
        x: copiedGroupPosition.x + i * this.COPY_OFFSET,
        y: copiedGroupPosition.y + i * this.COPY_OFFSET
      };
      if (!positions.includes(position) && (!this.workflowActionService.getOperatorGroup().hasGroup(pastedGroups[i]) ||
        this.workflowActionService.getJointGraphWrapper().getElementPosition(pastedGroups[i]).x !== position.x ||
        this.workflowActionService.getJointGraphWrapper().getElementPosition(pastedGroups[i]).y !== position.y)) {
        pastedGroups[i] = newGroupID;
        return this.getNonOverlappingPosition(position, positions);
      }
    }
    pastedGroups.push(newGroupID);
    position = {
      x: copiedGroupPosition.x + i * this.COPY_OFFSET,
      y: copiedGroupPosition.y + i * this.COPY_OFFSET
    };
    return this.getNonOverlappingPosition(position, positions);
  }


  /**
   * Utility function to find a non-overlapping position for the pasted operator.
   * The function will check if the current position overlaps with an existing
   * operator. If it does, the function will find a new non-overlapping position.
   * @param position
   * @param positions
   */
  private getNonOverlappingPosition(position: Point, positions: Point[]): Point {
    let overlapped = false;
    const operatorPositions = positions.concat(this.workflowActionService.getTexeraGraph().getAllOperators()
      .map(operator => this.workflowActionService.getOperatorGroup().getOperatorPositionByGroup(operator.operatorID)),
      this.workflowActionService.getOperatorGroup().getAllGroups().map(
        group => this.workflowActionService.getJointGraphWrapper().getElementPosition(group.groupID)));
    do {
      for (const operatorPosition of operatorPositions) {
        if (operatorPosition.x === position.x && operatorPosition.y === position.y) {
          position = { x: position.x + this.COPY_OFFSET, y: position.y + this.COPY_OFFSET };
          overlapped = true;
          break;
        }
        overlapped = false;
      }
    } while (overlapped);
    return position;
  }

  /**
   * handle the events of the cursor enter/leave a jointJS link cell
   *
   * Originally, such "hover -> appear" feature came as a default setting with JointJS library
   * However, in order to achieve conditional disappearance for the breakpoint button,
   * every interaction between the cursor and the link tools, including the delete button,
   * need to be handled manually
   */
  private handleLinkCursorHover(): void {
    // When the cursor hovers over a link, the delete button and the breakpoint button appear
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'link:mouseenter')
      .map(value => value[0])
      .subscribe(
        elementView => {
          if (environment.linkBreakpointEnabled) {
            this.getJointPaper().getModelById(elementView.model.id).attr({
              '.tool-remove': { display: 'block' },
            });
            this.getJointPaper().getModelById(elementView.model.id).findView(this.getJointPaper()).showTools();
          } else {
            // only display the delete button
            this.getJointPaper().getModelById(elementView.model.id).attr({
              '.tool-remove': { display: 'block' },
            });
          }
        }
      );

    /**
    * When the cursor leaves a link, the delete button disappears.
    * If there is no breakpoint present on that link, the breakpoint button also disappears,
    * otherwise, the breakpoint button is not changed.
    */
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'link:mouseleave')
      .map(value => value[0])
      .subscribe(
        elementView => {
          // ensure that the link element exists
          if (this.getJointPaper().getModelById(elementView.model.id)) {
            const LinksWithBreakpoint = this.workflowActionService.getJointGraphWrapper().getLinkIDsWithBreakpoint();
            if (!LinksWithBreakpoint.includes(elementView.model.id.toString())) {
              this.getJointPaper().getModelById(elementView.model.id).findView(this.getJointPaper()).hideTools();
            }
            this.getJointPaper().getModelById(elementView.model.id).attr({
              '.tool-remove': { display: 'none' },
            });
          }
        }
      );
  }

  /**
   * handles events/observables related to the breakpoint
   */
  private handleLinkBreakpoint(): void {
    this.handleLinkBreakpointToolAttachment();
    this.handleLinkBreakpointButtonClick();
    this.handleLinkBreakpointHighlighEvents();
    this.handleLinkBreakpointToggleEvents();
  }

  // when a link is added, append a breakpoint link-tool to its LinkView
  private handleLinkBreakpointToolAttachment(): void {
    this.workflowActionService.getJointGraphWrapper().getJointLinkCellAddStream().subscribe(link => {

      const linkView = link.findView(this.getJointPaper());
      const breakpointButtonTool = this.jointUIService.getBreakpointButton();
      const breakpointButton = new breakpointButtonTool();
      const toolsView = new joint.dia.ToolsView({
        name: 'basic-tools',
        tools: [breakpointButton]
      });
      linkView.addTools(toolsView);
      // tools remain hidden until the cursor hovers over it or a break point is added
      linkView.hideTools();
    });
  }

  /**
   * handles the events of the breakpoint button is clicked for a link
   * and converts that event to a workflow action
   */
  private handleLinkBreakpointButtonClick(): void {
    Observable
      .fromEvent<JointPaperEvent>(this.getJointPaper(), 'tool:breakpoint', { passive: true })
      .subscribe(
        event => {
          this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(<boolean>event[1].shiftKey);
          this.workflowActionService.getJointGraphWrapper().highlightLinks(event[0].model.id.toString());
        }
      );
  }

  /**
   * Highlight/unhighlight the link according to the observable value recieved.
   */
  private handleLinkBreakpointHighlighEvents(): void {
    this.workflowActionService.getJointGraphWrapper().getLinkHighlightStream()
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.getJointPaper().getModelById(linkID).attr({
            '.connection': { stroke: 'orange' },
            '.marker-source': { fill: 'orange' },
            '.marker-target': { fill: 'orange' }
          });
        });
      }
      );

    this.workflowActionService.getJointGraphWrapper().getLinkUnhighlightStream()
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          const linkView = this.getJointPaper().findViewByModel(linkID);
          // ensure that the link still exist
          if (this.getJointPaper().getModelById(linkID)) {
            this.getJointPaper().getModelById(linkID).attr({
              '.connection': { stroke: linkPathStrokeColor },
              '.marker-source': { fill: 'none' },
              '.marker-target': { fill: 'none' }
            });
          }
        });
      }
      );
  }

  /**
   * show/hide the breakpoint button according to the observable value received
   */
  private handleLinkBreakpointToggleEvents(): void {
    this.workflowActionService.getJointGraphWrapper().getLinkBreakpointShowStream()
      .subscribe(linkID => {
        this.getJointPaper().getModelById(linkID.linkID).findView(this.getJointPaper()).showTools();
      });

    this.workflowActionService.getJointGraphWrapper().getLinkBreakpointHideStream()
      .subscribe(linkID => {
        this.getJointPaper().getModelById(linkID.linkID).findView(this.getJointPaper()).hideTools();
      });
  }
}

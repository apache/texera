import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { WorkflowActionService } from '../../../service/workflow-graph/model/workflow-action.service' ;
import { Component, Input, AfterViewInit, OnInit, ElementRef, OnDestroy } from '@angular/core';
import { Observable, of, Subject, Subscription, zip } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { OperatorSchema } from '../../../types/operator-schema.interface';
import { assertType } from 'src/app/common/util/assert';

/**
 * OperatorLabelComponent is one operator box in the operator panel.
 *
 * @author Zuozhi Wang
 */
@Component({
  selector: 'texera-operator-label',
  templateUrl: './operator-label.component.html',
  styleUrls: ['./operator-label.component.scss']
})
export class OperatorLabelComponent implements OnInit, AfterViewInit, OnDestroy {

  public static operatorLabelPrefix = 'texera-operator-label-';
  public static operatorLabelSearchBoxPrefix = 'texera-operator-label-search-result-';

  // tooltipWindow is an instance of ngbTooltip (popup box)
  @Input() operator?: OperatorSchema;
  // whether the operator label is from the operator panel or the search box
  @Input() fromSearchBox?: boolean;
  public operatorLabelID?: string;

  // bound to ngClass
  public draggable = true;
  public animate = '';

  // is mouse down over this label
  private isMouseDown = false;

  // keep subscription in order to unsubscribe when component is destroyed
  private workflowModificationEnabledSubscription: Subscription;


  constructor(
    private dragDropService: DragDropService,
    private workflowActionService: WorkflowActionService,
  ) {
    this.workflowModificationEnabledSubscription = this.handleWorkFlowModificationEnabled();
  }

  ngOnInit() {
    if (! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    if (this.fromSearchBox) {
      this.operatorLabelID = OperatorLabelComponent.operatorLabelSearchBoxPrefix + this.operator.operatorType;
    } else {
      this.operatorLabelID = OperatorLabelComponent.operatorLabelPrefix + this.operator.operatorType;
    }
  }

  ngAfterViewInit() {
    if (! this.operatorLabelID || ! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    this.dragDropService.registerOperatorLabelDrag(this.operatorLabelID, this.operator.operatorType);

  }

  ngOnDestroy() {
    this.workflowModificationEnabledSubscription.unsubscribe();
  }

  // mouseDownEventStream sends out a value
  public mouseDown(): void {
    this.isMouseDown = true;
  }

  public mouseUp(): void {
    this.isMouseDown = false;
  }

  // mouseLeaveEventStream sends out a value
  public mouseLeave(): void {
    // reject mouse drag out if not draggable
    if (this.isMouseDown && !this.draggable) {
      this.animateReject();
    }

    this.isMouseDown = false;
  }

  private animateReject(): void {
    // remove and re-add shake animation to replay it
    this.animate = '';
    setTimeout(() => { this.animate = 'reject'; }, 0);
    setTimeout(() => { this.animate = ''; }, 400);
  }

  private handleWorkFlowModificationEnabled(): Subscription {
    return this.workflowActionService.getWorkflowModificationEnabledStream().subscribe( enabled => {
      this.draggable = enabled;
    });
  }

  public static isOperatorLabelElementFromSearchBox(elementID: string) {
    return elementID.startsWith(OperatorLabelComponent.operatorLabelSearchBoxPrefix);
  }
}

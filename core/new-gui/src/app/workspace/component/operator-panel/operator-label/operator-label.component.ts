import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { Component, Input, AfterViewInit, ViewChild, OnInit } from '@angular/core';
import { v4 as uuid } from 'uuid';
import { Observable, of, Subject} from 'rxjs';
import { takeUntil } from 'rxjs/operators';


import { OperatorSchema } from '../../../types/operator-schema.interface';
import { NgbTooltip } from '../../../../../../node_modules/@ng-bootstrap/ng-bootstrap/tooltip/tooltip';

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
export class OperatorLabelComponent implements OnInit, AfterViewInit {

  public static operatorLabelPrefix = 'texera-operator-label-';
  public static operatorLabelSearchBoxPrefix = 'texera-operator-label-search-result-';

  // tooltipWindow is an instance of ngbTooltip (popup box)
  @ViewChild('ngbTooltip') tooltipWindow: NgbTooltip | undefined;
  @Input() operator?: OperatorSchema;
  // whether the operator label is from the operator panel or the search box
  @Input() fromSearchBox?: boolean;
  public operatorLabelID?: string;

  // values from mouseEnterEventStream correspond to cursor entering operator label
  // values from mouseLeaveEventStream correspond to cursor leaving  operator label
  // each value from openCommandsStream correspond to a command to display operator description
  private mouseEnterEventStream = new Subject<void>();
  private mouseLeaveEventStream = new Subject<void>();
  private openCommandsStream = new Observable<void>();

  constructor(
    private dragDropService: DragDropService
  ) {
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

    // openCommandsStream generate a value when 2 conditions are met:
    //  1. an value from mouseEnterEventStream is observed
    //  2. within the next 500ms, no value is observed from mouseLeaveEvenStream
    this.openCommandsStream = this.mouseEnterEventStream.flatMap(v =>
      of(v).delay(500).pipe(takeUntil(this.mouseLeaveEventStream))
    );

    // whenever an value from openCommandsStream is observed, open tooltipWindow
    this.openCommandsStream.subscribe(v => {
      if (this.tooltipWindow) {
        this.tooltipWindow.open();
      }
    });

    // whenever an value from mouseLeaveEventStream is observed, close tooltipWindow
    this.mouseLeaveEventStream.subscribe(v => {
      if (this.tooltipWindow) {
        this.tooltipWindow.close();
      }
    });
  }

  // return openCommandStream to faciliate testing in spec.ts
  public getopenCommandsStream(): Observable<void> {
    return this.openCommandsStream;
  }

  // mouseEnterEventStream sends out a value
  public mouseEnter(): void {
    this.mouseEnterEventStream.next();
  }

  // mouseLeaveEventStream sends out a value
  public mouseLeave(): void {
    this.mouseLeaveEventStream.next();
  }

  public static isOperatorLabelElementFromSearchBox(elementID: string) {
    return elementID.startsWith(OperatorLabelComponent.operatorLabelSearchBoxPrefix);
  }
}

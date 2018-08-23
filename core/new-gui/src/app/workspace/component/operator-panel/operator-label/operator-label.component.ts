import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { Component, Input, AfterViewInit } from '@angular/core';
import { v4 as uuid } from 'uuid';

import { OperatorSchema } from '../../../types/operator-schema.interface';
import { Time } from '../../../../../../node_modules/@angular/common';

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
export class OperatorLabelComponent implements AfterViewInit {

  @Input() operator?: OperatorSchema;
  public operatorLabelID: string;
  public isHovering: boolean;
  public timer: any;

  constructor(
    private dragDropService: DragDropService
  ) {
    // generate a random ID for this DOM element
    this.operatorLabelID = 'texera-operator-label-' + uuid();
    this.isHovering = false;
  }

  ngAfterViewInit() {
    if (! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    this.dragDropService.registerOperatorLabelDrag(this.operatorLabelID, this.operator.operatorType);
  }

  displayDescription() {
    // this.isHovering = false;
    this.timer = setTimeout(() => this.isHovering = true, 1000);
  }

  hideDescription() {
    clearTimeout(this.timer);
    this.isHovering = false;
  }

}

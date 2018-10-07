import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { Component, Input, OnInit, AfterViewInit } from '@angular/core';
import { v4 as uuid } from 'uuid';

import { OperatorSchema } from '../../../types/operator-schema.interface';

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

  @Input() operator?: OperatorSchema;
  public operatorLabelID?: string;

  constructor(
    private dragDropService: DragDropService
  ) {
  }

  ngOnInit() {
    if (! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    this.operatorLabelID = 'texera-operator-label-' + this.operator.operatorType;
  }

  ngAfterViewInit() {
    if (! this.operatorLabelID || ! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    this.dragDropService.registerOperatorLabelDrag(this.operatorLabelID, this.operator.operatorType);
  }

}

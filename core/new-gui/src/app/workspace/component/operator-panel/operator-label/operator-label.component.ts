import { Component, OnInit, Input, AfterViewInit } from '@angular/core';
import { v4 as uuid } from 'uuid';

import { OperatorSchema } from '../../../types/operator-schema';

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

  @Input() operator: OperatorSchema;
  public operatorLabelID: string;

  constructor() {
  }

  ngOnInit() {
    // generate a random ID for this DOM element
    this.operatorLabelID = 'texera-operator-label-'  + uuid();
  }

  ngAfterViewInit() {
  }

}

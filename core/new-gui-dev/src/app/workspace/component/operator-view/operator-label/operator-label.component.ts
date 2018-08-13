import { Component, OnInit, Input, AfterViewInit } from '@angular/core';

import { OperatorDragDropService } from '../../../service/operator-drag-drop/operator-drag-drop.service';

/**
 * OperatorLabelComponent is one operator box in the operator panel.
 * After the componenet's view is initialized, it registers its DOM ID to the Drag and Drop Service,
 *  then the Drag and Drop Service will make this componenet draggable.
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

  constructor(private operatorDragDropService: OperatorDragDropService) {
  }

  ngOnInit() {
    this.operatorLabelID = 'texera-operator-label-'  + this.operator.operatorType;
  }

  ngAfterViewInit() {
    this.operatorDragDropService.registerDrag(this.operatorLabelID, this.operator.operatorType);
  }

}

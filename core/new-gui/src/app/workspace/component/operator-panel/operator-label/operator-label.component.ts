import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { Component, Input, AfterViewInit, ViewChild, ElementRef } from '@angular/core';
import { v4 as uuid } from 'uuid';
import { Observable, fromEvent, interval} from 'rxjs';

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
export class OperatorLabelComponent implements AfterViewInit {

  @ViewChild('t') t: any; // t is the specific tooltip window for an operator
  @Input() operator?: OperatorSchema;
  public operatorLabelID: string;
  private timer: any; // needed to add a delay to tooltip
  private isHovering: boolean;

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

  // show the tooltip window after 1500ms
  // var t means tooltip
  public displayDescription(t: any): void {
    this.isHovering = true;
    const secondsCounter = interval(1500);

    // filter(isHovering) ensures that the cursor is still inside the label
    // when secontsCounter emites an observable
    this.timer = secondsCounter.filter(val => this.isHovering).subscribe(val => (
      this.t.open(),
      // unsubscribe the timer after the first observable is received to
      // ensure that the function is not called only once.
      this.timer.unsubscribe()
    ));
  }

  // hide the tooltip window
  // reset the timer
  // set isHovering to false to indicate that the cursor has left the operator label
  public hideDescription(t: any): void {
    this.isHovering = false;
    this.t.close();
    this.timer.unsubscribe();
  }
}

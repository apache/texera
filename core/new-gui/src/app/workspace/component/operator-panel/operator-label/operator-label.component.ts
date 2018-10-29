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

  @ViewChild('t') t: any;
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

    // this.popup();
  }

  // private popup(): void {
  //   const operator_label = document.getElementById(this.operatorLabelID);
  //   // const operator_label = document.getElementsByTagName('MAT-CARD');
  //   const mouseEnter = fromEvent(operator_label, 'mouseenter');
  //   const mouseLeave = fromEvent(operator_label, 'mouseleave');

  //   mouseEnter.subscribe(() => {
  //     console.log('mouseEnter');
  //     this.t.open();
  //     // const secondsCounter = interval(1500);
  //     // // Subscribe to begin publishing values
  //     // secondsCounter.subscribe(
  //     //   this.t.open()
  //     // );
  //   });

  //   mouseLeave.subscribe(() => {
  //     this.t.close();
  //   });
  // }


  // show the tooltip window after 1500ms
  // var t means tooltip
  private displayDescription(t: any): void {
    this.isHovering = true;
    const secondsCounter = interval(1500);
    this.timer = secondsCounter.filter(val => this.isHovering).subscribe(val => (
      console.log(val),
      this.t.open(),
      this.timer.unsubscribe()
    ));
  }

  // hide the tooltip window
  // reset the timer
  private hideDescription(t: any): void {
    this.isHovering = false;
    this.t.close();
    this.timer.unsubscribe();
  }
}

import { DragDropService } from './../../../service/drag-drop/drag-drop.service';
import { Component, Input, AfterViewInit, ViewChild, ElementRef } from '@angular/core';
import { v4 as uuid } from 'uuid';
import { Observable, of, Subject} from 'rxjs';
import { takeUntil } from 'rxjs/operators';


import { OperatorSchema } from '../../../types/operator-schema.interface';
import { NgbTooltipWindow, NgbTooltip } from '../../../../../../node_modules/@ng-bootstrap/ng-bootstrap/tooltip/tooltip';

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

  @ViewChild('ngbTooltip') tooltipWindow: NgbTooltip | undefined;
  @Input() operator?: OperatorSchema;
  public operatorLabelID: string;

  private mouseEnterSubject$ = new Subject<void>();
  private mouseLeaveSubject$ = new Subject<void>();



  constructor(
    private dragDropService: DragDropService
  ) {
    // generate a random ID for this DOM element
    this.operatorLabelID = 'texera-operator-label-' + uuid();
  }

  ngAfterViewInit() {
    if (! this.operator) {
      throw new Error('operator label component: operator is not specified');
    }
    this.dragDropService.registerOperatorLabelDrag(this.operatorLabelID, this.operator.operatorType);

    this.mouseEnterSubject$.flatMap(v =>
      of(v).delay(500).pipe(takeUntil(this.mouseLeaveSubject$))
    ).subscribe(v => {
      if (this.tooltipWindow) {
        this.tooltipWindow.open();
      }
    });

    this.mouseLeaveSubject$.subscribe(v => {
      if (this.tooltipWindow) {
        this.tooltipWindow.close();
      }
    });
  }

  mouseEnter(): void {
    this.mouseEnterSubject$.next();
  }

  mouseLeave(): void {
    this.mouseLeaveSubject$.next();
  }
}

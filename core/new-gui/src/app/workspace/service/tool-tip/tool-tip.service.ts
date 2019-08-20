import { Injectable } from '@angular/core';
import { Subject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ToolTipService {
  /**
   * this service is to communicate between the parent operator and the child tooltip.
   * the tool tip is the interactive component which can show the real time data sent from back end
   * @author Simon Zhou
   */
  private tooltipShowSubject: Subject<string> = new Subject();
  constructor() { }

  public handleTooltipShowEvent(tooltipID: string): void {
    this.tooltipShowSubject.next(tooltipID);
  }

  public handleTooltipHiddenEvent(tooltipID: string): void {
    this.tooltipShowSubject.next(tooltipID);
  }

  public getTooltipShowSubjectStream(): Observable<string> {
    return this.tooltipShowSubject.asObservable();
  }


}

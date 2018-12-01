import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
@Injectable({
  providedIn: 'root'
})
export class ResultPanelToggleService {

  private showResultPanel: boolean = false;
  private toggleDisplayChangeStream = new Subject<boolean>();

  constructor() { }


  /**
   * Gets an observable for operator is dropped on the main workflow editor event
   * Contains an object with:
   *  boolean : the status of showresultPanel
   */
  public getToggleChangeStream(): Observable<boolean> {
    return this.toggleDisplayChangeStream.asObservable();
  }

  /**
   * notify the subject of the event
   * if the showResultPanel is true, the css of workspace will be texera-workspace-grid-container
   * and resultPanel will be shown.
   */
  public openResultPanel(): void {
    this.showResultPanel = true;
    this.toggleDisplayChangeStream.next(true);
  }

  /**
   * notify the subject of the event
   * if showRescultPanel is false, the css of workspace will be texera-original-workspace-grid-container
   * and resultPanel will be hidden.
   */
  public closeResultPanel(): void {
    this.showResultPanel = false;
    this.toggleDisplayChangeStream.next(false);
  }

  /**
   * switch the status of resultpanel and grid css style of workspace
   * if resultPanel is open, then it will be closed, or
   * if resultPanel is closed, then it will open.
   * @param flag
   */
  public toggleResultPanel(currentResultPanelStatus: boolean): void {
    if (currentResultPanelStatus) {
      this.closeResultPanel();
    } else {
      this.openResultPanel();
    }
  }


}










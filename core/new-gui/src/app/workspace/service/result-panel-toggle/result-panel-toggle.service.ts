import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

/**
 * ResultPanelToggleService handles the logic to open / close the result panel
 *  when the user clicks the panel toggle or when the result is returned
 *  from the backend to be displayed on the result panel.
 *
 * @author Angela Wang
 */
@Injectable({
  providedIn: 'root'
})
export class ResultPanelToggleService {

  private currentResultPanelStatus: boolean = false;
  private toggleDisplayChangeStream = new Subject<boolean>();
  constructor() { }

  public isResultPanelOpen() {
    return this.currentResultPanelStatus;
  }

  /**
   * Gets the observable for result panel toggle change event.
   * Contains a boolean variable that indicates:
   *  - the new status for the result panel
   */

  public getToggleChangeStream(): Observable<boolean> {
    return this.toggleDisplayChangeStream.asObservable();
  }

  /**
   * Notify the toggle display subject to open the result panel.
   *
   * This will trigger an event that modifies the current css of
   *  workspace to use 'texera-workspace-grid-container' to show
   *  the result panel.
   *
   */
  public openResultPanel(): void {
    this.currentResultPanelStatus = true;
    this.toggleDisplayChangeStream.next(true);
  }

  /**
   * Notify the toggle display subject to close the result panel.
   *
   * This will trigger an event that modifies the current css of
   *  workspace to use 'texera-original-workspace-grid-container' to hide
   *  the result panel.
   *
   */
  public closeResultPanel(): void {
    this.currentResultPanelStatus = false;
    this.toggleDisplayChangeStream.next(false);
  }

  /**
   * Toggle the current status of result panel and modify the css
   *  grid-cell design of the workspace to hide/show the result panel.
   *
   * When current status = open  : close the result panel
   * When current status = close : open the result panel
   *
   */
  public toggleResultPanel(): void {
    if (this.currentResultPanelStatus) {
      this.closeResultPanel();
    } else {
      this.openResultPanel();
    }
  }

}








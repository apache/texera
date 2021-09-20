import { Component } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";

/**
 * ResultPanelToggleComponent is the small bar directly above ResultPanelComponent at the
 *  bottom level. When the user interface first initialized, ResultPanelComponent will be
 *  hidden and ResultPanelToggleComponent will be at the bottom of the UI.
 *
 * This Component is a toggle button to open / close the result panel.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-panel-toggle",
  templateUrl: "./result-panel-toggle.component.html",
  styleUrls: ["./result-panel-toggle.component.scss"],
})
export class ResultPanelToggleComponent {
  public showResultPanel: boolean = false;

  constructor(private resultPanelToggleService: ResultPanelToggleService) {
    this.resultPanelToggleService
      .getToggleChangeStream()
      .pipe(untilDestroyed(this))
      .subscribe(newPanelStatus => (this.showResultPanel = newPanelStatus));
  }

  /**
   * When the result panel toggle is clicked, it will call 'toggleResultPanel'
   *  to switch the status of the result panel.
   */
  onClickResultBar(): void {
    this.resultPanelToggleService.toggleResultPanel();
  }
}

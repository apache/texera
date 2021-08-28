import { Component, OnDestroy, OnInit } from "@angular/core";
import { ResultPanelToggleService } from "../../service/result-panel-toggle/result-panel-toggle.service";
import { Subscription } from 'rxjs';

/**
 * ResultPanelToggleComponent is the small bar directly above ResultPanelComponent at the
 *  bottom level. When the user interface first initialized, ResultPanelComponent will be
 *  hidden and ResultPanelToggleComponent will be at the bottom of the UI.
 *
 * This Component is a toggle button to open / close the result panel.
 */
@Component({
  selector: "texera-result-panel-toggle",
  templateUrl: "./result-panel-toggle.component.html",
  styleUrls: ["./result-panel-toggle.component.scss"]
})
export class ResultPanelToggleComponent implements OnInit, OnDestroy {

  subscriptions = new Subscription();

  showResultPanel: boolean = false;

  constructor(private resultPanelToggleService: ResultPanelToggleService) {}

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  ngOnInit() {
    this.subscriptions.add(this.resultPanelToggleService.getToggleChangeStream().subscribe(
      newPanelStatus => this.showResultPanel = newPanelStatus,
    ));
  }

  /**
   * When the result panel toggle is clicked, it will call 'toggleResultPanel'
   *  to switch the status of the result panel.
   */
  onClickResultBar(): void {
    this.resultPanelToggleService.toggleResultPanel();
  }


}

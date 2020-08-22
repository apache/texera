import { Component, OnInit } from '@angular/core';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';

/**
 * ResultPanelToggleComponent is the small bar directly above ResultPanelCompoent at the
 *  bottom level. When the user interface first initialized, ResultPanelCompoent will be
 *  hidden and ResultPanelToggleComponent will be at the bottom of the UI.
 *
 * This Component is a toggle button to open / close the result panel.
 *
 * @author Angela Wang
 */
@Component({
  selector: 'texera-result-panel-toggle',
  templateUrl: './result-panel-toggle.component.html',
  styleUrls: ['./result-panel-toggle.component.scss']
})
export class ResultPanelToggleComponent implements OnInit {

  public showResultPanel: boolean = false;

  constructor(private resultPanelToggleService: ResultPanelToggleService) {
    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      newPanelStatus => this.showResultPanel = newPanelStatus,
    );
  }

  ngOnInit() {
  }

  /**
   * When the result panel toggle is clicked, it will call 'toggleResultPanel'
   *  to switch the status of the result panel.
   */
  public onClickResultBar(): void {
    this.resultPanelToggleService.toggleResultPanel();
  }
}



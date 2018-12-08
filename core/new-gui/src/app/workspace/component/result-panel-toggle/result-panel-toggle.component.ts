import { Component, OnInit } from '@angular/core';
import { ResultPanelToggleService } from './../../service/result-panel-toggle/result-panel-toggle.service';
@Component({
  selector: 'texera-result-panel-toggle',
  templateUrl: './result-panel-toggle.component.html',
  styleUrls: ['./result-panel-toggle.component.scss']
})
export class ResultPanelToggleComponent implements OnInit {

  public showResultPanel: boolean = false;
  constructor(private resultPanelToggleService: ResultPanelToggleService) {
    this.resultPanelToggleService.getToggleChangeStream().subscribe(
      value => this.showResultPanel = value,
    );
  }


  ngOnInit() {
  }

  /**
   * click the resultBar and it will switch the result panel and let it hide or show
   */

  public onClickResultBar(): void {
    this.resultPanelToggleService.toggleResultPanel(this.showResultPanel);
  }
}



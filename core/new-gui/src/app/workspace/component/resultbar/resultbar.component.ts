import { Component, OnInit } from '@angular/core';
import{ ResultPanelService} from './../../service/result-panel/result-panel.service'

@Component({
    selector: 'texera-resultbar',
    templateUrl: './resultbar.component.html',
    styleUrls: ['./resultbar.component.scss']
})

export class ResultbarComponent implements OnInit {

    constructor(public resultPanelService:ResultPanelService) { }

    ngOnInit(){}
 /**
   * click the resultBar and it will switch the result panel and let it hide or show
   */
  
  public onClickResultBar():void{
    
    if(this.resultPanelService.getShowResultPanel()){
      this.resultPanelService.setShowResultPanel(false);
    } else {
      this.resultPanelService.setShowResultPanel(true);
    }
    
  }
}
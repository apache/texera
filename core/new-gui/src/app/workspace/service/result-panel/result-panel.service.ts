import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';
import {ResultPanelComponent} from '../../component/result-panel/result-panel.component';
 
 @Injectable({
     providedIn:'root'
 })

 export class ResultPanelService{

    constructor() { }
    private showResultPanel:boolean = false;
    /**
     * if showRescultPanel is false, the css of workspace will be texera-original-workspace-grid-container
     * and resultPanel will be hidden or if the showResultPanel is true, the css of workspace will be texera-workspace-grid-container
     * and resultPanel will be shown
     */

    public setShowResultPanel(flag:boolean):void{
        this.showResultPanel = flag;
    }

    public getShowResultPanel():boolean{
        return this.showResultPanel;
    }
    
 } 




import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';

import {ResultPanelComponent} from '../../component/result-panel/result-panel.component';
 
 @Injectable({
     providedIn:'root'
 })
 export class ResultPanelService{
    showResultPanel:boolean = false;

    setShowResultPanel(flag:boolean):void{
        this.showResultPanel = flag;
    }

    
 } 
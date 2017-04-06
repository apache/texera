import { Component, ViewChild  } from '@angular/core';
import { CurrentDataService } from './current-data-service';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: '[the-navbar]',
    templateUrl: './navigation-bar.component.html',
    styleUrls: ['style.css']
})
export class NavigationBarComponent {
  operatorId: number;

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.operatorId = data.operatorNum;
      }
    );
  }


  onClick(event) {
        this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
        this.currentDataService.processData();
  }

	DeleteOp(data : any){
        jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
        this.currentDataService.clearData();
        this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
	}
}

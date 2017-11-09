import { Component, ViewChild  } from '@angular/core';
import { CurrentDataService } from '../services/current-data-service';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: '[the-navbar]',
    templateUrl: './navigation-bar.component.html',
    styleUrls: ['../style.css']
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

  testing() {
    var w1 = jQuery(window).height();   // returns height of browser viewport
    var d1 = jQuery(document).height(); // returns height of HTML document (same as pageHeight in screenshot)
    var w2 = jQuery(window).width();   // returns width of browser viewport
    var d2 = jQuery(document).width();
    console.log(w1);
    console.log(d1);
    console.log(w2);
    console.log(d2);
  }
  onClick(event) {
        jQuery('.navigation-btn').button('loading');
        this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        this.currentDataService.processRunData();
  }

	deleteOperator() {
        if (this.operatorId == null){
          return;
        }
        jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
        this.currentDataService.clearData();
        this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
	}

}

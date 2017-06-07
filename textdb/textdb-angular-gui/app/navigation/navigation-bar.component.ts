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


  onClick(event) {
        jQuery.showLoading({allowHide: true});
        this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
        this.currentDataService.processData();
  }

	deleteOperator() {
        jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
        this.currentDataService.clearData();
        this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
	}

  fileChange(event) {
    let fileList: FileList = event.target.files;
    this.currentDataService.uploadDictionary(fileList[0]);
  }
}

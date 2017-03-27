import { Component } from '@angular/core';
import { CurrentDataService } from './current-data-service';

declare var jQuery: any;
@Component({
	moduleId: module.id,
	selector: 'flowchart-container',
	template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
		</div>
	`,
	styleUrls: ['style.css'],
	providers: [CurrentDataService]
})
export class TheFlowchartComponent {

	constructor(private currentDataService : CurrentDataService) {
		currentDataService.newAddition$.subscribe(
			data => {
				console.log("In flowchart component" + data);
				console.log("In flowchart " + data.operatorNum);
			}
		);
	}

	initialize(data: any) {
		var current = this;
		jQuery('#the-flowchart').flowchart({
			data: data,
    	multipleLinksOnOutput: true,
			onOperatorSelect : function (operatorId){
				// current.currentDataService.selectData(operatorId);
				return true;
			}
		});
	}

}

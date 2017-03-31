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
})
export class TheFlowchartComponent {
	constructor(private currentDataService : CurrentDataService) { }
	initialize(data: any) {
		var current = this;
		jQuery('#the-flowchart').flowchart({
			data: data,
    	multipleLinksOnOutput: true,
			onOperatorSelect : function (operatorId){
				console.log("operator is selected");
				current.currentDataService.selectData(operatorId);
				return true;
			},
			onOperatorUnselect : function (operatorId){
				console.log("operator is unselected");
				return true;
			}
		});
	}
}

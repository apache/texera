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
		
		jQuery('html').keyup(function(e){ //key binding function
			if(e.keyCode === 8){ //backspace
				var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
				if (current_id !== null){
					jQuery('#the-flowchart').flowchart('deleteSelected');
					current.currentDataService.clearData();
				}
			} else if (e.keyCode === 46){ //delete
				var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
				if (current_id !== null){
					jQuery('#the-flowchart').flowchart('deleteSelected');
					current.currentDataService.clearData();
				}
			}
		})

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

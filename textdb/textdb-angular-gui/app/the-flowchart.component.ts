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
	TheOperatorNumNow : number;
	constructor(private currentDataService : CurrentDataService) {
		currentDataService.newAddition$.subscribe(
			data => {
				console.log("IN NEW ADDITION : " + data.operatorNum);
				this.TheOperatorNumNow =  data.operatorNum;
			}
		);
	}
	initialize(data: any) {


		var current = this;


		// unselect operator when user click other div
		jQuery('html').mouseup(function(e){
			var container = jQuery(".form-control");
			if (container.is(e.target)){
				jQuery("#the-flowchart").flowchart("unselectOperator");
			}
		});

		jQuery('html').keyup(function(e){ //key binding function
			if(e.keyCode === 8){ //backspace
				var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
				if (current_id !== null){
					jQuery('#the-flowchart').flowchart('deleteSelected');
					current.currentDataService.clearData();
					current.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
				}
			} else if (e.keyCode === 46){ //delete
				var current_id = jQuery('#the-flowchart').flowchart('getSelectedOperatorId');
				if (current_id !== null){
					jQuery('#the-flowchart').flowchart('deleteSelected');
					current.currentDataService.clearData();
					current.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
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

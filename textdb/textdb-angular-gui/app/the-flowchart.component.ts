import { Component } from '@angular/core';
declare var jQuery: any;

@Component({
	moduleId: module.id,
	selector: 'flowchart-container',
	template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
		</div>
	`,
	styleUrls: ['style.css']
})
export class TheFlowchartComponent {

	initialize(data: any) {
		jQuery('#the-flowchart').flowchart({
			data: data,
            multipleLinksOnOutput: true
		});
	}

}

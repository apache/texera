import { Component, ViewChild } from '@angular/core';
import { TheFlowchartComponent } from './the-flowchart.component';;
import { OperatorBarComponent } from './operator-bar.component';;
declare var jQuery: any;

@Component({
	moduleId: module.id,
  selector: 'my-app',
  template: `
		<nav the-navbar id="css-navbar" class="navbar navbar-toggleable-md navbar-light bg-faded"></nav>
		<nav operator-bar id="css-operator-bar" class="navbar navbar-toggleable-md navbar-light bg-faded" #theOperatorBar></nav>
		<flowchart-container class="container fill" #theFlowchart></flowchart-container>
	`,
	styleUrls: ['style.css']
})
export class AppComponent  { 
	name = 'Angular';
	
  @ViewChild('theFlowchart') theFlowchart: TheFlowchartComponent;
	@ViewChild('theOperatorBar') theOperatorBar: OperatorBarComponent;

	ngAfterViewInit() {
		var current = this;
		jQuery(document).ready(function() {
			current.theFlowchart.initialize();
		});
	}
}

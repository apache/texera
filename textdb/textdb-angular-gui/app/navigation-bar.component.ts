import { Component } from '@angular/core';
declare var jQuery: any;

@Component({
    moduleId: module.id,
  selector: '[the-navbar]',
  templateUrl: './navigation-bar.component.html'
})
export class NavigationBarComponent {

	DeleteOp(data : any){
    jQuery('#the-flowchart').flowchart('deleteSelected');
	}
}

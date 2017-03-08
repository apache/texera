import { Component } from '@angular/core';

import { CurrentDataService } from './current-data-service';

declare var jQuery: any;

@Component({
    moduleId: module.id,
    selector: 'side-bar-container',
    templateUrl: './side-bar.component.html',
    styleUrls: ['style.css']
})
export class SideBarComponent {
    data: any;
    attributes: string[] = [];
    operator = "Operator";
    submitted = false;
    operatorId: number;

    constructor(private currentDataService: CurrentDataService) {
        currentDataService.newAddition$.subscribe(
            data => {
                this.data = data.operatorData;
                this.operatorId = data.operatorNum;
                this.operator = data.operatorData.properties.title;
                for(var attribute in data.operatorData.properties.attributes){
                    this.attributes.push(attribute);
                }
            });
    }

    humanize(name: string): string{
        var frags = name.split('_');
        for (var i=0; i<frags.length; i++) {
            frags[i] = frags[i].charAt(0).toUpperCase() + frags[i].slice(1);
        }
        return frags.join(' ');
    }

    onSubmit() {
        this.submitted = true;
        jQuery('#the-flowchart').flowchart('setOperatorData', this.operatorId, this.data);
        this.currentDataService.setData(this.data);
    }

}


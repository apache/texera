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


    tempSubmitted = false;
    tempData: any;
    tempArrayOfData: any;

    hiddenList : string[] = ["operator_type","limit","offset"];
    selectorList : string[] = ["matching_type","nlp_type","predicate_type","operator_type","limit","offset"];
    matcherList : string[] = ["conjunction","phrase","substring"];
    nlpList : string[] = ["noun","verb","adjective","adverb","ne_all","number","location","person","organization","money","percent","date","time"];
    predicateList : string[] = ["CharacterDistance", "SimilarityJoin"];

    checkInHidden(name : string){
      return jQuery.inArray(name,this.hiddenList);
    }
    checkInSelector(name: string){
      return jQuery.inArray(name,this.selectorList);
    }

    constructor(private currentDataService: CurrentDataService) {
        currentDataService.newAddition$.subscribe(
            data => {
                this.submitted = false;
                this.tempSubmitted = false;
                this.data = data.operatorData;
                this.operatorId = data.operatorNum;
                this.operator = data.operatorData.properties.title;
                this.attributes = [];
                for(var attribute in data.operatorData.properties.attributes){
                    this.attributes.push(attribute);
                }
            });

        currentDataService.checkPressed$.subscribe(
            data => {
                this.tempArrayOfData = [];
                this.submitted = false;
                this.tempSubmitted = true;
                this.tempData = data.returnedData;
                console.log(this.tempData);
                // this.tempArrayOfData = Object.keys(this.tempData);
            });
    }

    createResultFrame (JSON_Data : string) {
      return JSON_Data;
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
        this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
    }

    onDelete(){
          this.submitted = false;
          this.tempSubmitted = false;
          this.operator = "Operator";
          this.attributes = [];
          jQuery('#the-flowchart').flowchart('deleteSelected');
          this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
    }
}

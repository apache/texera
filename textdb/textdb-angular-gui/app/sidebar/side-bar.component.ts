import {Component, ViewChild, OnInit} from '@angular/core';

import { CurrentDataService } from '../services/current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';
import { TableMetadata } from "../services/table-metadata";
import {log} from "util";

declare var jQuery: any;
declare var Backbone: any;

declare var PrettyJSON: any;

@Component({
  moduleId: module.id,
  selector: 'side-bar-container',
  templateUrl: './side-bar.component.html',
  styleUrls: ['../style.css']
})

export class SideBarComponent {
  data: any;
  attributes: string[] = [];

  operatorId: number;
  operatorTitle: string;

  hiddenList: string[] = ["operatorType", "luceneAnalyzer", "matchingType"];

  selectorList: string[] = ["dictionaryEntries", "password", "matchingType", 
    "nlpEntityType", "splitType", "splitOption", "sampleType", "comparisonType", 
    "aggregationType", "attributes", "tableName", "attribute"].concat(this.hiddenList);

  matcherList: string[] = ["conjunction", "phrase", "substring"];
  nlpEntityList: string[] = ["noun", "verb", "adjective", "adverb", "ne_all", 
    "number", "location", "person", "organization", "money", "percent", "date", "time"];
  regexSplitList: string[] = ["left", "right", "standalone"];
  nlpSplitList: string[] = ["oneToOne", "oneToMany"];
  samplerList: string[] = ["random", "firstk"];

  compareList: string[] = ["=", ">", ">=", "<", "<=", "≠"];
  aggregationList: string[] = ["min", "max", "count", "sum", "average"];

  attributeItems:Array<string> = [];
  tableNameItems:Array<string> = [];
  selectedAttributesList:Array<string> = [];
  selectedAttributeMulti:string = "";
  selectedAttributeSingle:string = "";
  metadataList:Array<TableMetadata> = [];

  dictionaryNames: Array<string> = [];
  dictionaryContent: Array<string> = [];
  selectedDictionary:string = "";

  @ViewChild('MyModal')
  modal: ModalComponent;

  ModalOpen() {
    this.modal.open();
  }
  ModalClose() {
    this.modal.close();
  }

  checkInHidden(name: string) {
    return jQuery.inArray(name, this.hiddenList);
  }
  checkInSelector(name: string) {
    return jQuery.inArray(name, this.selectorList);
  }

  constructor(private currentDataService: CurrentDataService) {
    currentDataService.newAddition$.subscribe(
      data => {
        this.data = data.operatorData;
        this.operatorId = data.operatorNum;
        this.operatorTitle = data.operatorData.properties.title;
        this.attributes = [];
        for (var attribute in data.operatorData.properties.attributes) {
          this.attributes.push(attribute);
        }

        // initialize selected attributes
        this.selectedAttributeMulti = "";
        this.selectedAttributeSingle = "";
        this.selectedDictionary = "";

        if (data.operatorData.properties.attributes.attributes) {
          this.selectedAttributesList = data.operatorData.properties.attributes.attributes;
        } else if (data.operatorData.properties.attributes.attribute) {
          this.selectedAttributesList = [data.operatorData.properties.attributes.attribute]
        }
        if (data.operatorData.properties.attributes.tableName) {
          this.getAttributesForTable(data.operatorData.properties.attributes.tableName);
        }
        if (data.operatorData.properties.attributes.dictionaryEntries) {
          this.dictionaryContent = data.operatorData.properties.attributes.dictionaryEntries;
        }

      });

    currentDataService.checkPressed$.subscribe(
      data => {
        jQuery.hideLoading();
        console.log(data);
        if (data.code === 0) {
          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: JSON.parse(data.message)
          });
        } else {
          var node = new PrettyJSON.view.Node({
            el: jQuery("#elem"),
            data: {"message": data.message}
          });
        }

        this.ModalOpen();

      });

    currentDataService.metadataRetrieved$.subscribe(
      data => {
        this.metadataList = data;
        let metadata: (Array<TableMetadata>) = data;
        metadata.forEach(x => {
          this.tableNameItems.push((x.tableName));
        });
      }
    );

    currentDataService.dictionaryNames$.subscribe(
      data => {
        console.log("dict data is: ");
        console.log(data);
        this.dictionaryNames = data;
      }
    );

    currentDataService.dictionaryContent$.subscribe(
      data => {
        this.dictionaryContent = [];
        for(let entry of data){
          this.dictionaryContent.push(entry.trim());
        }
        this.data.properties.attributes.dictionaryEntries = this.dictionaryContent;
        this.onFormChange("dictionary");
      }
    );

  }

  humanize(name: string): string {
    // TODO: rewrite this function to handle camel case
    // e.g.: humanize camelCase -> camel case
    var frags = name.split('_');
    for (var i = 0; i < frags.length; i++) {
      frags[i] = frags[i].charAt(0).toUpperCase() + frags[i].slice(1);
    }
    return frags.join(' ');
  }

  onFormChange (attribute: string) {
    jQuery("#the-flowchart").flowchart("setOperatorData", this.operatorId, this.data);
  }

  onDelete() {
    this.operatorTitle = "Operator";
    this.attributes = [];
    this.dictionaryContent = [];
    jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
    this.currentDataService.setAllOperatorData(jQuery('#the-flowchart').flowchart('getData'));
  }

  attributeAdded (type: string) {
    if (type === "multi") {
      this.selectedAttributesList.push(this.selectedAttributeMulti);
      this.data.properties.attributes.attributes = this.selectedAttributesList;
      this.onFormChange("attributes");
    } else if (type === "single") {
      this.data.properties.attributes.attribute = this.selectedAttributeSingle;
      this.onFormChange("attribute");
    }
  }

  manuallyAdded (event:string) {
    if (event.length === 0) {
      // removed all attributes
      this.selectedAttributesList = [];
    } else {
      this.selectedAttributesList = event.split(",");
    }

    this.data.properties.attributes.attributes = this.selectedAttributesList;
    this.onFormChange("attributes");
  }

  getAttributesForTable (event:string) {
    if (!event) {
      return;
    }

    this.attributeItems = [];

    this.metadataList.forEach(x => {
      if (x.tableName === event) {
        x.attributes.forEach(
          y => {
            if (!y.attributeName.startsWith("_")) {
              this.attributeItems.push(y.attributeName);
            }
          });
      }
    });

    this.onFormChange("tableName");
  }

  dictionarySelected() {
    this.currentDataService.getDictionaryContent(this.selectedDictionary);
  }

  dictionaryManuallyAdded(event: string) {
    if (event.length === 0) {
      this.dictionaryContent = [];
    } else {
      this.dictionaryContent = event.split(",");
    }
    this.data.properties.attributes.dictionaryEntries = this.dictionaryContent;
    this.onFormChange("dictionary");
  }
}

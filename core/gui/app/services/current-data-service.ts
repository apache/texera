import { Injectable } from '@angular/core';
import { Subject }    from 'rxjs/Subject';
import { Response, Http } from '@angular/http';
import { Headers } from '@angular/http';
import 'rxjs/add/operator/toPromise';


import { Data } from './data';
import { TableMetadata } from "./table-metadata";
import any = jasmine.any;
import {element} from "protractor";
import {isUndefined} from "util";

declare var jQuery: any;

const apiUrl = "http://localhost:8080/api";
const texeraRunUrl = apiUrl + "/queryplan/execute";
const texeraAutoRunUrl = apiUrl + "/queryplan/autocomplete";
const tableMetadataUrl = apiUrl + "/resources/table-metadata";
const uploadDictionaryUrl = apiUrl + "/upload/dictionary";
const getDictionariesUrl = apiUrl + "/resources/dictionaries";
const getDictionaryContentUrl = apiUrl + "/resources/dictionary?name=";
const downloadExcelUrl = apiUrl + "/download/result?resultID=";

const defaultData = {
    top: 20,
    left: 20,
    properties: {
        title: 'Operator',
        inputs: {},
        outputs: {},
        attributes : {},
    }
}

@Injectable()
export class CurrentDataService {
    allOperatorData : Data;

    private newAddition = new Subject<any>();
    newAddition$ = this.newAddition.asObservable();

    private checkPressed = new Subject<any>();
    checkPressed$ = this.checkPressed.asObservable();

    private metadataRetrieved = new Subject<any>();
    metadataRetrieved$ = this.metadataRetrieved.asObservable();

    private dictionaryNames= new Subject<any>();
    dictionaryNames$ = this.dictionaryNames.asObservable();

    private dictionaryContent = new Subject<any>();
    dictionaryContent$ = this.dictionaryContent.asObservable();

    private inputSchemaContent = new Subject<any>();
    inputSchemaContent$ = this.inputSchemaContent.asObservable();

    constructor(private http: Http) { }

    setAllOperatorData(operatorData : any): void {
        this.allOperatorData = {id: 1, jsonData: operatorData};
    }

    selectData(operatorNum : number): void {
        var data_now = jQuery("#the-flowchart").flowchart("getOperatorData",operatorNum);
        this.newAddition.next({operatorNum: operatorNum, operatorData: data_now});
        this.setAllOperatorData(jQuery("#the-flowchart").flowchart("getData"));
    }

    clearAllOperatorAttributeFromCurrentSource(sourceOperatorId: number) : void {
        var data = jQuery("#the-flowchart").flowchart("getData");
        var adjacentList = [];
        for (var link in data.links) {
            if (data.links[link]["fromOperator"] == sourceOperatorId) {
                console.log("ID " + sourceOperatorId);
                var toOperatorId = data.links[link].toOperator;
                var data_now = jQuery("#the-flowchart").flowchart("getOperatorData",toOperatorId);
                if ('attributes' in data_now.properties.attributes) {
                    data_now.properties.attributes.attributes = [];
                } else if ('attribute' in data_now.properties.attributes) {
                    data_now.properties.attributes.attribute = "";
                }
                jQuery("#the-flowchart").flowchart("setOperatorData",toOperatorId, data_now);
            }
        }

    }

    clearToOperatorAttribute(linkId: number) : void {
        var data = jQuery("#the-flowchart").flowchart("getData");
        var toOperatorId = data.links[linkId].toOperator;
        var data_now = jQuery("#the-flowchart").flowchart("getOperatorData",toOperatorId);
        if ('attributes' in data_now.properties.attributes) {
            data_now.properties.attributes.attributes = [];
        } else if ('attribute' in data_now.properties.attributes) {
            data_now.properties.attributes.attribute = "";
        }
        jQuery("#the-flowchart").flowchart("setOperatorData",toOperatorId, data_now);
    }

    clearData() : void {
        this.newAddition.next({operatorNum : null, operatorData: defaultData});
    }

    processData(): object {

        let texeraJson = {operators: {}, links: {}};
        var operators = [];
        var links = [];

        var listAttributes : string[] = ["attributes", "dictionaryEntries"]

        for (var operatorIndex in this.allOperatorData.jsonData.operators) {
            var currentOperator = this.allOperatorData.jsonData['operators'];
            if (currentOperator.hasOwnProperty(operatorIndex)) {
                var attributes = {};
                attributes["operatorID"] = operatorIndex;
                for (var attribute in currentOperator[operatorIndex]['properties']['attributes']) {
                    if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)) {
                        attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
                        // if attribute is an array property, and it's not an array
                        if (jQuery.inArray(attribute, listAttributes) != -1 && ! Array.isArray(attributes[attribute])) {
                            attributes[attribute] = attributes[attribute].split(",").map((item) => item.trim());
                        }
                        // if the value is a string and can be converted to a boolean value
                        if (attributes[attribute] instanceof String && Boolean(attributes[attribute])) {
                            attributes[attribute] = (attributes[attribute].toLowerCase() === 'true')
                        }
                    }
                }
                operators.push(attributes);
            }
        }
        for(var link in this.allOperatorData.jsonData.links){
            var destination = {};
            var currentLink = this.allOperatorData.jsonData['links'];
            if (currentLink[link].hasOwnProperty("fromOperator")){
                destination["origin"] = currentLink[link]['fromOperator'].toString();
                destination["destination"] = currentLink[link]['toOperator'].toString();
                links.push(destination);
            }
        }

        texeraJson.operators = operators;
        texeraJson.links = links;

        return texeraJson;
    }

    processAutoData(): object {

        let texeraJson = {operators: {}, links: {}};
        var operators = [];
        var links = [];
        var emptyTableNameOperator = [];

        var listAttributes : string[] = ["attributes", "dictionaryEntries"]

        for (var operatorIndex in this.allOperatorData.jsonData.operators) {
            var currentOperator = this.allOperatorData.jsonData['operators'];
            if (currentOperator.hasOwnProperty(operatorIndex)) {
                var attributes = {};
                var emptyTableName = false;
                attributes["operatorID"] = operatorIndex;
                for (var attribute in currentOperator[operatorIndex]['properties']['attributes']) {
                    if (attribute === "tableName" &&
                        currentOperator[operatorIndex]['properties']['attributes'][attribute] === "") {
                        emptyTableName = true;
                        emptyTableNameOperator.push(operatorIndex);
                        break;
                    }
                    if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)) {
                        attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
                        // if attribute is an array property, and it's not an array
                        if (jQuery.inArray(attribute, listAttributes) != -1 && ! Array.isArray(attributes[attribute])) {
                            attributes[attribute] = attributes[attribute].split(",").map((item) => item.trim());
                        }
                        // if the value is a string and can be converted to a boolean value
                        if (attributes[attribute] instanceof String && Boolean(attributes[attribute])) {
                            attributes[attribute] = (attributes[attribute].toLowerCase() === 'true')
                        }
                    }
                }
                if (emptyTableName != true) {
                    operators.push(attributes);
                }
            }
        }
        for(var link in this.allOperatorData.jsonData.links){
            var destination = {};
            var currentLink = this.allOperatorData.jsonData['links'];
            if (currentLink[link].hasOwnProperty("fromOperator")){
                if (emptyTableNameOperator.indexOf(currentLink[link]['fromOperator'].toString()) === -1
                && emptyTableNameOperator.indexOf(currentLink[link]['toOperator'].toString()) === -1) {
                    destination["origin"] = currentLink[link]['fromOperator'].toString();
                    destination["destination"] = currentLink[link]['toOperator'].toString();
                    links.push(destination);
                }
            }
        }

        texeraJson.operators = operators;
        texeraJson.links = links;

        return texeraJson;
    }

    processRunData(): void {
        let texeraJson = this.processData();
        this.sendRunRequest(texeraJson);
    }

    processAutoPlanData(): void {
        let texeraJson = this.processAutoData();
        this.sendAutoRunRequest(texeraJson);
    }

    private sendRunRequest(texeraJson: any): void {

        let headers = new Headers({ 'Content-Type': 'application/json' });
        console.log("Texera JSON is:");
        console.log(JSON.stringify(texeraJson));
        this.http.post(texeraRunUrl, JSON.stringify(texeraJson), {headers: headers})
            .subscribe(
                data => {
                    this.checkPressed.next(data.json());
                },
                err => {
                    this.checkPressed.next(err.json());
                }
            );
    }

    private sendAutoRunRequest(texeraJson: any): void {
        let headers = new Headers({ 'Content-Type': 'application/json' });
        console.log("Texera JSON is:");
        console.log(JSON.stringify(texeraJson));
        this.http.post(texeraAutoRunUrl, JSON.stringify(texeraJson), {headers: headers})
            .subscribe(
                data => {
                    console.log(data.json());
                    let result = data.json();
                    if (result.code == 0) {
                        this.inputSchemaContent.next(result.result);
                    }
                },
                err => {
                    console.log(err.json());
                    this.checkPressed.next(err.json());
                }
            );
    }

    getMetadata(): void {
        let headers = new Headers({ 'Content-Type': 'application/json' });
        this.http.get(tableMetadataUrl, {headers: headers})
            .subscribe(
                data => {
                    let result = (JSON.parse(data.json().message));
                    let metadata: Array<TableMetadata> = [];
                    result.forEach((x, y) =>
                        metadata.push(new TableMetadata(x.tableName, x.schema.attributes))
                    );
                    this.metadataRetrieved.next(metadata);
                },
                err => {
                    console.log("Error at getMetadata() in current-data-service.ts \n Error: "+err);
                }
            );
    }

    uploadDictionary(file: File) {
        let formData:FormData = new FormData();
        formData.append('file', file, file.name);
        this.http.post(uploadDictionaryUrl, formData, null)
            .subscribe(
                data => {
                    alert(file.name + ' is uploaded');
                    // after adding a new dictionary, refresh the list
                    this.getDictionaries();
                },
                err => {
                    alert('Error occurred while uploading ' + file.name);
                    console.log('Error occurred while uploading ' + file.name + '\nError message: ' + err);
                }
            );
    }

    getDictionaries(): void {
        let headers = new Headers({ 'Content-Type': 'application/json' });
        this.http.get(getDictionariesUrl, {headers: headers})
            .subscribe(
                data => {
                    let result = JSON.parse(data.json().message);
                    this.dictionaryNames.next(result);
                },
                err => {
                    console.log("Error at getDictionaries() in current-data-service.ts \n Error: "+err);
                }
            );
    }

    getDictionaryContent(name: string): void {
        let headers = new Headers({ 'Content-Type': 'application/json' });
        this.http.get(getDictionaryContentUrl+name, {headers: headers})
            .subscribe(
                data => {
                    let result = (data.json().message).split(",");

                    this.dictionaryContent.next(result);
                },
                err => {
                    console.log("Error at getDictionaries() in current-data-service.ts \n Error: "+err);
                }
            );
    }

    downloadExcel(resultID: string): void {
        if (resultID === "") {
            console.log("resultID is empty")
        } else {
            console.log("proceed to http request")
            let downloadUrl: string = downloadExcelUrl + resultID;
            console.log(downloadUrl);
            this.http.get(downloadUrl).toPromise().then(function(data) {
                window.location.href = downloadUrl;
            });
        }
    }
}

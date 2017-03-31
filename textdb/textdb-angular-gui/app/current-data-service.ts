import { Injectable } from '@angular/core';
import { Subject }    from 'rxjs/Subject';
import { Response, Http } from '@angular/http';
import { Headers } from '@angular/http';

import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';


import { Data } from './data';

declare var jQuery: any;


@Injectable()
export class CurrentDataService {
    currentData : Data;
    TEXTDBJSON: any;
    returnedData: any;

    private newAddition = new Subject<any>();
    newAddition$ = this.newAddition.asObservable();

    private checkPressed = new Subject<any>();
    checkPressed$ = this.checkPressed.asObservable();


    private textdbUrl = 'http://localhost:8080/queryplan/execute';

    constructor(private http: Http) { }



    getData(): any {
        return this.currentData;
    }

    setData(data : any): void {
        this.currentData = {id: 1, jsonData: data};
    }

    addData(operatorData : any, operatorNum: number, allData : any): void {
        this.newAddition.next({operatorNum: operatorNum, operatorData: operatorData});
        this.setData(allData);
    }

    selectData(operatorNum : number): void {
      var data_now = jQuery("#the-flowchart").flowchart("getOperatorData",operatorNum);
      this.newAddition.next({operatorNum: operatorNum, operatorData: data_now});
      console.log("Current Num = " + operatorNum);

      this.setData(jQuery("#the-flowchart").flowchart("getData"));
    }

    processData(): void {
        this.TEXTDBJSON = {operators: {}, links: {}};
        var operators = [];
        var links = [];

        for (var operatorIndex in this.currentData.jsonData.operators) {
            var currentOperator = this.currentData.jsonData['operators'];
            if (currentOperator.hasOwnProperty(operatorIndex)) {
                var attributes = {};
                for (var attribute in currentOperator[operatorIndex]['properties']['attributes']) {
                    if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)) {
                        attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
                    }
                }
                operators.push(attributes);
            }
        }
        for(var link in this.currentData.jsonData.links){
            var destination = {};
            var currentLink = this.currentData.jsonData['links'];
            if (currentLink[link].hasOwnProperty("fromOperator")){
                destination["from"] = currentLink[link]['fromOperator'];
                destination["to"] = currentLink[link]['toOperator'];
                links.push(destination);
            }
        }

        this.TEXTDBJSON.operators = operators;
        this.TEXTDBJSON.links = links;
        this.sendRequest();
    }

    private sendRequest(): void {
        this.returnedData = {};
        let headers = new Headers({ 'Content-Type': 'application/json' });

        this.http.post(this.textdbUrl, JSON.stringify(this.TEXTDBJSON), {headers: headers})
            .map(this.extractData)
            .subscribe(
                data => {
                    this.returnedData = data;
                    this.checkPressed.next({returnedData: data});
                },
                err => {
                    let sampleData = {
                        operators: {
                            operator1: {
                                top: 20,
                                left: 20,
                                properties: {
                                    title: 'Operator 1',
                                    inputs: {},
                                    outputs: {
                                        output_1: {
                                            label: 'Output 1',
                                        }
                                    }
                                }
                            },
                            operator2: {
                                top: 80,
                                left: 300,
                                properties: {
                                    title: 'Operator 2',
                                    inputs: {
                                        input_1: {
                                            label: 'Input 1',
                                        },
                                        input_2: {
                                            label: 'Input 2',
                                        },
                                    },
                                    outputs: {}
                                }
                            },
                        },
                        links: {
                            link_1: {
                                fromOperator: 'operator1',
                                fromConnector: 'output_1',
                                toOperator: 'operator2',
                                toConnector: 'input_2',
                            },
                        }
                    };
                    this.checkPressed.next({returnedData: JSON.stringify(sampleData)});
                }
            );
    }

    private extractData(res: Response) {
        return res.json().data || { };
    }

    private handleError(error: any): Promise<any> {
        console.error('An error occurred', error); // for demo purposes only
        return Promise.reject(error.message || error);
    }

    getDataSlowly(): Promise<Data[]> {
        return new Promise(resolve => {
            // Simulate server latency with 2 second delay
            setTimeout(() => resolve(this.getData()), 2000);
        });
    }
}

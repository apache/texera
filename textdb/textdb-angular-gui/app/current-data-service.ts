import { Injectable } from '@angular/core';
import { Subject }    from 'rxjs/Subject';

import { Data } from './data';

@Injectable()
export class CurrentDataService {
    currentData : Data;

    private newAddition = new Subject<any>();

    newAddition$ = this.newAddition.asObservable();

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

    getDataSlowly(): Promise<Data[]> {
        return new Promise(resolve => {
            // Simulate server latency with 2 second delay
            setTimeout(() => resolve(this.getData()), 2000);
        });
    }
}
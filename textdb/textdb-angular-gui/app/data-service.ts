import { Injectable } from '@angular/core';

import { Data } from './data';
import { DATA } from './mock-data';

@Injectable()
export class DataService {
    getData(): Promise<Data[]> {
        return Promise.resolve(DATA);
    }
    getDataSlowly(): Promise<Data[]> {
        return new Promise(resolve => {
            // Simulate server latency with 2 second delay
            setTimeout(() => resolve(this.getData()), 2000);
        });
    }
}
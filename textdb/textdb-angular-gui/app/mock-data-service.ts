import { Injectable } from '@angular/core';

import { Data } from './data';
import { DEFAULT_DATA } from './mock-data';
import { DEFAULT_MATCHERS } from './mock-data';

@Injectable()
export class MockDataService {
    getData(): Promise<Data[]> {
        return Promise.resolve(DEFAULT_DATA);
    }

    getMatchers(): Promise<Data[]> {
        return Promise.resolve(DEFAULT_MATCHERS);
    }

    getDataSlowly(): Promise<Data[]> {
        return new Promise(resolve => {
            // Simulate server latency with 2 second delay
            setTimeout(() => resolve(this.getData()), 2000);
        });
    }
}

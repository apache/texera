import { TestBed, inject } from '@angular/core/testing';

import { MiniMapService } from './mini-map.service';
import { marbles } from 'rxjs-marbles';

import * as joint from 'jointjs';

describe('MiniMapService', () => {

  let miniMapService: MiniMapService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MiniMapService]
    });

    miniMapService = TestBed.get(MiniMapService);
  });

  it('should be created', inject([MiniMapService], (service: MiniMapService) => {
    expect(service).toBeTruthy();
  }));

  it('should triggle the getMiniMapInitializeStream when initializeMapPaper() is called', marbles((m) => {
    const mockMapPaper = new joint.dia.Paper({});
    m.hot('-e-').do(event => miniMapService.initializeMapPaper(mockMapPaper)).subscribe();

    const executedStream = miniMapService.getMiniMapInitializeStream().map(event => 'e');
    const expectedStream = '-e-';

    m.expect(executedStream).toBeObservable(expectedStream);

  }));
});

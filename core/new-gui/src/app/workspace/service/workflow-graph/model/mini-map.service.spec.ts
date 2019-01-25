import { TestBed, inject } from '@angular/core/testing';

import { MiniMapService } from './mini-map.service';

describe('MiniMapService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [MiniMapService]
    });
  });

  it('should be created', inject([MiniMapService], (service: MiniMapService) => {
    expect(service).toBeTruthy();
  }));
});

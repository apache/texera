import { TestBed, inject } from '@angular/core/testing';

import { DragDropServiceService } from './drag-drop-service.service';

describe('DragDropServiceService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [DragDropServiceService]
    });
  });

  it('should be created', inject([DragDropServiceService], (service: DragDropServiceService) => {
    expect(service).toBeTruthy();
  }));
});

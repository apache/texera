import { TestBed, inject } from '@angular/core/testing';

import { DragDropService } from './drag-drop.service';

describe('DragDropService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [DragDropService]
    });
  });

  it('should be created', inject([DragDropService], (service: DragDropService) => {
    expect(service).toBeTruthy();
  }));
});

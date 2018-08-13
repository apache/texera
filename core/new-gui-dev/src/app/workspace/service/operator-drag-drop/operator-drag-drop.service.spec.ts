import { TestBed, inject } from '@angular/core/testing';

import { OperatorDragDropService } from './operator-drag-drop.service';

describe('OperatorDragDropService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OperatorDragDropService]
    });
  });

  it('should be created', inject([OperatorDragDropService], (service: OperatorDragDropService) => {
    expect(service).toBeTruthy();
  }));
});

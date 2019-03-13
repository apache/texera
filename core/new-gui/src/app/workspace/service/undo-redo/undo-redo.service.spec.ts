import { TestBed, inject } from '@angular/core/testing';

import { UndoRedoService } from './undo-redo.service';

describe('UndoRedoService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UndoRedoService]
    });
  });

  it('should be created', inject([UndoRedoService], (service: UndoRedoService) => {
    expect(service).toBeTruthy();
  }));
});

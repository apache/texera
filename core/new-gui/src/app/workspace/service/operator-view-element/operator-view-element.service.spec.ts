import { TestBed, inject } from '@angular/core/testing';

import { OperatorViewElementService } from './operator-view-element.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';

describe('OperatorViewElementService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        OperatorViewElementService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
    });
  });

  it('should be created', inject([OperatorViewElementService], (service: OperatorViewElementService) => {
    expect(service).toBeTruthy();
  }));

});

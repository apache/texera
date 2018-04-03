import { TestBed, inject } from '@angular/core/testing';
import * as joint from 'jointjs';

import { OperatorViewElementService } from './operator-view-element.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { MOCK_OPERATOR_METADATA } from '../operator-metadata/mock-operator-metadata.data';

describe('OperatorViewElementService', () => {
  let service: OperatorViewElementService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        OperatorViewElementService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService}
      ],
    });
  });

  it('should be created', inject([OperatorViewElementService], (ser: OperatorViewElementService) => {
    service = ser;
    expect(service).toBeTruthy();
  }));

  it('getJointjsOperatorElement() should create an operatorElement', () => {
    const result = service.getJointjsOperatorElement('ScanSource', 'operator1', 100, 100);
    expect(result).toBeTruthy();
  });

  it('getJointjsOperatorElement() should throw an error', () => {
    const nonExistingOperator = 'NotExistOperator';
    expect(service.getJointjsOperatorElement(nonExistingOperator, 'operatorNaN', 100, 100))
    .toThrow(new Error('OperatorViewElementService.getOperatorViewElement: ' +
    'cannot find operatorType: ' + nonExistingOperator));
  });

  it('setupCustomJointjsModel() should create a custom jointjs model in constructor', () => {
    expect(joint.shapes.devs['TexeraOperatorShape']).toBeTruthy();
  });
});

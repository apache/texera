import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { JointModelService } from './jointjs-model.service';
import { WorkflowModelActionService } from './workflow-model-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';

describe('JointModelService', () => {

  const mockJointPaper = {
    pageOffset: () => {
      return { x: 50, y: 50 };
    }
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointModelService,
        WorkflowModelActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    });
  });

  it('should be created', inject([JointModelService], (service: JointModelService) => {
    expect(service).toBeTruthy();
  }));





});

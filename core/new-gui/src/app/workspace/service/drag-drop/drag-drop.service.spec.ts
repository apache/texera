import { JointUIService } from './../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { DragDropService } from './drag-drop.service';
import { JointModelService } from '../workflow-graph/model/joint-model.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';

describe('DragDropService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        JointModelService,
        WorkflowActionService,
        WorkflowUtilService,
        DragDropService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    });
  });

  it('should be created', inject([DragDropService], (service: DragDropService) => {
    expect(service).toBeTruthy();
  }));
});

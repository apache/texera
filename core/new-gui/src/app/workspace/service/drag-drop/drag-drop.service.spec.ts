import { JointUIService } from './../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { DragDropService } from './drag-drop.service';
import { JointModelService } from '../workflow-graph/model/joint-model.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { getMockOperatorMetaData } from '../operator-metadata/mock-operator-metadata.data';

import { marbles, Context } from 'rxjs-marbles';

describe('DragDropService', () => {

  let service: DragDropService;

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

    service = TestBed.get(DragDropService);
  });

  it('should be created', inject([DragDropService], (injectedService: DragDropService) => {
    expect(injectedService).toBeTruthy();
  }));


  it('should successfully register the element as draggable', marbles((m) => {

    const dragElementID = 'testing-draggable-1';
    jQuery('body').append(`<div id="${dragElementID}"></div>`);


    const operatorType = getMockOperatorMetaData().operators[0].operatorType;
    service.registerOperatorLabelDrag(dragElementID, operatorType);

    expect(jQuery('#' + dragElementID).is('.ui-draggable')).toBeTruthy();

  }));


  it('should successfully register the element as droppable', marbles((m) => {

    const dropElement = 'testing-droppable-1';
    jQuery('body').append(`<div id="${dropElement}"></div>`);

    const operatorType = getMockOperatorMetaData().operators[0].operatorType;
    service.registerWorkflowEditorDrop(dropElement);

    expect(jQuery('#' + dropElement).is('.ui-droppable')).toBeTruthy();

  }));

});

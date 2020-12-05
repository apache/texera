import { HttpClient } from '@angular/common/http';
import { inject, TestBed } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';
import { WorkflowContent } from '../../../common/type/workflow';
import { OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { JointUIService } from '../joint-ui/joint-ui.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { UndoRedoService } from '../undo-redo/undo-redo.service';
import { mockPoint, mockResultPredicate, mockScanPredicate, mockScanResultLink } from '../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { WorkflowUtilService } from '../workflow-graph/util/workflow-util.service';

import { WorkflowCacheService } from './workflow-cache.service';

describe('WorkflowCacheService', () => {
  let autoSaveWorkflowService: WorkflowCacheService;
  let workflowActionService: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowCacheService,
        WorkflowActionService,
        UndoRedoService,
        JointUIService,
        WorkflowUtilService,
        {provide: OperatorMetadataService, useClass: StubOperatorMetadataService},
        {provide: HttpClient}
      ]
    });

    // remove all items in local storage before each test
    localStorage.clear();
    autoSaveWorkflowService = TestBed.get(WorkflowCacheService);
    workflowActionService = TestBed.get(WorkflowActionService);
  });

  it('should be created', inject([WorkflowCacheService], (service: WorkflowCacheService) => {
    expect(service).toBeTruthy();
  }));

  it('should check if the local storage is updated when operator add event is triggered', marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    m.hot('-e-').do(() => workflowActionService.addOperator(mockScanPredicate, mockPoint))
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(1);
        expect(savedWorkflow.operators[0].operatorID).toEqual(mockScanPredicate.operatorID);
        expect(savedWorkflow.operators[0]).toEqual(mockScanPredicate);
        expect(savedWorkflow.operatorPositions[mockScanPredicate.operatorID]).toEqual(mockPoint);
      }
    );
  }));

  it('should check if the local storage is updated when operator delete event is triggered', marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    m.hot('-e-').do(() => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.deleteOperator(mockScanPredicate.operatorID);
      })
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(0);
      }
    );
  }));

  it('should check if the local storage is updated when link add event is triggered', marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    m.hot('-e-').do(() => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
      })
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(2);
        expect(savedWorkflow.links.length).toEqual(1);
        expect(savedWorkflow.links[0]).toEqual(mockScanResultLink);
      }
    );
  }));

  it('should check if the local storage is updated when link delete event is triggered', marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    m.hot('-e-').do(() => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        workflowActionService.deleteLink(mockScanResultLink.source, mockScanResultLink.target);
      })
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(2);
        expect(savedWorkflow.links.length).toEqual(0);
      }
    );
  }));

  it(`should check if the local storage is updated when operator delete event is triggered when there
      exists a link on the deleted operator`, marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    m.hot('-e-').do(() => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        workflowActionService.deleteOperator(mockScanPredicate.operatorID);
      })
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(1);
        expect(savedWorkflow.operators[0]).toEqual(mockResultPredicate);
        expect(savedWorkflow.links.length).toEqual(0);
      }
    );
  }));

  it('should check if the local storage is updated when operator property change event is triggered', marbles((m) => {
    autoSaveWorkflowService.registerAutoCacheWorkFlow();
    const mockProperties = {tableName: 'mockTableName'};
    m.hot('-e-').do(() => {
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, mockProperties);
      })
      .delay(100).subscribe(
      () => {
        // get items in the storage
        const savedWorkflowJson = localStorage.getItem('workflow');
        if (!savedWorkflowJson) {
          expect(false).toBeTruthy();
          return;
        }

        const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);
        expect(savedWorkflow.operators.length).toEqual(1);
        expect(savedWorkflow.operators[0].operatorProperties).toEqual(mockProperties);
      }
    );
  }));

  it('should successfully loaded what is stored inside local storage when "loadWorkflow()" is called ', marbles((m) => {
    const operatorPositions: { [key: string]: Point } = {};
    operatorPositions[mockScanPredicate.operatorID] = mockPoint;
    const operators: OperatorPredicate[] = [];
    operators.push(mockScanPredicate);
    const links: OperatorLink[] = [];

    const mockWorkflow: WorkflowContent = {
      operators, operatorPositions, links, breakpoints: {}
    };

    localStorage.setItem('workflow', JSON.stringify(mockWorkflow));

    autoSaveWorkflowService.loadWorkflow();

    const savedWorkflowJson = localStorage.getItem('workflow');
    if (!savedWorkflowJson) {
      expect(false).toBeTruthy();
      return;
    }

    const savedWorkflow: WorkflowContent = JSON.parse(savedWorkflowJson);

    expect(savedWorkflow.operators.length).toEqual(1);
    expect(savedWorkflow.operators[0]).toEqual(mockScanPredicate);

  }));
});

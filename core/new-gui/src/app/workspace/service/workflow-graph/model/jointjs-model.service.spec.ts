import { Point } from './../../../types/common.interface';
import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { TestBed, inject } from '@angular/core/testing';

import { JointModelService } from './jointjs-model.service';
import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { mockScanSourcePredicate } from './mock-workflow-data';
import { marbles } from 'rxjs-marbles';

/**
 * A mock object for Joint Paper
 * JointModelService uses its pageOffset() method.
 */
const mockJointPaper = {
  pageOffset: () => {
    return { x: 50, y: 50 };
  }
};

const mockPoint: Point = { x: 100, y: 100 };

describe('JointModelService', () => {

  describe('should react to events from workflow action', () => {

    beforeEach(() => {
      TestBed.configureTestingModule({
        providers: [
          JointModelService,
          WorkflowActionService,
          JointUIService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ]
      });

    });

    it('should be created', inject([JointModelService], (service: JointModelService) => {
      expect(service).toBeTruthy();
    }));

    it('should add an operator element when add operator is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, 'onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: mockScanSourcePredicate, point: mockPoint } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);
      jointModelService.registerJointPaper(mockJointPaper as any);

      workflowActionService.onAddOperatorAction().subscribe({
        complete: () => {
          expect(jointModelService.getJointGraph().getCell(mockScanSourcePredicate.operatorID)).toBeTruthy();
          expect(jointModelService.getJointGraph().getCell(mockScanSourcePredicate.operatorID).isElement()).toBeTruthy();
        }
      });

    }));

    it('should emit operator delete event correctly when delete operator is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, 'onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: mockScanSourcePredicate, point: mockPoint } })
      );

      spyOn(workflowActionService, 'onDeleteOperatorAction').and.returnValue(
        m.hot('--d-|', { d: { operatorID: mockScanSourcePredicate.operatorID } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);
      jointModelService.registerJointPaper(mockJointPaper as any);

      workflowActionService.onDeleteOperatorAction().subscribe({
        complete: () => {
          expect(jointModelService.getJointGraph().getCells().length).toEqual(0);
          expect(jointModelService.getJointGraph().getCell(mockScanSourcePredicate.operatorID)).toBeFalsy();
        }
      });

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('--e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

    }));

  });

  describe('should react to events from JointJS user actions from UI', () => {

    let workflowActionService: WorkflowActionService;
    let jointModelService: JointModelService;

    beforeEach(() => {
      TestBed.configureTestingModule({
        providers: [
          JointModelService,
          WorkflowActionService,
          JointUIService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ]
      });

      workflowActionService = TestBed.get(WorkflowActionService);
      jointModelService = TestBed.get(JointModelService);
      jointModelService.registerJointPaper(mockJointPaper as any);

    });

    it('should be created', inject([JointModelService], (service: JointModelService) => {
      expect(service).toBeTruthy();
    }));

    it('should emit operator delete event correctly when operator is deleted by JointJS', marbles((m) => {
      workflowActionService = TestBed.get(WorkflowActionService);
      // get Joint Model Service
      jointModelService = TestBed.get(JointModelService);
      jointModelService.registerJointPaper(mockJointPaper as any);

      workflowActionService.addOperator(mockScanSourcePredicate, mockPoint);

      m.hot('-e-').do(v => jointModelService.getJointGraph().getCell(mockScanSourcePredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('-e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);



    }));

  });


});


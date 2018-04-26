import { TestBed, inject } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';

import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { JointModelService } from './jointjs-model.service';
import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';

import { mockScanSourcePredicate } from './mock-workflow-data';
import { Point } from './../../../types/common.interface';


describe('JointModelService', () => {

  const mockPoint: Point = { x: 100, y: 100 };

  /**
   * Gets the JointJS graph object <joint.dia.Grap>) from JointModelSerivce
   * @param jointModelService
   */
  function getJointGraph(jointModelService: JointModelService): joint.dia.Graph {
    // we don't want to expose the jointGraph to be public accessible,
    //   but we need to access it in the test cases,
    //   therefore we cast it to <any> type to bypass the private constraint
    // if the jointGraph object is changed, this needs to be changed as well
    return (jointModelService as any).jointGraph;
  }

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
      // do not initialize the services in beforeEach
      // because we need to spy on them in each test case
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

      workflowActionService.onAddOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCell(mockScanSourcePredicate.operatorID)).toBeTruthy();
          expect(getJointGraph(jointModelService).getCell(mockScanSourcePredicate.operatorID).isElement()).toBeTruthy();
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

      workflowActionService.onDeleteOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCells().length).toEqual(0);
          expect(getJointGraph(jointModelService).getCell(mockScanSourcePredicate.operatorID)).toBeFalsy();
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

    });

    it('should be created', inject([JointModelService], (service: JointModelService) => {
      expect(service).toBeTruthy();
    }));

    it('should emit operator delete event correctly when operator is deleted by JointJS', marbles((m) => {

      workflowActionService.addOperator(mockScanSourcePredicate, mockPoint);

      m.hot('-e-').do(v => getJointGraph(jointModelService).getCell(mockScanSourcePredicate.operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('-e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);



    }));

  });


});


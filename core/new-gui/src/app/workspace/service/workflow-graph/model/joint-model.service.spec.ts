import { TestBed, inject } from '@angular/core/testing';
import { marbles } from 'rxjs-marbles';
import { isEqual } from 'lodash';

import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { JointModelService } from './joint-model.service';
import { WorkflowActionService } from './workflow-action.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';

import {
  getMockScanPredicate, getMockResultPredicate, getMockScanResultLink,
  getMockSentimentPredicate, getMockScanSentimentLink, getMockSentimentResultLink,
  getMockPoint
} from './mock-workflow-data';
import { Point } from './../../../types/common.interface';


describe('JointModelService', () => {

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

  /**
   * This sub-test suite tests if the module reacts to the events from internal calls of our code correctly.
   * Calling addOperator, deleteOperator, addLink, deleteLink, etc... should cause the corresponding
   *  actions to be done in JointJS.
   */
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

    it('should add an operator element when it is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: getMockScanPredicate(), point: getMockPoint() } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onAddOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID)).toBeTruthy();
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID).isElement()).toBeTruthy();
        }
      });

    }));

    it('should delete an operator correctly when it is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-|', { a: { operator: getMockScanPredicate(), point: getMockPoint() } })
      );

      spyOn(workflowActionService, '_onDeleteOperatorAction').and.returnValue(
        m.hot('--d-|', { d: { operatorID: getMockScanPredicate().operatorID } })
      );

      // get Joint Model Service
      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onDeleteOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCells().length).toEqual(0);
          expect(getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID)).toBeFalsy();
        }
      });

    }));


    it('should add an operator link correctly when it is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-b-|', {
          a: { operator: getMockScanPredicate(), point: getMockPoint() },
          b: { operator: getMockResultPredicate(), point: getMockPoint() }
        })
      );

      spyOn(workflowActionService, '_onAddLinkAction').and.returnValue(
        m.hot('----c-|', {
          c: { link: getMockScanResultLink() }
        })
      );

      const jointModelService: JointModelService = TestBed.get(JointModelService);

      workflowActionService._onAddLinkAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getLinks().length).toEqual(1);
          expect(getJointGraph(jointModelService).getCell(getMockScanResultLink().linkID)).toBeTruthy();
        }
      });
    }));

    it('should delete link correctly when it is called in workflow action', marbles((m) => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
      spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
        m.hot('-a-b-|', {
          a: { operator: getMockScanPredicate(), point: getMockPoint() },
          b: { operator: getMockResultPredicate(), point: getMockPoint() }
        })
      );

      spyOn(workflowActionService, '_onAddLinkAction').and.returnValue(
        m.hot('----c-|', {
          c: { link: getMockScanResultLink() }
        })
      );

      spyOn(workflowActionService, '_onDeleteLinkAction').and.returnValue(
        m.hot('-----d-|', {
          d: { linkID: getMockScanResultLink().linkID }
        })
      );

      const jointModelService = TestBed.get(JointModelService);
      workflowActionService._onDeleteOperatorAction().subscribe({
        complete: () => {
          expect(getJointGraph(jointModelService).getCells().length).toEqual(2);
          expect(getJointGraph(jointModelService).getLinks().length).toEqual(0);
          expect(getJointGraph(jointModelService).getCell(getMockScanResultLink().linkID)).toBeFalsy();
        }
      });
    }));

    it('should delete all the links attached to the operator when the operator is deleted',
      marbles((m) => {
        const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
        spyOn(workflowActionService, '_onAddOperatorAction').and.returnValue(
          m.hot('-a-b-c|', {
            a: { operator: getMockScanPredicate(), point: getMockPoint() },
            b: { operator: getMockResultPredicate(), point: getMockPoint() },
            c: { operator: getMockSentimentPredicate(), point: getMockPoint() }
          })
        );

        spyOn(workflowActionService, '_onAddLinkAction').and.returnValue(
          m.hot('------e-f|', {
            e: { link: getMockScanSentimentLink() },
            f: { link: getMockSentimentResultLink() }
          })
        );

        spyOn(workflowActionService, '_onDeleteOperatorAction').and.returnValue(
          m.hot('---------d|', {
            d: { operatorID: getMockSentimentPredicate().operatorID }
          })
        );

        const jointModelService: JointModelService = TestBed.get(JointModelService);

        workflowActionService._onDeleteOperatorAction().subscribe({
          complete: () => {
            expect(getJointGraph(jointModelService).getElements().length).toEqual(2);
            expect(getJointGraph(jointModelService).getCell(getMockSentimentPredicate().operatorID)).toBeFalsy();
            expect(getJointGraph(jointModelService).getLinks().length).toEqual(0);
            expect(getJointGraph(jointModelService).getCell(getMockScanSentimentLink().linkID)).toBeFalsy();
            expect(getJointGraph(jointModelService).getCell(getMockSentimentResultLink().linkID)).toBeFalsy();
          }
        });


      }));

  });

  /**
   * This sub-test suite tests if the JointJS module outputs the corresponding events properly
   *  when the actions are done through either UI or own code.
   */
  describe('should output JointJS model changes events correctly when actions happen from the UI', () => {

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

      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());

      m.hot('-e-').do(v => getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID).remove()).subscribe();

      const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
      const expectedStream = m.hot('-e-');

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);

    }));


    it('should emit link add event correctly when a link is connected by JointJS', marbles((m) => {
      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
      workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(getMockScanResultLink());

      m.hot('-e-').do(event => getJointGraph(jointModelService).addCell(mockScanResultLinkCell)).subscribe();

      const jointLinkAddStream = jointModelService.onJointLinkCellAdd().map(value => 'e');
      const expectedStream = m.hot('-e-');

      m.expect(jointLinkAddStream).toBeObservable(expectedStream);

    }));


    it('should emit link delete event correctly when a link is deleted by JointJS', marbles((m) => {
      workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
      workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());


      const mockScanResultLinkCell = JointUIService.getJointLinkCell(getMockScanResultLink());
      getJointGraph(jointModelService).addCell(mockScanResultLinkCell);

      m.hot('---e-').do(event => getJointGraph(jointModelService).getCell(getMockScanResultLink().linkID).remove()).subscribe();

      const jointLinkDeleteStream = jointModelService.onJointLinkCellDelete().map(value => 'e');
      const expectedStream = m.hot('---e-');

      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

    }));

    /**
     * When the user deletes an operator in the UI, jointJS will delete the connected links automatically.
     *
     * This test verfies that when an operator is deleted, causing the one connected link to be deleted,
     *   the JointJS event Observalbe streams are emitted correctly.
     * It should emit one operator delete event and one link delete event at the same time.
     */
    it(`should emit operator delete event and link delete event correctly
          when an operator along with one connected link are deleted by JonitJS`
      , marbles((m) => {
        workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
        workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());


        const mockScanResultLinkCell = JointUIService.getJointLinkCell(getMockScanResultLink());
        getJointGraph(jointModelService).addCell(mockScanResultLinkCell);

        m.hot('-e-').do(event => getJointGraph(jointModelService).getCell(getMockScanPredicate().operatorID).remove()).subscribe();

        const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
        const jointLinkDeleteStream = jointModelService.onJointLinkCellDelete().map(value => 'e');

        const expectedStream = '-e-';

        m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
        m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);

      }));

    /**
     *
     * This test verfies that when an operator is deleted, causing *multiple* connected links to be deleted,
     *   the JointJS event Observalbe streams are emitted correctly.
     * It should emit one operator delete event and one link delete event at the same time.
     */
    it(`should emit operator delete event and link delete event correctly when
        an operator along with multiple links are deleted by JointJS`, marbles((m) => {
        workflowActionService.addOperator(getMockScanPredicate(), getMockPoint());
        workflowActionService.addOperator(getMockSentimentPredicate(), getMockPoint());
        workflowActionService.addOperator(getMockResultPredicate(), getMockPoint());

        const mockScanSentimentLinkCell = JointUIService.getJointLinkCell(getMockScanSentimentLink());
        const mockSentimentResultLinkCell = JointUIService.getJointLinkCell(getMockSentimentResultLink());
        getJointGraph(jointModelService).addCell(mockScanSentimentLinkCell);
        getJointGraph(jointModelService).addCell(mockSentimentResultLinkCell);

        m.hot('-e--').do(event => getJointGraph(jointModelService).getCell(getMockSentimentPredicate().operatorID).remove()).subscribe();

        const jointOperatorDeleteStream = jointModelService.onJointOperatorCellDelete().map(value => 'e');
        const jointLinkDeleteStream = jointModelService.onJointLinkCellDelete().map(value => 'e');

        const expectedStream = '-e--';
        const expectedMultiStream = '-(ee)--';

        m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
        m.expect(jointLinkDeleteStream).toBeObservable(expectedMultiStream);

      }));

  });


});


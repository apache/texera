import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { JointGraphWrapper } from './../../service/workflow-graph/model/joint-graph-wrapper';
import { DragDropService } from './../../service/drag-drop/drag-drop.service';
import { WorkflowUtilService } from './../../service/workflow-graph/util/workflow-util.service';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ValidationWorkflowService } from './../../service/validation/validation-workflow.service';

import { WorkflowEditorComponent } from './workflow-editor.component';

import { marbles } from 'rxjs-marbles';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../service/operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../../service/joint-ui/joint-ui.service';
import { WorkflowGraph, WorkflowGraphReadonly } from '../../service/workflow-graph/model/workflow-graph';

import * as joint from 'jointjs';
import {
  mockScanPredicate, mockPoint, mockScanResultLink, mockResultPredicate
} from '../../service/workflow-graph/model/mock-workflow-data';

class StubWorkflowActionService {

  private jointGraph = new joint.dia.Graph();
  private jointGraphWrapper = new JointGraphWrapper(this.jointGraph);
  private readonly texeraGraph = new WorkflowGraph();

  public attachJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper.Options {
    paperOptions.model = this.jointGraph;
    return paperOptions;
  }

  public getJointGraphWrapper(): JointGraphWrapper {
    return this.jointGraphWrapper;
  }

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }
}

describe('WorkflowEditorComponent', () => {

  /**
   * This sub test suite test if the JointJS paper is integrated with our Angular component well.
   * It uses a fake stub Workflow model that only provides the binding of JointJS graph.
   * It tests if manipulating the JointJS graph is correctly shown in the UI.
   */
  describe('JointJS Paper', () => {
    let component: WorkflowEditorComponent;
    let fixture: ComponentFixture<WorkflowEditorComponent>;
    let jointGraph: joint.dia.Graph;

    beforeEach(async(() => {
      TestBed.configureTestingModule({
        declarations: [WorkflowEditorComponent],
        providers: [
          JointUIService,
          WorkflowUtilService,
          DragDropService,
          ValidationWorkflowService,
          { provide: WorkflowActionService, useClass: StubWorkflowActionService },
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
        ]
      })
        .compileComponents();
    }));

    beforeEach(() => {
      fixture = TestBed.createComponent(WorkflowEditorComponent);
      component = fixture.componentInstance;
      // detect changes first to run ngAfterViewInit and bind Model
      fixture.detectChanges();
      jointGraph = component.getJointPaper().model;
    });

    it('should create', () => {
      expect(component).toBeTruthy();
    });

    it('should create element in the UI after adding operator in the model', () => {
      const operatorID = 'test_one_operator_1';

      const element = new joint.shapes.basic.Rect();
      element.set('id', operatorID);

      jointGraph.addCell(element);

      expect(component.getJointPaper().findViewByModel(element.id)).toBeTruthy();
    });

    it('should create a graph of multiple cells in the UI', () => {
      const operator1 = 'test_multiple_1_op_1';
      const operator2 = 'test_multiple_1_op_2';

      const element1 = new joint.shapes.basic.Rect({
        size: { width: 100, height: 50 },
        position: { x: 100, y: 400 }
      });
      element1.set('id', operator1);

      const element2 = new joint.shapes.basic.Rect({
        size: { width: 100, height: 50 },
        position: { x: 100, y: 400 }
      });
      element2.set('id', operator2);

      const link1 = new joint.dia.Link({
        source: { id: operator1 },
        target: { id: operator2 }
      });

      jointGraph.addCell(element1);
      jointGraph.addCell(element2);
      jointGraph.addCell(link1);

      // check the model is added correctly
      expect(jointGraph.getElements().find(el => el.id === operator1)).toBeTruthy();
      expect(jointGraph.getElements().find(el => el.id === operator2)).toBeTruthy();
      expect(jointGraph.getLinks().find(link => link.id === link1.id)).toBeTruthy();


      // check the view is updated correctly
      expect(component.getJointPaper().findViewByModel(element1.id)).toBeTruthy();
      expect(component.getJointPaper().findViewByModel(element2.id)).toBeTruthy();
      expect(component.getJointPaper().findViewByModel(link1.id)).toBeTruthy();
    });

  });

  /**
   * This sub test suites test the Integration of WorkflowEditorComponent with external modules,
   *  such as drag and drop module, and highlight operator module.
   */
  describe('External Module Integration', () => {

    let component: WorkflowEditorComponent;
    let fixture: ComponentFixture<WorkflowEditorComponent>;
    let workflowActionService: WorkflowActionService;
<<<<<<< HEAD
    let validationWorkflowService: ValidationWorkflowService;
=======
    let dragDropService: DragDropService;

>>>>>>> 04058d4456852b8b8943aa7a09124f2ae0df9ff7
    beforeEach(async(() => {
      TestBed.configureTestingModule({
        declarations: [WorkflowEditorComponent],
        providers: [
          JointUIService,
          WorkflowUtilService,
          WorkflowActionService,
<<<<<<< HEAD
          ValidationWorkflowService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
=======
          DragDropService,
          { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
>>>>>>> 04058d4456852b8b8943aa7a09124f2ae0df9ff7
        ]
      })
        .compileComponents();
    }));

    beforeEach(() => {
      fixture = TestBed.createComponent(WorkflowEditorComponent);
      component = fixture.componentInstance;
      workflowActionService = TestBed.get(WorkflowActionService);
<<<<<<< HEAD
      validationWorkflowService = TestBed.get(ValidationWorkflowService);
=======
      dragDropService = TestBed.get(DragDropService);
>>>>>>> 04058d4456852b8b8943aa7a09124f2ae0df9ff7
      // detect changes to run ngAfterViewInit and bind Model
      fixture.detectChanges();
    });

    it('should register itself as a droppable element', () => {
      const jqueryElement = jQuery(`#${component.WORKFLOW_EDITOR_JOINTJS_ID}`);
      expect(jqueryElement.data('uiDroppable')).toBeTruthy();
    });

    it('should try to highlight the operator when user mouse clicks on an operator', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      // install a spy on the highlight operator function and pass the call through
      const highlightOperatorFunctionSpy = spyOn(jointGraphWrapper, 'highlightOperator').and.callThrough();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);

      // find the joint Cell View object of the operator element
      const jointCellView = component.getJointPaper().findViewByModel(mockScanPredicate.operatorID);

      // tirgger a click on the cell view using its jQuery element
      jointCellView.$el.trigger('mousedown');

      fixture.detectChanges();

      // assert the function is called once
      expect(highlightOperatorFunctionSpy.calls.count()).toEqual(1);
      // assert the highlighted operator is correct
      expect(jointGraphWrapper.getCurrentHighlightedOpeartorID()).toEqual(mockScanPredicate.operatorID);
    });

    it('should react to operator highlight event and change the appearance of the operator to be highlighted', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);

      // highlight the operator
      jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

      // find the joint Cell View object of the operator element
      const jointCellView = component.getJointPaper().findViewByModel(mockScanPredicate.operatorID);

      // find the cell's child element with the joint highlighter class name `joint-highlight-stroke`
      const jointHighlighterElements = jointCellView.$el.children('.joint-highlight-stroke');

      // the element should have the highlighter element in it
      expect(jointHighlighterElements.length).toEqual(1);
    });

    it('should react to operator unhighlight event and change the appearance of the operator to be unhighlighted', () => {
      const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
      workflowActionService.addOperator(mockScanPredicate, mockPoint);

      // highlight the oprator first
      jointGraphWrapper.highlightOperator(mockScanPredicate.operatorID);

      // find the joint Cell View object of the operator element
      const jointCellView = component.getJointPaper().findViewByModel(mockScanPredicate.operatorID);

      // find the cell's child element with the joint highlighter class name `joint-highlight-stroke`
      const jointHighlighterElements = jointCellView.$el.children('.joint-highlight-stroke');

      // the element should have the highlighter element in it right now
      expect(jointHighlighterElements.length).toEqual(1);

      // then unhighlight the operator
      jointGraphWrapper.unhighlightCurrent();

      // the highlighter element should not exist
      const jointHighlighterElementAfterUnhighlight = jointCellView.$el.children('.joint-highlight-stroke');
      expect(jointHighlighterElementAfterUnhighlight.length).toEqual(0);
    });

<<<<<<< HEAD
    it('should react to operator validation and change the color of operator box if the operator is valid ',
         () => {
    const jointGraphWrapper = workflowActionService.getJointGraphWrapper();
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);
    const newProperty = { 'tableName': 'test-table' };
    workflowActionService.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    const operator1 = component.getJointPaper().getModelById(mockScanPredicate.operatorID);
    const operator2 = component.getJointPaper().getModelById(mockResultPredicate.operatorID);
    expect(operator1.attr('rect/stroke')).toEqual('#CFCFCF');
    expect(operator2.attr('rect/stroke')).toEqual('#CFCFCF');

=======
    it('should react to jointJS paper zoom event', marbles((m) => {
      const mockScaleRatio = 0.5;
      m.hot('-e-').do(() => workflowActionService.getJointGraphWrapper().setZoomProperty(mockScaleRatio)).subscribe(
        () => {
          const currentScale = component.getJointPaper().scale();
          expect(currentScale.sx).toEqual(mockScaleRatio);
          expect(currentScale.sy).toEqual(mockScaleRatio);
        }
      );
    }));

    it('should react to jointJS paper restore default offset event', marbles((m) => {
      const mockTranslation = 20;
      const originalOffset = component.getJointPaper().translate();
      component.getJointPaper().translate(mockTranslation, mockTranslation);
      expect(component.getJointPaper().translate().tx).not.toEqual(originalOffset.tx);
      expect(component.getJointPaper().translate().ty).not.toEqual(originalOffset.ty);
      m.hot('-e-').do(() => workflowActionService.getJointGraphWrapper().restoreDefaultZoomAndOffset()).subscribe(
        () => {
          expect(component.getJointPaper().translate().tx).toEqual(originalOffset.tx);
          expect(component.getJointPaper().translate().ty).toEqual(originalOffset.ty);
        }
      );
    }));
>>>>>>> 04058d4456852b8b8943aa7a09124f2ae0df9ff7

  });



  });


});

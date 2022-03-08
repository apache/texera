import { JointUIService } from "../joint-ui/joint-ui.service";
import { inject, TestBed } from "@angular/core/testing";
import { DragDropService } from "./drag-drop.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { mockOperatorMetaData } from "../operator-metadata/mock-operator-metadata.data";
import * as jQuery from "jquery";
import "../../../../../node_modules/jquery-ui-dist/jquery-ui";
import { marbles } from "rxjs-marbles";
import {
  mockMultiInputOutputPredicate,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { map } from "rxjs/operators";
import { VIEW_RESULT_OP_TYPE } from "../workflow-graph/model/workflow-graph";

describe("DragDropService", () => {
  let dragDropService: DragDropService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        UndoRedoService,
        WorkflowUtilService,
        DragDropService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });

    dragDropService = TestBed.get(DragDropService);

    // custom equality disregards link ID (since I use DragDropService.getNew)
    jasmine.addCustomEqualityTester((link1: OperatorLink, link2: OperatorLink) => {
      if (typeof link1 === "object" && typeof link2 === "object") {
        if (link1.source === link2.source && link1.target === link2.target) {
          return true;
        } else {
          return false;
        }
      }
    });
  });

  it("should be created", inject([DragDropService], (injectedService: DragDropService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should successfully register the element as draggable", () => {
    const dragElementID = "testing-draggable-1";
    jQuery("body").append(`<div id="${dragElementID}"></div>`);

    const operatorType = mockOperatorMetaData.operators[0].operatorType;
    dragDropService.registerOperatorLabelDrag(dragElementID, operatorType);

    expect(jQuery("#" + dragElementID).is(".ui-draggable")).toBeTruthy();
  });

  it("should successfully register the element as droppable", () => {
    const dropElement = "testing-droppable-1";
    jQuery("body").append(`<div id="${dropElement}"></div>`);

    dragDropService.registerWorkflowEditorDrop(dropElement);

    expect(jQuery("#" + dropElement).is(".ui-droppable")).toBeTruthy();
  });

  it(
    "should add an operator when the element is dropped",
    marbles(m => {
      const operatorType = mockOperatorMetaData.operators[0].operatorType;

      const marbleString = "-a-|";
      const marbleValues = {
        a: {
          operatorType: operatorType,
          offset: { x: 100, y: 100 },
          dragElementID: "mockID",
        },
      };

      spyOn(dragDropService, "getOperatorDropStream").and.returnValue(m.hot(marbleString, marbleValues));

      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

      dragDropService.handleOperatorDropEvent();

      const addOperatorStream = workflowActionService
        .getTexeraGraph()
        .getOperatorAddStream()
        .pipe(map(() => "a"));

      const expectedStream = m.hot("-a-");
      m.expect(addOperatorStream).toBeObservable(expectedStream);
    })
  );

  it("should successfully create a new operator link given 2 operator predicates", () => {
    const createdLink: OperatorLink = (dragDropService as any).getNewOperatorLink(
      mockScanPredicate,
      mockResultPredicate
    );

    expect(createdLink.source).toEqual(mockScanResultLink.source);
    expect(createdLink.target).toEqual(mockScanResultLink.target);
  });

  it("should find 3 input operatorPredicates and 3 output operatorPredicates for an operatorPredicate with 3 input / 3 output ports", () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const workflowUtilService: WorkflowUtilService = TestBed.get(WorkflowUtilService);

    const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const [inputOps, outputOps] = (dragDropService as any).findClosestOperators(
      { x: 50, y: 0 },
      mockMultiInputOutputPredicate
    );

    workflowActionService.addOperator(input1, { x: 0, y: 0 });
    workflowActionService.addOperator(input2, { x: 0, y: 10 });
    workflowActionService.addOperator(input3, { x: 0, y: 20 });
    workflowActionService.addOperator(output1, { x: 100, y: 0 });
    workflowActionService.addOperator(output2, { x: 100, y: 10 });
    workflowActionService.addOperator(output3, { x: 100, y: 20 });

    expect(inputOps).toEqual([input1, input2, input3]);
    expect(outputOps).toEqual([output1, output2, output3]);
  });

  it('should publish operatorPredicates to highlight streams when calling "updateHighlighting(prevHighlights,newHighlights)"', async () => {
    const workflowUtilService: WorkflowActionService = TestBed.get(WorkflowActionService);
    const highlights: string[] = [];
    const unhighlights: string[] = [];
    const expectedHighlights = [mockScanPredicate.operatorID, mockScanPredicate.operatorID];
    const expectedUnhighlights = [mockScanPredicate.operatorID, mockResultPredicate.operatorID];
    // allow test to run for 10ms before checking, since observables are async
    const timeout = new Promise(resolve => setTimeout(resolve, 10));

    dragDropService.getOperatorSuggestionHighlightStream().subscribe(operatorID => {
      highlights.push(operatorID);
    });
    dragDropService.getOperatorSuggestionUnhighlightStream().subscribe(operatorID => {
      unhighlights.push(operatorID);
    });

    // highlighting update situations
    (dragDropService as any).updateHighlighting([mockScanPredicate], [mockScanPredicate]); // no change
    (dragDropService as any).updateHighlighting([], [mockScanPredicate]); // new highlight
    (dragDropService as any).updateHighlighting([mockScanPredicate], []); // new unhighlight
    (dragDropService as any).updateHighlighting([mockResultPredicate], [mockScanPredicate]); // new highlight and unhighlight

    // allow test to run for up to 500ms before checking, since observables are async
    await timeout;
    expect(highlights).toEqual(expectedHighlights);
    expect(unhighlights).toEqual(expectedUnhighlights);
  });

  it("should not find any operator when the mouse coordinate is greater than the threshold defined", () => {
    const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);

    workflowActionService.addOperator(mockScanPredicate, { x: 0, y: 0 });

    const [inputOps, outputOps] = (dragDropService as any).findClosestOperators(
      {
        x: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
        y: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
      },
      mockResultPredicate
    );

    expect(inputOps).toEqual([]);
  });

  it(
    "should update highlighting, add operator, and add links when an operator is dropped",
    marbles(async m => {
      const workflowActionService: WorkflowActionService = TestBed.get(WorkflowActionService);
      const workflowUtilService: WorkflowUtilService = TestBed.get(WorkflowUtilService);
      const graph = workflowActionService.getJointGraphWrapper();

      const operatorType = "MultiInputOutput";
      const operator = mockMultiInputOutputPredicate;

      const marbleString = "-e-|";
      const marbleValues = {
        e: {
          operatorType: operatorType,
          offset: { x: 1005, y: 1001 },
          dragElementID: "mockID",
        },
      };

      const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const heightSortedInputs: OperatorPredicate[] = [input1, input2, input3];
      const heightSortedOutputs: OperatorPredicate[] = [output1, output2, output3];

      // lists to be populated by observables/streams
      const highlights: string[] = [];
      const unhighlights: string[] = [];
      const links: OperatorLink[] = [];
      // expected end results of above lists
      const expectedHighlights: OperatorPredicate[] = []; // expected empty
      const expectedUnhighlights = [
        input1.operatorID,
        input2.operatorID,
        input3.operatorID,
        output1.operatorID,
        output2.operatorID,
        output3.operatorID,
      ];
      const expectedLinks: OperatorLink[] = []; // NOT EXPECTED EMPTY: populated below

      // populate expected links.
      heightSortedInputs.forEach(inputOperator => {
        expectedLinks.push((dragDropService as any).getNewOperatorLink(inputOperator, operator, expectedLinks));
      });
      heightSortedOutputs.forEach(outputOperator => {
        expectedLinks.push((dragDropService as any).getNewOperatorLink(operator, outputOperator, expectedLinks));
      });

      const timeout = new Promise(resolve => setTimeout(resolve, 500)); // await 500ms before checking expect(s), since observables are async

      // add operators to graph
      workflowActionService.addOperator(input1, { x: 0, y: 10 });
      workflowActionService.addOperator(input2, { x: 0, y: 20 });
      workflowActionService.addOperator(input3, { x: 0, y: 30 });
      workflowActionService.addOperator(output1, { x: 100, y: 10 });
      workflowActionService.addOperator(output2, { x: 100, y: 20 });
      workflowActionService.addOperator(output3, { x: 100, y: 30 });

      // subscribe to streams and push them to lists (in order to populate highlights,unhighlights,links)
      dragDropService.getOperatorSuggestionHighlightStream().subscribe(operatorID => {
        highlights.push(operatorID);
      });
      dragDropService.getOperatorSuggestionUnhighlightStream().subscribe(operatorID => {
        unhighlights.push(operatorID);
      });
      workflowActionService
        .getTexeraGraph()
        .getLinkAddStream()
        .subscribe(link => {
          links.push(link);
        });

      // replace dragDropService.getOperatorDropStream: observable with fake Marble observable that publishes only marbleValues['e']
      spyOn(dragDropService, "getOperatorDropStream").and.returnValue(m.hot(marbleString, marbleValues));

      // since dragDropService.getOperatorDropStream is replaced by Marble observable, will drop marbleValues['e']
      dragDropService.handleOperatorDropEvent();

      // use 500 ms promise to wait for async events to finish executing
      await timeout;
      expect(highlights).toEqual(expectedHighlights as any);
      expect(unhighlights).toEqual(expectedUnhighlights as any);
      expect(links).toEqual(expectedLinks); // depends on custom jasmine equality comparison function, defined at top in beforeEach{...}
    })
  );
});

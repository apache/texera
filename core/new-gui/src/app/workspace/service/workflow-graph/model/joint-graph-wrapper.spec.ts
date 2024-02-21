import { WorkflowActionService } from "./workflow-action.service";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { TestBed } from "@angular/core/testing";
import { marbles } from "rxjs-marbles";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "./mock-workflow-data";
import * as joint from "jointjs";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { environment } from "../../../../../environments/environment";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { map, share, tap } from "rxjs/operators";

describe("JointGraphWrapperService", () => {
  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;
  let jointUIService: JointUIService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });
    jointGraph = new joint.dia.Graph();
    jointGraphWrapper = new JointGraphWrapper(jointGraph);
    jointUIService = TestBed.inject(JointUIService);
  });

  it(
    "should emit operator delete event correctly when operator is deleted by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));

      m.hot("-e-")
        .pipe(tap(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const expectedStream = m.hot("-e-");

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should emit link add event correctly when a link is connected by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);

      m.hot("-e-")
        .pipe(tap(() => jointGraph.addCell(mockScanResultLinkCell)))
        .subscribe();

      const jointLinkAddStream = jointGraphWrapper.getJointLinkCellAddStream().pipe(map(() => "e"));
      const expectedStream = m.hot("-e-");

      m.expect(jointLinkAddStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should emit link delete event correctly when a link is deleted by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
      jointGraph.addCell(mockScanResultLinkCell);

      m.hot("---e-")
        .pipe(tap(() => jointGraph.getCell(mockScanResultLink.linkID).remove()))
        .subscribe();

      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));
      const expectedStream = m.hot("---e-");

      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);
    })
  );

  /**
   * When the user deletes an operator in the UI, jointJS will delete the connected links automatically.
   *
   * This test verifies that when an operator is deleted, causing the one connected link to be deleted,
   *   the JointJS event Observable streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(
    `should emit operator delete event and link delete event correctly
          when an operator along with one connected link are deleted by JointJS`,
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
      jointGraph.addCell(mockScanResultLinkCell);

      m.hot("-e-")
        .pipe(tap(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));

      const expectedStream = "-e-";

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);
    })
  );

  /**
   *
   * This test verifies that when an operator is deleted, causing *multiple* connected links to be deleted,
   *   the JointJS event Observalbe streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(
    `should emit operator delete event and link delete event correctly when
        an operator along with multiple links are deleted by JointJS`,
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockSentimentPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanSentimentLinkCell = JointUIService.getJointLinkCell(mockScanSentimentLink);
      const mockSentimentResultLinkCell = JointUIService.getJointLinkCell(mockSentimentResultLink);
      jointGraph.addCell(mockScanSentimentLinkCell);
      jointGraph.addCell(mockSentimentResultLinkCell);

      m.hot("-e--")
        .pipe(tap(() => jointGraph.getCell(mockSentimentPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));

      const expectedStream = "-e--";
      const expectedMultiStream = "-(ee)--";

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedMultiStream);
    })
  );

  it(
    "should emit a highlight event correctly when an operator is highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add one operator, it should be automatically highlighted
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([
        mockScanPredicate.operatorID,
      ]);
      // unhighlight the current operator
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockScanPredicate.operatorID);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([]);

      // prepare marble operation for highlighting an operator
      const highlightActionMarbleEvent = m.hot("-a-|", { a: mockScanPredicate.operatorID }).pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operator is correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);
        },
      });
    })
  );

  it(
    "should emit a highlight event correctly when multiple operators are highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add two operators, they should be automatically highlighted
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([
        mockScanPredicate.operatorID,
        mockResultPredicate.operatorID,
      ]);

      // unhighlight current operators
      workflowActionService
        .getJointGraphWrapper()
        .unhighlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([]);

      // prepare marble operation for highlighting two operators
      const highlightActionMarbleEvent = m
        .hot("-a-|", {
          a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
        })
        .pipe(share());

      // highlight those operators at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(...value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operators are correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([
            mockScanPredicate.operatorID,
            mockResultPredicate.operatorID,
          ]);
        },
      });
    })
  );

  it(
    "should emit an unhighlight event correctly when an operator is unhighlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight an operator
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

      // prepare marble operation for unhighlighting an operator
      const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

      // unhighlight that operator at events
      unhighlightActionMarbleEvent.subscribe(() =>
        localJointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID)
      );

      // prepare expected output unhighlight event stream
      const expectedUnhighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(
        expectedUnhighlightEventStream
      );

      // expect no operator is currently highlighted
      unhighlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
        },
      });
    })
  );

  it(
    "should emit an unhighlight event correctly when multiple operators are unhighlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight two operators
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      workflowActionService
        .getJointGraphWrapper()
        .highlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);

      // prepare marble operation for unhighlighting two operators
      const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

      // unhighlight those operators at events
      unhighlightActionMarbleEvent.subscribe(() =>
        localJointGraphWrapper.unhighlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID)
      );

      // prepare expected output unhighlight event stream
      const expectedUnhighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(
        expectedUnhighlightEventStream
      );

      // expect no operator is currently highlighted
      unhighlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
        },
      });
    })
  );

  it(
    "should unhighlight previous highlighted operator if a new operator is highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);

      // unhighlight the last operator in case of automatic highlight
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockResultPredicate.operatorID);

      // prepare marble operation for highlighting one operator, then highlight another
      const highlightActionMarbleEvent = m
        .hot("-a-b-|", {
          a: mockScanPredicate.operatorID,
          b: mockResultPredicate.operatorID,
        })
        .pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-b-", {
        a: [mockScanPredicate.operatorID],
        b: [mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operator is correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockResultPredicate.operatorID]);
        },
      });
    })
  );

  it(
    "should ignore the action if trying to highlight the same currently highlighted operator",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add an operator, it should be automatically highlighted
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      // unhighlight it
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockScanPredicate.operatorID);

      // prepare marble operation for highlighting the same operator twice
      const highlightActionMarbleEvent = m
        .hot("-a-b-|", {
          a: mockScanPredicate.operatorID,
          b: mockScanPredicate.operatorID,
        })
        .pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream: the second highlight is ignored
      const expectedHighlightEventStream = m.hot("-a---", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);
    })
  );

  it(
    "should unhighlight the currently highlighted operator if it is deleted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight the operator
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      localJointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

      // prepare the delete operator action marble test
      const deleteOperatorActionMarble = m.hot("-a-").pipe(share());
      deleteOperatorActionMarble.subscribe(() => workflowActionService.deleteOperator(mockScanPredicate.operatorID));

      // expect that the unhighlight event stream is triggered
      const expectedEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(expectedEventStream);

      // expect that the current highlighted operator is undefined
      deleteOperatorActionMarble.subscribe({
        complete: () => expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]),
      });
    })
  );

  it("should get operator position successfully if the operator exists in the paper", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(mockPoint);
  });

  it("should throw an error if operator does not exist in the paper when calling 'getElementPosition()'", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    expect(function () {
      localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID);
    }).toThrowError(`element with ID ${mockScanPredicate.operatorID} doesn't exist`);
  });

  it("should throw an error if the id we are using is linkID when calling 'getElementPosition()'", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    expect(function () {
      localJointGraphWrapper.getElementPosition(mockScanResultLink.linkID);
    }).toThrowError(`${mockScanResultLink.linkID} is not an element`);
  });

  it("should repositions the operator successfully if the operator exists in the paper", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    // changes the operator's position
    localJointGraphWrapper.setElementPosition(mockScanPredicate.operatorID, 10, 10);

    const expectedPosition = { x: mockPoint.x + 10, y: mockPoint.y + 10 };
    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(expectedPosition);
  });

  it("should successfully set a new zoom property", () => {
    const mockNewZoomProperty = 0.5;

    let currentZoomRatio = jointGraphWrapper.getZoomRatio();
    expect(currentZoomRatio).toEqual(1);

    jointGraphWrapper.setZoomProperty(mockNewZoomProperty);
    currentZoomRatio = jointGraphWrapper.getZoomRatio();
    expect(currentZoomRatio).toEqual(mockNewZoomProperty);
  });

  it(
    "should triggle getWorkflowEditorZoomStream when new zoom ratio is set",
    marbles(m => {
      const mockNewZoomProperty = 0.5;

      m.hot("-e-")
        .pipe(tap(event => jointGraphWrapper.setZoomProperty(mockNewZoomProperty)))
        .subscribe();
      const zoomStream = jointGraphWrapper.getWorkflowEditorZoomStream().pipe(map(value => "e"));
      const expectedStream = "-e-";

      m.expect(zoomStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should trigger getRestorePaperOffsetStream when resumeDefaultZoomAndOffset is called",
    marbles(m => {
      m.hot("-e-")
        .pipe(tap(() => jointGraphWrapper.restoreDefaultZoomAndOffset()))
        .subscribe();
      const restoreStream = jointGraphWrapper.getRestorePaperOffsetStream().pipe(map(value => "e"));
      const expectedStream = "-e-";

      m.expect(restoreStream).toBeObservable(expectedStream);
    })
  );

  it("should move all highlighted operators together when any one of them is moved", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockResultPredicate, pos: mockPoint },
      ],
      []
    );
    localJointGraphWrapper.highlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // change one operator's position
    localJointGraphWrapper.setElementPosition(mockScanPredicate.operatorID, 10, 10);

    const expectedPosition = { x: mockPoint.x + 10, y: mockPoint.y + 10 };

    // expect both operators to be in the new position
    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(expectedPosition);
    expect(localJointGraphWrapper.getElementPosition(mockResultPredicate.operatorID)).toEqual(expectedPosition);
  });

  describe("when linkBreakpoint is enabled", () => {
    beforeAll(() => {
      environment.linkBreakpointEnabled = true;
    });

    afterAll(() => {
      environment.linkBreakpointEnabled = false;
    });

    it(
      "should emit link highlight event correctly when a link is selected",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m.hot("-a-|", { a: mockScanResultLink.linkID }).pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          localJointGraphWrapper.highlightLink(value);
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);

        // expect the current highlighted link to be correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanResultLink.linkID]);
          },
        });
      })
    );

    it(
      "should emit an unhighlight event correctly when an link is unhighlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        // add one operator
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        // highlight the operator
        localJointGraphWrapper.highlightLink(mockScanResultLink.linkID);

        // prepare marble operation for unhighlighting an operator
        const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

        // unhighlight that operator at events
        unhighlightActionMarbleEvent.subscribe(() => localJointGraphWrapper.unhighlightLink(mockScanResultLink.linkID));

        // prepare expected output highlight event stream
        const expectedUnhighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedUnhighlightEventStream);

        // expect the current highlighted operator is correct
        unhighlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
          },
        });
      })
    );

    it(
      "should emit an unhighlight event correctly when an highlighted link is deleted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        // add one operator
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        // highlight the operator
        localJointGraphWrapper.highlightLink(mockScanResultLink.linkID);

        // prepare marble operation for unhighlighting an operator
        const deleteActionMarbleEvent = m.hot("-a-|").pipe(share());

        // unhighlight that operator at events
        deleteActionMarbleEvent.subscribe(() => workflowActionService.deleteLinkWithID(mockScanResultLink.linkID));

        // prepare expected output highlight event stream
        const expectedUnhighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedUnhighlightEventStream);

        // expect the current highlighted operator is correct
        deleteActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
          },
        });
      })
    );

    it(
      "should unhighlight previous highlighted link if another link is selected/highlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanSentimentLink);
        workflowActionService.addLink(mockSentimentResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m
          .hot("-a-b-|", {
            a: mockScanSentimentLink.linkID,
            b: mockSentimentResultLink.linkID,
          })
          .pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          localJointGraphWrapper.highlightLink(value);
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a-b-", {
          a: [mockScanSentimentLink.linkID],
          b: [mockSentimentResultLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);

        // prepare expected output unhighlight event stream
        const expectedLinUnhighlightEventStream = m.hot("---a-", {
          a: [mockScanSentimentLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedLinUnhighlightEventStream);

        // expect the current highlighted link to be correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockSentimentResultLink.linkID]);
          },
        });
      })
    );

    it(
      "should unhighlight previous highlighted links if an operator is highlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m
          .hot("-a-b-|", {
            a: mockScanResultLink.linkID,
            b: mockResultPredicate.operatorID,
          })
          .pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          if (value === mockResultPredicate.operatorID) {
            localJointGraphWrapper.highlightOperators(value);
          } else {
            localJointGraphWrapper.highlightLink(value);
          }
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a---", {
          a: [mockScanResultLink.linkID],
        });

        const expectedOperatorHighlightEventStream = m.hot("---b-", {
          b: [mockResultPredicate.operatorID],
        });

        // prepare expected output highlight event stream
        const expectedLinkUnhighlightEventStream = m.hot("---c-", {
          c: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);
        m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(
          expectedOperatorHighlightEventStream
        );
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedLinkUnhighlightEventStream);

        // expect the current highlighted operator is correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockResultPredicate.operatorID]);
          },
        });
      })
    );
  });
});

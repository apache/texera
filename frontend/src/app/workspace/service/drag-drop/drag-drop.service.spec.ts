/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { JointUIService } from "../joint-ui/joint-ui.service";
import { inject, TestBed } from "@angular/core/testing";
import { DragDropService } from "./drag-drop.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { marbles } from "rxjs-marbles";
import {
  mockMultiInputOutputPredicate,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { VIEW_RESULT_OP_TYPE } from "../workflow-graph/model/workflow-graph";
import { commonTestProviders } from "../../../common/testing/test-utils";

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
        ...commonTestProviders,
      ],
    });

    dragDropService = TestBed.inject(DragDropService);

    // custom equality disregards link ID (since I use DragDropService.getNew)
    /* TODO(vitest): no equivalent — port via expect.extend */ ((..._args: unknown[]) => {})(
      (link1: OperatorLink, link2: OperatorLink) => {
        if (typeof link1 === "object" && typeof link2 === "object") {
          return link1.source === link2.source && link1.target === link2.target;
        }
      }
    );
  });

  it("should be created", inject([DragDropService], (injectedService: DragDropService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should successfully create a new operator link given 2 operator predicates", () => {
    const createdLink: OperatorLink = (dragDropService as any).getNewOperatorLink(
      mockScanPredicate,
      mockResultPredicate
    );

    expect(createdLink.source).toEqual(mockScanResultLink.source);
    expect(createdLink.target).toEqual(mockScanResultLink.target);
  });

  it("should find 3 input operatorPredicates and 3 output operatorPredicates for an operatorPredicate with 3 input / 3 output ports", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);

    const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);

    workflowActionService.addOperator(input1, { x: 0, y: 0 });
    workflowActionService.addOperator(input2, { x: 0, y: 10 });
    workflowActionService.addOperator(input3, { x: 0, y: 20 });
    workflowActionService.addOperator(output1, { x: 100, y: 0 });
    workflowActionService.addOperator(output2, { x: 100, y: 10 });
    workflowActionService.addOperator(output3, { x: 100, y: 20 });

    // Probe at the centroid between the input and output columns. With the
    // SUGGESTION_DISTANCE_THRESHOLD = 300, all 6 operators are in range; the
    // 3 to the left are ranked as inputs, the 3 to the right as outputs.
    // Order within each list is heap-internal and not guaranteed by the
    // implementation — assert membership only.
    const [inputOps, outputOps] = (dragDropService as any).findClosestOperators(
      { x: 50, y: 0 },
      mockMultiInputOutputPredicate
    );

    expect(inputOps).toHaveLength(3);
    expect(inputOps).toEqual(expect.arrayContaining([input1, input2, input3]));
    expect(outputOps).toHaveLength(3);
    expect(outputOps).toEqual(expect.arrayContaining([output1, output2, output3]));
  });

  it('should publish operatorPredicates to highlight streams when calling "updateHighlighting(prevHighlights,newHighlights)"', async () => {
    TestBed.inject(WorkflowActionService);
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
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);

    workflowActionService.addOperator(mockScanPredicate, { x: 0, y: 0 });

    const [inputOps] = (dragDropService as any).findClosestOperators(
      {
        x: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
        y: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
      },
      mockResultPredicate
    );

    expect(inputOps).toEqual([]);
  });

  it("should add the dropped operator with links to suggested neighbors and unhighlight prior suggestions", async () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);
    const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);

    // Real main jointjs paper attached to a hidden DOM host so coordinate
    // transforms in `dragStarted` / mousemove / `dragDropped` resolve
    // without stubs. jsdom doesn't compute layout, so the SVG polyfill's
    // identity matrices collapse `pageToLocalPoint(x, y)` to (0, 0)
    // regardless of input — that's why operators are placed at x=±100
    // around the origin below.
    const paperHost = document.createElement("div");
    const flyingOpHost = document.createElement("div");
    flyingOpHost.id = "flyingOP";
    document.body.appendChild(paperHost);
    document.body.appendChild(flyingOpHost);
    try {
      workflowActionService.getJointGraphWrapper().attachMainJointPaper({ el: paperHost });

      // Inputs at negative x and outputs at positive x so the (0, 0) drop
      // point classifies them correctly via `findClosestOperators` (which
      // compares operator x against mouse x).
      workflowActionService.addOperator(input1, { x: -100, y: 10 });
      workflowActionService.addOperator(input2, { x: -100, y: 20 });
      workflowActionService.addOperator(input3, { x: -100, y: 30 });
      workflowActionService.addOperator(output1, { x: 100, y: 10 });
      workflowActionService.addOperator(output2, { x: 100, y: 20 });
      workflowActionService.addOperator(output3, { x: 100, y: 30 });

      const unhighlights: string[] = [];
      dragDropService.getOperatorSuggestionUnhighlightStream().subscribe(id => unhighlights.push(id));
      const links: OperatorLink[] = [];
      workflowActionService
        .getTexeraGraph()
        .getLinkAddStream()
        .subscribe(link => links.push(link));

      // dragStarted creates a fresh `op` of the given type and subscribes
      // to window mousemove to populate suggestionInputs / suggestionOutputs.
      dragDropService.dragStarted("MultiInputOutput");
      const droppedOp = (dragDropService as any).op as OperatorPredicate;

      // Drive the suggestion pipeline. Any mousemove will do — jsdom's
      // `pageToLocalPoint` collapses to (0, 0) regardless of the
      // dispatched coordinates.
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 0, clientY: 0 }));
      await new Promise(resolve => setTimeout(resolve, 0));

      dragDropService.dragDropped({ x: 0, y: 0 });
      // Tear down the window-level mousemove subscriptions installed by
      // `dragStarted`. Without this the `first()` mouseup observer stays
      // armed and a stray mousemove from a later spec re-enters this
      // service's suggestion pipeline.
      window.dispatchEvent(new MouseEvent("mouseup"));
      await new Promise(resolve => setTimeout(resolve, 0));

      // Each suggested operator should have been unhighlighted at drop time.
      expect(unhighlights).toEqual(
        expect.arrayContaining([
          input1.operatorID,
          input2.operatorID,
          input3.operatorID,
          output1.operatorID,
          output2.operatorID,
          output3.operatorID,
        ])
      );
      expect(unhighlights).toHaveLength(6);

      // 3 input→droppedOp links and 3 droppedOp→output links.
      expect(links).toHaveLength(6);
      const inputLinks = links.filter(l => l.target.operatorID === droppedOp.operatorID);
      const outputLinks = links.filter(l => l.source.operatorID === droppedOp.operatorID);
      expect(inputLinks.map(l => l.source.operatorID).sort()).toEqual(
        [input1.operatorID, input2.operatorID, input3.operatorID].sort()
      );
      expect(outputLinks.map(l => l.target.operatorID).sort()).toEqual(
        [output1.operatorID, output2.operatorID, output3.operatorID].sort()
      );
    } finally {
      // Always clean up the DOM hosts even if an assertion above threw,
      // so the JointJS papers don't leak into later specs.
      document.body.removeChild(paperHost);
      document.body.removeChild(flyingOpHost);
    }
  });

  // Defensive cleanup: dragStarted-based specs create a `#flyingOP` host and
  // arm window-level mousemove/mouseup subscriptions. Each such spec disarms
  // itself via a dispatched mouseup, but if an assertion throws first, this
  // removes any leftover host so a later spec's dragStarted doesn't reuse it.
  afterEach(() => {
    document.getElementById("flyingOP")?.remove();
  });

  describe("doesOperatorIntersectPath (bounding-box vs. path geometry)", () => {
    const makePath = (length: number, pointFn: (len: number) => { x: number; y: number }): SVGPathElement =>
      ({
        getTotalLength: () => length,
        getPointAtLength: (len: number) => pointFn(len),
      }) as unknown as SVGPathElement;

    it("returns true when a sampled path point falls inside the operator bounding box", () => {
      const bounds = { x: 0, y: 0, width: 200, height: 140 };
      // Points march along y = 50 from x = 0 to x = 100, all within the box.
      const path = makePath(100, len => ({ x: len, y: 50 }));

      expect((dragDropService as any).doesOperatorIntersectPath(bounds, path)).toBe(true);
    });

    it("returns false when every sampled path point is outside the operator bounding box", () => {
      const bounds = { x: 0, y: 0, width: 100, height: 100 };
      const path = makePath(300, () => ({ x: 500, y: 500 }));

      expect((dragDropService as any).doesOperatorIntersectPath(bounds, path)).toBe(false);
    });

    it("clamps the number of path samples to a minimum of 5 and a maximum of 20", () => {
      // A box far off-screen guarantees no point ever matches, forcing the
      // full sample sweep so the sample count is observable.
      const bounds = { x: -1000, y: -1000, width: 1, height: 1 };

      // floor(40 / 20) = 2, clamped up to the minimum of 5 → 5 samples → 6 probes (i = 0..5).
      const shortPath = makePath(40, () => ({ x: 9999, y: 9999 }));
      const shortSpy = vi.spyOn(shortPath, "getPointAtLength");
      (dragDropService as any).doesOperatorIntersectPath(bounds, shortPath);
      expect(shortSpy).toHaveBeenCalledTimes(6);

      // floor(10000 / 20) = 500, clamped down to the maximum of 20 → 20 samples → 21 probes.
      const longPath = makePath(10000, () => ({ x: 9999, y: 9999 }));
      const longSpy = vi.spyOn(longPath, "getPointAtLength");
      (dragDropService as any).doesOperatorIntersectPath(bounds, longPath);
      expect(longSpy).toHaveBeenCalledTimes(21);
    });
  });

  describe("createEdgeReconnectionLinks", () => {
    const addOpChain = () => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);
      const scan = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const result = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      workflowActionService.addOperator(scan, { x: 0, y: 0 });
      workflowActionService.addOperator(result, { x: 100, y: 0 });
      const origLink: OperatorLink = {
        linkID: workflowUtilService.getLinkRandomUUID(),
        source: { operatorID: scan.operatorID, portID: scan.outputPorts[0].portID },
        target: { operatorID: result.operatorID, portID: result.inputPorts[0].portID },
      };
      workflowActionService.addLink(origLink);
      return { workflowActionService, workflowUtilService, scan, result, origLink };
    };

    it("splices a new operator into an intersected edge, deleting the original link and returning source→new and new→target links", () => {
      const { workflowActionService, workflowUtilService, scan, result, origLink } = addOpChain();
      const newOp = workflowUtilService.getNewOperatorPredicate("NlpSentiment"); // has both input + output ports

      const newLinks: OperatorLink[] = (dragDropService as any).createEdgeReconnectionLinks(newOp, origLink);

      // The intersected link is removed from the graph.
      expect(
        workflowActionService
          .getTexeraGraph()
          .getAllLinks()
          .map(l => l.linkID)
      ).not.toContain(origLink.linkID);

      // Two reconnection links are returned that thread the new operator into the edge.
      expect(newLinks).toHaveLength(2);
      const srcToNew = newLinks.find(l => l.source.operatorID === scan.operatorID);
      const newToTgt = newLinks.find(l => l.target.operatorID === result.operatorID);

      expect(srcToNew).toBeDefined();
      expect(srcToNew!.source.portID).toBe(origLink.source.portID);
      expect(srcToNew!.target).toEqual({ operatorID: newOp.operatorID, portID: newOp.inputPorts[0].portID });

      expect(newToTgt).toBeDefined();
      expect(newToTgt!.source).toEqual({ operatorID: newOp.operatorID, portID: newOp.outputPorts[0].portID });
      expect(newToTgt!.target.portID).toBe(origLink.target.portID);
    });

    it("falls back to suggestion links and leaves the edge intact when the new operator has no output port", () => {
      const { workflowActionService, workflowUtilService, origLink } = addOpChain();
      // A View-Results operator has an input port but no output port.
      const sinkOnly = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const deleteSpy = vi.spyOn(workflowActionService, "deleteLinkWithID");

      const newLinks: OperatorLink[] = (dragDropService as any).createEdgeReconnectionLinks(sinkOnly, origLink);

      // No suggestions are set, so the fallback produces no links...
      expect(newLinks).toEqual([]);
      // ...and, crucially, the original edge is NOT deleted in the fallback path.
      expect(deleteSpy).not.toHaveBeenCalled();
      expect(
        workflowActionService
          .getTexeraGraph()
          .getAllLinks()
          .map(l => l.linkID)
      ).toContain(origLink.linkID);
    });
  });

  describe("dragDropped edge-drop routing and operatorDropStream", () => {
    it("routes through edge reconnection and emits operatorDropStream when the drop lands on an existing edge", () => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);

      const paperHost = document.createElement("div");
      document.body.appendChild(paperHost);
      try {
        workflowActionService.getJointGraphWrapper().attachMainJointPaper({ el: paperHost });

        const scan = workflowUtilService.getNewOperatorPredicate("ScanSource");
        const result = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
        workflowActionService.addOperator(scan, { x: -100, y: 0 });
        workflowActionService.addOperator(result, { x: 100, y: 0 });
        const origLink: OperatorLink = {
          linkID: workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: scan.operatorID, portID: scan.outputPorts[0].portID },
          target: { operatorID: result.operatorID, portID: result.inputPorts[0].portID },
        };
        workflowActionService.addLink(origLink);

        // Set the ghost operator directly and force the drop onto the edge, so
        // the intersected-link branch of dragDropped is taken.
        const newOp = workflowUtilService.getNewOperatorPredicate("NlpSentiment");
        (dragDropService as any).op = newOp;
        vi.spyOn(dragDropService as any, "findIntersectedLink").mockReturnValue(origLink);

        let dropped = false;
        dragDropService.operatorDropStream.subscribe(() => (dropped = true));

        dragDropService.dragDropped({ x: 0, y: 0 });

        expect(dropped).toBe(true);
        expect(workflowActionService.getTexeraGraph().hasOperator(newOp.operatorID)).toBe(true);

        const links = workflowActionService.getTexeraGraph().getAllLinks();
        expect(links.map(l => l.linkID)).not.toContain(origLink.linkID);
        expect(links).toHaveLength(2);
        expect(
          links.some(l => l.source.operatorID === scan.operatorID && l.target.operatorID === newOp.operatorID)
        ).toBe(true);
        expect(
          links.some(l => l.source.operatorID === newOp.operatorID && l.target.operatorID === result.operatorID)
        ).toBe(true);
      } finally {
        document.body.removeChild(paperHost);
      }
    });
  });

  describe("edge-intersection highlighting helpers", () => {
    it("toggles the intersected link's stroke attributes between highlighted and default", () => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);
      const paperHost = document.createElement("div");
      document.body.appendChild(paperHost);
      try {
        workflowActionService.getJointGraphWrapper().attachMainJointPaper({ el: paperHost });
        const scan = workflowUtilService.getNewOperatorPredicate("ScanSource");
        const result = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
        workflowActionService.addOperator(scan, { x: 0, y: 0 });
        workflowActionService.addOperator(result, { x: 100, y: 0 });
        const link: OperatorLink = {
          linkID: workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: scan.operatorID, portID: scan.outputPorts[0].portID },
          target: { operatorID: result.operatorID, portID: result.inputPorts[0].portID },
        };
        workflowActionService.addLink(link);

        const paper = workflowActionService.getJointGraphWrapper().getMainJointPaper();
        const jointLink = paper.getModelById(link.linkID);
        expect(jointLink).toBeDefined();

        (dragDropService as any).highlightEdgeIntersection(link);
        expect((jointLink.attr(".connection") as any).stroke).toBe("#FF6B35");
        expect((jointLink.attr(".marker-source") as any).fill).toBe("#FF6B35");
        expect((jointLink.attr(".marker-target") as any).fill).toBe("#FF6B35");

        (dragDropService as any).clearEdgeIntersectionHighlight(link);
        expect((jointLink.attr(".connection") as any).stroke).toBe("#848484");
        expect((jointLink.attr(".marker-source") as any).fill).toBe("none");
        expect((jointLink.attr(".marker-target") as any).fill).toBe("none");

        // A link whose ID is not in the graph resolves to no joint model and is a safe no-op.
        expect(() =>
          (dragDropService as any).highlightEdgeIntersection({ linkID: "ghost" } as OperatorLink)
        ).not.toThrow();
        expect(() =>
          (dragDropService as any).clearEdgeIntersectionHighlight({ linkID: "ghost" } as OperatorLink)
        ).not.toThrow();
      } finally {
        document.body.removeChild(paperHost);
      }
    });

    it("no-ops safely (highlight/clear/findIntersectedLink) when no main paper is attached", () => {
      const link = { linkID: "nonexistent" } as OperatorLink;
      expect(() => (dragDropService as any).highlightEdgeIntersection(link)).not.toThrow();
      expect(() => (dragDropService as any).clearEdgeIntersectionHighlight(link)).not.toThrow();
      expect((dragDropService as any).findIntersectedLink({ x: 0, y: 0 })).toBeNull();
    });
  });

  describe("handleOperatorRecommendationOnDrag (drag lifecycle)", () => {
    it("uses fallback mouse coordinates and unit scale when no paper is attached during mousemove", async () => {
      const flyingOpHost = document.createElement("div");
      flyingOpHost.id = "flyingOP";
      document.body.appendChild(flyingOpHost);
      const findClosestSpy = vi.spyOn(dragDropService as any, "findClosestOperators");
      try {
        // No main joint paper is attached, so pageToLocalPoint()/scale() are
        // undefined and the mousemove handler must fall back to raw client
        // coordinates and a { sx: 1, sy: 1 } scale.
        dragDropService.dragStarted("NlpSentiment");
        window.dispatchEvent(new MouseEvent("mousemove", { clientX: 42, clientY: 24 }));
        await new Promise(resolve => setTimeout(resolve, 0));

        expect(findClosestSpy).toHaveBeenCalled();
        const lastCall = findClosestSpy.mock.calls[findClosestSpy.mock.calls.length - 1];
        expect(lastCall[0]).toEqual({ x: 42, y: 24 });
      } finally {
        window.dispatchEvent(new MouseEvent("mouseup"));
        await new Promise(resolve => setTimeout(resolve, 0));
        document.body.removeChild(flyingOpHost);
      }
    });

    it("highlights, re-highlights, and clears intersected edges across mousemove and mouseup", async () => {
      const flyingOpHost = document.createElement("div");
      flyingOpHost.id = "flyingOP";
      document.body.appendChild(flyingOpHost);

      const linkA = { linkID: "edge-A" } as OperatorLink;
      const linkB = { linkID: "edge-B" } as OperatorLink;
      const highlightSpy = vi.spyOn(dragDropService as any, "highlightEdgeIntersection").mockImplementation(() => {});
      const clearSpy = vi.spyOn(dragDropService as any, "clearEdgeIntersectionHighlight").mockImplementation(() => {});
      const intersectSpy = vi
        .spyOn(dragDropService as any, "findIntersectedLink")
        .mockReturnValueOnce(linkA)
        .mockReturnValueOnce(linkB);
      try {
        // NlpSentiment has both input and output ports, so the edge-insertion
        // feedback path is active.
        dragDropService.dragStarted("NlpSentiment");

        window.dispatchEvent(new MouseEvent("mousemove", { clientX: 1, clientY: 1 }));
        await new Promise(resolve => setTimeout(resolve, 0));
        // First intersection: highlight linkA.
        expect(highlightSpy).toHaveBeenNthCalledWith(1, linkA);

        window.dispatchEvent(new MouseEvent("mousemove", { clientX: 2, clientY: 2 }));
        await new Promise(resolve => setTimeout(resolve, 0));
        // Intersection changed A→B: clear the previous A, highlight the new B.
        expect(clearSpy).toHaveBeenNthCalledWith(1, linkA);
        expect(highlightSpy).toHaveBeenNthCalledWith(2, linkB);

        window.dispatchEvent(new MouseEvent("mouseup"));
        await new Promise(resolve => setTimeout(resolve, 0));
        // Drag ends while B is highlighted → mouseup clears the active highlight.
        expect(clearSpy).toHaveBeenNthCalledWith(2, linkB);

        expect(intersectSpy).toHaveBeenCalledTimes(2);
      } finally {
        window.dispatchEvent(new MouseEvent("mouseup"));
        await new Promise(resolve => setTimeout(resolve, 0));
        document.body.removeChild(flyingOpHost);
      }
    });
  });

  describe("findClosestOperators (priority-queue selection and free-port filtering)", () => {
    it("keeps only the nearest free-port operator per side and excludes operators with no free ports", () => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);

      // Probe operator with exactly one input and one output port, so each
      // side's priority queue overflows (forcing greatestDistance + pop) once
      // a second closer candidate appears.
      const probe = workflowUtilService.getNewOperatorPredicate("NlpSentiment");

      const srcFar = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const srcNear = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const srcOccupied = workflowUtilService.getNewOperatorPredicate("ScanSource");
      const sinkFar = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const sinkNear = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
      const dummy = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);

      // Insertion order matters: adding the farther candidate first, then the
      // nearer one, drives the "queue is full, is this closer than the current
      // max?" branch and the subsequent pop.
      workflowActionService.addOperator(srcFar, { x: -100, y: 0 });
      workflowActionService.addOperator(srcNear, { x: -10, y: 0 });
      workflowActionService.addOperator(srcOccupied, { x: -20, y: 0 });
      workflowActionService.addOperator(sinkFar, { x: 100, y: 0 });
      workflowActionService.addOperator(sinkNear, { x: 10, y: 0 });
      // Placed beyond SUGGESTION_DISTANCE_THRESHOLD so it is never a candidate;
      // it only exists to occupy srcOccupied's single output port.
      workflowActionService.addOperator(dummy, { x: 1000, y: 1000 });

      // Occupy srcOccupied's only output port so hasFreeOutputPorts() rejects it.
      workflowActionService.addLink({
        linkID: workflowUtilService.getLinkRandomUUID(),
        source: { operatorID: srcOccupied.operatorID, portID: srcOccupied.outputPorts[0].portID },
        target: { operatorID: dummy.operatorID, portID: dummy.inputPorts[0].portID },
      });

      const [inputOps, outputOps] = (dragDropService as any).findClosestOperators({ x: 0, y: 0 }, probe) as [
        OperatorPredicate[],
        OperatorPredicate[],
      ];

      expect(inputOps.map(o => o.operatorID)).toEqual([srcNear.operatorID]);
      expect(outputOps.map(o => o.operatorID)).toEqual([sinkNear.operatorID]);
    });
  });
});

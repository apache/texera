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

// import { mockResultPredicate, mockPoint } from './../workflow-graph/model/mock-workflow-data';
// import { TestBed, inject } from '@angular/core/testing';
// import * as joint from 'jointjs';

// import { JointUIService, deleteButtonPath, sourceOperatorHandle, targetOperatorHandle } from './joint-ui.service';
// import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
// import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
// import { mockScanPredicate, mockSentimentPredicate } from '../workflow-graph/model/mock-workflow-data';
// import { mockScanStatistic1, mockScanStatistic2 } from '../workflow-status/mock-workflow-status';

// describe('JointUIService', () => {
//   let service: JointUIService;

//   beforeEach(() => {
//     TestBed.configureTestingModule({
//       providers: [
//         JointUIService,
//         { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
//       ],
//     });
//     service = TestBed.get(JointUIService);
//   });

//   it('should be created', inject([JointUIService], (injectedService: JointUIService) => {
//     expect(injectedService).toBeTruthy();
//   }));

//   /**
//    * Check if the getJointOperatorElement() can successfully creates a JointJS Element
//    */
//   it('should create an JointJS Element successfully when the function is called', () => {
//     const result = service.getJointOperatorElement(
//       mockScanPredicate, mockPoint);
//     expect(result).toBeTruthy();
//   });

//   /**
//    * Check if the error in getJointOperatorElement() is correctly thrown
//    */
//   it('should throw an error with an non existing operator', () => {
//     expect(() => {
//       service.getJointOperatorElement(
//         {
//           operatorID: 'nonexistOperator',
//           operatorType: 'nonexistOperatorType',
//           operatorProperties: {},
//           inputPorts: [],
//           outputPorts: [],
//           showAdvanced: true
//         },
//         mockPoint
//       );
//     }).toThrowError(new RegExp(`doesn't exist`));
//   });

//   /**
//    * Check if the getJointTooltipElement() can successfully creates a JointJS Element
//    */
//   it('should create an JointJS Element successfully when the function is called', () => {
//     const result = service.getJointOperatorStatusTooltipElement(
//       mockScanPredicate, mockPoint);
//     expect(result).toBeTruthy();
//   });

//   /**
//    * Check if the error in getJointTooltipElement() is correctly thrown
//    */
//   it('should throw an error with an non existing operator', () => {
//     expect(() => {
//       service.getJointOperatorStatusTooltipElement(
//         {
//           operatorID: 'nonexistOperator',
//           operatorType: 'nonexistOperatorType',
//           operatorProperties: {},
//           inputPorts: [],
//           outputPorts: [],
//           showAdvanced: true
//         },
//         mockPoint
//       );
//     }).toThrowError(new RegExp(`doesn't exist`));
//   });

//   /**
//    * Check if showTooltip/hideTooltip works properly
//    */
//   it('should reveal/hide tooltip and its content when showToolTip/hideTooltip is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     // tooltip should not be shown when operator is just created
//     // disply attr should be none
//     const tooltipId = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
//     const graph_tooltip1 = jointGraph.getCell(tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toEqual('none');
//     // showTooltip removes display == none attr to show tooltip
//     service.showOperatorStatusToolTip(paper, tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toBeUndefined();
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toBeUndefined();
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toBeUndefined();
//     // hideTooltip adds display == none attr to hide tooltip
//     service.hideOperatorStatusToolTip(paper, tooltipId);
//     expect(graph_tooltip1.attr('polygon')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorCount')['display']).toEqual('none');
//     expect(graph_tooltip1.attr('#operatorSpeed')['display']).toEqual('none');
//   });

//   /**
//    * check if tooltip content can be updated properly
//    */
//   it('should update the content in the tooltip when changeOperatorTooltipInfo is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     const tooltipId = JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID);
//     const graph_tooltip = jointGraph.getCell(tooltipId);
//     // tooltip should not contain any content when created
//     expect(graph_tooltip.attr('#operatorCount')['text']).toBeUndefined();
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toBeUndefined();
//     // updating it with mock statistics
//     service.changeOperatorStatusTooltipInfo(paper, tooltipId, mockScanStatistic1);
//     expect(graph_tooltip.attr('#operatorCount')['text']).toEqual('Output:' + mockScanStatistic1.outputCount + ' tuples');
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toEqual('Speed:' + mockScanStatistic1.speed + ' tuples/s');
//     // updating it with another mock statistics
//     service.changeOperatorStatusTooltipInfo(paper, tooltipId, mockScanStatistic2);
//     expect(graph_tooltip.attr('#operatorCount')['text']).toEqual('Output:' + mockScanStatistic2.outputCount + ' tuples');
//     expect(graph_tooltip.attr('#operatorSpeed')['text']).toEqual('Speed:' + mockScanStatistic2.speed + ' tuples/s');
//   });

//   it('should change the operator state name and color when changeOperatorStates is called', () => {
//     // creating a JointJS graph with one operator and its tooltip
//     const jointGraph = new joint.dia.Graph();
//     const jointPaperOptions: joint.dia.Paper.Options = {model: jointGraph};
//     const paper = new joint.dia.Paper(jointPaperOptions);

//     jointGraph.addCell(
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//     ));

//     // operator state name and color should be changed correctly
//     const graph_operator = jointGraph.getCell(mockScanPredicate.operatorID);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Ready');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('green');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Initializing);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Initializing');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('orange');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Running);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Running');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('orange');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Pausing);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Pausing');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('red');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Paused);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Paused');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('red');
//     service.changeOperatorStates(paper, mockScanPredicate.operatorID, OperatorStates.Completed);
//     expect(graph_operator.attr('#operatorStates')['text']).toEqual('Completed');
//     expect(graph_operator.attr('#operatorStates')['fill']).toEqual('green');
//   });

//   /**
//    * Check if the number of inPorts and outPorts created by getJointOperatorElement()
//    * matches the port number specified by the operator metadata
//    */
//   it('should create correct number of inPorts and outPorts based on operator metadata', () => {
//     const element1 = service.getJointOperatorElement(mockScanPredicate, mockPoint);
//     const element2 = service.getJointOperatorElement(mockSentimentPredicate, mockPoint);
//     const element3 = service.getJointOperatorElement(mockResultPredicate, mockPoint);

//     const inPortCount1 = element1.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount1 = element1.getPorts().filter(port => port.group === 'out').length;
//     const inPortCount2 = element2.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount2 = element2.getPorts().filter(port => port.group === 'out').length;
//     const inPortCount3 = element3.getPorts().filter(port => port.group === 'in').length;
//     const outPortCount3 = element3.getPorts().filter(port => port.group === 'out').length;

//     expect(inPortCount1).toEqual(0);
//     expect(outPortCount1).toEqual(1);
//     expect(inPortCount2).toEqual(1);
//     expect(outPortCount2).toEqual(1);
//     expect(inPortCount3).toEqual(1);
//     expect(outPortCount3).toEqual(0);

//   });

//   /**
//    * Check if the custom attributes / svgs are correctly used by the JointJS graph
//    */
//   it('should apply the custom SVG styling to the JointJS element', () => {

//     const graph = new joint.dia.Graph();
//     // operator and its tooltip element should be added together
//     graph.addCell([
//       service.getJointOperatorElement(
//         mockScanPredicate,
//         mockPoint
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockScanPredicate,
//         mockPoint
//       )
//     ]);
//     graph.addCell([
//       service.getJointOperatorElement(
//         mockResultPredicate,
//         { x: 500, y: 100 }
//       ),
//       service.getJointOperatorStatusTooltipElement(
//         mockResultPredicate,
//         { x: 500, y: 100 }
//       )
//       ]);

//     const link = JointUIService.getJointLinkCell({
//       linkID: 'link-1',
//       source: { operatorID: 'operator1', portID: 'out0' },
//       target: { operatorID: 'operator2', portID: 'in0' },
//     });

//     graph.addCell(link);

//     const graph_operator1 = graph.getCell(mockScanPredicate.operatorID);
//     const graph_operator2 = graph.getCell(mockResultPredicate.operatorID);
//     const graph_link = graph.getLinks()[0];
//     const graph_tooltip1 = graph.getCell(JointUIService.getOperatorStatusTooltipElementID(mockScanPredicate.operatorID));

//     // testing getCustomTooltipStyleAttrs()
//     // style: {'pointer-events': 'none'} makes tooltip unselectable thus not draggable
//     expect(graph_tooltip1.attr('polygon')).toEqual({
//       fill: '#FFFFFF', 'follow-scale': true, stroke: 'purple', 'stroke-width': '2',
//         rx: '5px', ry: '5px', refPoints: '0,30 150,30 150,120 85,120 75,150 65,120 0,120',
//         display: 'none', style: {'pointer-events': 'none'}
//     });
//     expect(graph_tooltip1.attr('#operatorCount')).toEqual({
//       fill: '#595959', 'font-size': '12px', ref: 'polygon',
//       'y-alignment': 'middle',
//       'x-alignment': 'left',
//       'ref-x': .05, 'ref-y': .2,
//       display: 'none', style: {'pointer-events': 'none'}
//     });
//     expect(graph_tooltip1.attr('#operatorSpeed')).toEqual({
//       fill: '#595959',
//       ref: 'polygon',
//       'x-alignment': 'left',
//       'font-size': '12px',
//       'ref-x': .05, 'ref-y': .5,
//       display: 'none', style: {'pointer-events': 'none'}
//     });

//     // testing getCustomOperatorStyleAttrs()
//     expect(graph_operator1.attr('#operatorStates')).toEqual({
//       text:  'Ready' , fill: 'green', 'font-size': '14px', 'visible' : false,
//       'ref-x': 0.5, 'ref-y': -10, ref: 'rect', 'y-alignment': 'middle', 'x-alignment': 'middle'
//     });
//     expect(graph_operator1.attr('rect')).toEqual(
//       { fill: '#FFFFFF', 'follow-scale': true, stroke: 'red', 'stroke-width': '2',
//       rx: '5px', ry: '5px' }
//     );
//     expect(graph_operator2.attr('rect')).toEqual(
//       { fill: '#FFFFFF', 'follow-scale': true, stroke: 'red', 'stroke-width': '2',
//       rx: '5px', ry: '5px' }
//     );
//     expect(graph_operator1.attr('.delete-button')).toEqual(
//       {
//         x: 60, y: -20, cursor: 'pointer',
//         fill: '#D8656A', event: 'element:delete'
//       }
//     );
//     expect(graph_operator2.attr('.delete-button')).toEqual(
//       {
//         x: 60, y: -20, cursor: 'pointer',
//         fill: '#D8656A', event: 'element:delete'
//       }
//     );

//     // testing getDefaultLinkElement()
//     expect(graph_link.attr('.marker-source/d')).toEqual(sourceOperatorHandle);
//     expect(graph_link.attr('.marker-target/d')).toEqual(targetOperatorHandle);
//     expect(graph_link.attr('.tool-remove path/d')).toEqual(deleteButtonPath);
//   });
// });

import { of } from "rxjs";
import * as joint from "jointjs";
import { JointUIService, operatorNameClass } from "./joint-ui.service";
import { OperatorPredicate } from "../../types/workflow-common.interface";

describe("JointUIService", () => {
  // Pre-existing spec body is commented out. Placeholder keeps Vitest's
  // discovery happy; rewriting the real tests against the new test
  // runner is tracked in #4861.
  it.todo("add unit tests for JointUIService");

  describe("truncateOperatorDisplayName", () => {
    // Deterministic measurer: 10px per character. With the 200-px budget,
    // 20 chars fit exactly; longer strings get truncated to a prefix plus "…".
    const measure = (text: string) => text.length * 10;
    const budget = JointUIService.MAX_OPERATOR_NAME_PIXELS;
    const charsThatFit = budget / 10;

    it("returns the name unchanged when it fits within the pixel budget", () => {
      const name = "a".repeat(charsThatFit);
      expect(JointUIService.truncateOperatorDisplayName(name, measure)).toBe(name);
    });

    it("truncates and appends an ellipsis when the name exceeds the budget", () => {
      const name = "a".repeat(charsThatFit + 10);
      const result = JointUIService.truncateOperatorDisplayName(name, measure);
      expect(result.endsWith("…")).toBe(true);
      expect(measure(result)).toBeLessThanOrEqual(budget);
      // Ellipsis takes 10px, leaving 190px for the prefix → 19 chars.
      expect(result).toBe("a".repeat(charsThatFit - 1) + "…");
    });

    it("returns an empty string unchanged", () => {
      expect(JointUIService.truncateOperatorDisplayName("", measure)).toBe("");
    });

    it("truncates CJK characters at code-point boundaries", () => {
      // CJK characters are each a single code point (UTF-16 length 1) — the
      // 10-px measurer treats them like any other char. 19 chars fit in the
      // 190-px prefix budget once the ellipsis is reserved.
      const name = "你".repeat(charsThatFit + 5);
      const result = JointUIService.truncateOperatorDisplayName(name, measure);
      expect(result).toBe("你".repeat(charsThatFit - 1) + "…");
      expect(measure(result)).toBeLessThanOrEqual(budget);
    });

    it("truncates emoji at grapheme boundaries (no orphan surrogates)", () => {
      // 🎉 is U+1F389, a single grapheme but a UTF-16 surrogate pair (length 2).
      // With the 10-px-per-code-unit measurer each 🎉 costs 20 px.
      const name = "🎉".repeat(20);
      const result = JointUIService.truncateOperatorDisplayName(name, measure);
      // Prefix budget 190 / 20 px per emoji = 9 full emojis kept.
      expect(result).toBe("🎉".repeat(9) + "…");
      // Result must be re-iterable as the same set of grapheme clusters —
      // i.e. no half-surrogate at the boundary.
      const segments = Array.from(result);
      expect(segments).toEqual([..."🎉".repeat(9), "…"]);
    });

    it("keeps a ZWJ grapheme cluster (family emoji) intact when truncating", () => {
      // 👨‍👩‍👧‍👦 is one grapheme cluster but 11 UTF-16 code units (4 emojis joined
      // by 3 ZWJ chars). With the 10-px measurer each family costs 110 px,
      // so the 190-px prefix budget keeps exactly one family.
      const name = "👨‍👩‍👧‍👦".repeat(5);
      const result = JointUIService.truncateOperatorDisplayName(name, measure);
      // Skip the strict assertion if Intl.Segmenter isn't available; the
      // code-point fallback would split the cluster, which we cannot avoid
      // without the segmenter.
      const hasSegmenter = typeof Intl !== "undefined" && typeof Intl.Segmenter === "function";
      if (hasSegmenter) {
        expect(result).toBe("👨‍👩‍👧‍👦" + "…");
      }
      expect(result.endsWith("…")).toBe(true);
    });

    it("falls back to code-point iteration when Intl.Segmenter is unavailable", () => {
      const intlAsAny = Intl as unknown as { Segmenter?: typeof Intl.Segmenter };
      const original = intlAsAny.Segmenter;
      delete intlAsAny.Segmenter;
      try {
        // Surrogate-pair safety still holds via Array.from.
        const result = JointUIService.truncateOperatorDisplayName("🎉".repeat(20), measure);
        expect(result).toBe("🎉".repeat(9) + "…");
      } finally {
        intlAsAny.Segmenter = original;
      }
    });

    it("uses the default canvas-based measurer when no measurer is injected", () => {
      // Stub getContext → null so the default measurer routes through the
      // fallback path (avoids jsdom's "Not implemented" warning spam from
      // the dozens of measurer calls the binary search makes).
      const originalGetContext = HTMLCanvasElement.prototype.getContext;
      (HTMLCanvasElement.prototype as unknown as { getContext: () => null }).getContext = () => null;
      (JointUIService as unknown as { measureCtx: CanvasRenderingContext2D | null }).measureCtx = null;
      try {
        const result = JointUIService.truncateOperatorDisplayName("a".repeat(100));
        expect(result.endsWith("…")).toBe(true);
        expect(result.length).toBeLessThan(100);
      } finally {
        HTMLCanvasElement.prototype.getContext = originalGetContext;
        (JointUIService as unknown as { measureCtx: CanvasRenderingContext2D | null }).measureCtx = null;
      }
    });
  });

  describe("measureOperatorNameWidth", () => {
    // Static cache lives on the class; reset it between tests so each one
    // starts from a clean slate and re-enters getMeasureContext.
    const resetCache = () => {
      (JointUIService as unknown as { measureCtx: CanvasRenderingContext2D | null }).measureCtx = null;
    };
    beforeEach(resetCache);
    afterEach(resetCache);

    it("falls back to a per-char approximation when no canvas 2D context is available", () => {
      // Stub the prototype to return null explicitly — this mirrors the
      // production behavior in environments that don't support canvas, and
      // avoids jsdom's "Not implemented: getContext" warning spam.
      const originalGetContext = HTMLCanvasElement.prototype.getContext;
      (HTMLCanvasElement.prototype as unknown as { getContext: () => null }).getContext = () => null;
      try {
        expect(JointUIService.measureOperatorNameWidth("")).toBe(0);
        expect(JointUIService.measureOperatorNameWidth("hello")).toBe("hello".length * 7);
      } finally {
        HTMLCanvasElement.prototype.getContext = originalGetContext;
      }
    });

    it("uses Canvas measureText when a 2D context is available, and caches it", () => {
      const measureSpy = vi.fn((s: string) => ({ width: s.length * 12 }));
      const fakeCtx = { font: "", measureText: measureSpy } as unknown as CanvasRenderingContext2D;
      const getContextSpy = vi.fn(() => fakeCtx);
      const originalGetContext = HTMLCanvasElement.prototype.getContext;
      // Stub only on the prototype; restored in finally.
      (HTMLCanvasElement.prototype as unknown as { getContext: typeof getContextSpy }).getContext = getContextSpy;
      try {
        expect(JointUIService.measureOperatorNameWidth("hello")).toBe(5 * 12);
        // Second call hits the cached-ctx branch — should not create another canvas.
        expect(JointUIService.measureOperatorNameWidth("hi")).toBe(2 * 12);
        expect(getContextSpy).toHaveBeenCalledTimes(1);
        expect(measureSpy).toHaveBeenCalledTimes(2);
      } finally {
        HTMLCanvasElement.prototype.getContext = originalGetContext;
      }
    });
  });

  describe("changeOperatorJointDisplayName", () => {
    it("writes the truncated caption to the joint model's text attr", () => {
      // Stub getContext → null so the binary-search inside
      // truncateOperatorDisplayName routes through the fallback measurer
      // instead of spamming jsdom's "Not implemented: getContext" warning.
      const originalGetContext = HTMLCanvasElement.prototype.getContext;
      (HTMLCanvasElement.prototype as unknown as { getContext: () => null }).getContext = () => null;
      (JointUIService as unknown as { measureCtx: CanvasRenderingContext2D | null }).measureCtx = null;
      try {
        const attrSpy = vi.fn();
        const getModelByIdSpy = vi.fn(() => ({ attr: attrSpy }));
        const jointPaper = { getModelById: getModelByIdSpy } as unknown as joint.dia.Paper;
        // changeOperatorJointDisplayName is an instance method but uses no
        // `this` state; pass a minimal metadata stub so the constructor's
        // subscribe doesn't throw.
        const metadataStub = { getOperatorMetadata: () => of({ operators: [], groups: [] }) };
        const service = new JointUIService(metadataStub as never);

        const operator = { operatorID: "op-1" } as OperatorPredicate;
        // Long enough to force truncation under the 200-px budget.
        const longName = "abcdefghij".repeat(20);
        service.changeOperatorJointDisplayName(operator, jointPaper, longName);

        expect(getModelByIdSpy).toHaveBeenCalledWith("op-1");
        expect(attrSpy).toHaveBeenCalledTimes(1);
        const [selector, rendered] = attrSpy.mock.calls[0];
        expect(selector).toBe(`.${operatorNameClass}/text`);
        expect(typeof rendered).toBe("string");
        expect((rendered as string).endsWith("…")).toBe(true);
        expect((rendered as string).length).toBeLessThan(longName.length);
      } finally {
        HTMLCanvasElement.prototype.getContext = originalGetContext;
        (JointUIService as unknown as { measureCtx: CanvasRenderingContext2D | null }).measureCtx = null;
      }
    });
  });
});

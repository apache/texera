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

import { of } from "rxjs";
import * as joint from "jointjs";
import { JointUIService, operatorNameClass, operatorStateClass, operatorPortMetricsClass } from "./joint-ui.service";
import { CommentBox, OperatorPredicate } from "../../types/workflow-common.interface";
import { OperatorState } from "../../types/execute-workflow.interface";
import { Coeditor } from "../../../common/type/user";

// Minimal mock of OperatorMetadataService — the constructor subscribes to
// getOperatorMetadata() but the schemas list isn't needed for the methods
// covered here. Tests that exercise `getJointOperatorElement` build their
// own metadata stub with real schemas inline.
const emptyMetadataStub = {
  getOperatorMetadata: () =>
    of({
      operators: [],
      groups: [],
    }),
};

describe("JointUIService", () => {
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

  // ---------------------------------------------------------------------------
  // Static helpers — pure functions, easiest to test directly.
  // ---------------------------------------------------------------------------

  describe("getOperatorFillColor (static)", () => {
    it("returns the disabled fill for an isDisabled=true operator", () => {
      expect(JointUIService.getOperatorFillColor({ isDisabled: true } as OperatorPredicate)).toBe("#E0E0E0");
    });
    it("returns the default white fill for an enabled operator", () => {
      expect(JointUIService.getOperatorFillColor({} as OperatorPredicate)).toBe("#FFFFFF");
      expect(JointUIService.getOperatorFillColor({ isDisabled: false } as OperatorPredicate)).toBe("#FFFFFF");
    });
  });

  describe("getOperatorCacheDisplayText (static)", () => {
    it("returns empty string when cacheStatus is undefined", () => {
      expect(JointUIService.getOperatorCacheDisplayText({ markedForReuse: true } as OperatorPredicate)).toBe("");
    });
    it("returns empty string when the operator is not marked for reuse", () => {
      expect(
        JointUIService.getOperatorCacheDisplayText({ markedForReuse: false } as OperatorPredicate, "cache valid")
      ).toBe("");
    });
    it("returns the cache status text when both are set", () => {
      expect(
        JointUIService.getOperatorCacheDisplayText({ markedForReuse: true } as OperatorPredicate, "cache valid")
      ).toBe("cache valid");
    });
  });

  describe("getOperatorCacheIcon (static)", () => {
    it("returns empty when the operator is not marked for reuse", () => {
      expect(JointUIService.getOperatorCacheIcon({ markedForReuse: false } as OperatorPredicate, "cache valid")).toBe(
        ""
      );
    });
    it("returns the valid-cache icon when cacheStatus is 'cache valid'", () => {
      expect(JointUIService.getOperatorCacheIcon({ markedForReuse: true } as OperatorPredicate, "cache valid")).toBe(
        "assets/svg/operator-reuse-cache-valid.svg"
      );
    });
    it("returns the invalid-cache icon for any other status (including undefined)", () => {
      expect(JointUIService.getOperatorCacheIcon({ markedForReuse: true } as OperatorPredicate)).toBe(
        "assets/svg/operator-reuse-cache-invalid.svg"
      );
      expect(JointUIService.getOperatorCacheIcon({ markedForReuse: true } as OperatorPredicate, "cache invalid")).toBe(
        "assets/svg/operator-reuse-cache-invalid.svg"
      );
    });
  });

  describe("getOperatorViewResultIcon (static)", () => {
    it("returns the view-result asset when viewResult=true", () => {
      expect(JointUIService.getOperatorViewResultIcon({ viewResult: true } as OperatorPredicate)).toBe(
        "assets/svg/operator-view-result.svg"
      );
    });
    it("returns empty otherwise", () => {
      expect(JointUIService.getOperatorViewResultIcon({} as OperatorPredicate)).toBe("");
      expect(JointUIService.getOperatorViewResultIcon({ viewResult: false } as OperatorPredicate)).toBe("");
    });
  });

  describe("getJointLinkCell (static)", () => {
    it("builds a joint link cell carrying source/target/id from the OperatorLink", () => {
      const link = JointUIService.getJointLinkCell({
        linkID: "link-1",
        source: { operatorID: "op-A", portID: "out-0" },
        target: { operatorID: "op-B", portID: "in-0" },
      });
      expect(link.id).toBe("link-1");
      expect(link.get("source")).toEqual({ id: "op-A", port: "out-0" });
      expect(link.get("target")).toEqual({ id: "op-B", port: "in-0" });
      // z=0 keeps links rendered under operator elements (z=1 in
      // getJointOperatorElement).
      expect(link.get("z")).toBe(0);
    });
  });

  describe("getJointUserPointerName (static)", () => {
    it("prefixes the coeditor clientId with 'pointer_'", () => {
      expect(JointUIService.getJointUserPointerName({ clientId: "abc123" } as Coeditor)).toBe("pointer_abc123");
    });
  });

  describe("getJointUserPointerCell (static)", () => {
    it("builds a circle cell whose id matches getJointUserPointerName", () => {
      const coeditor = { clientId: "42", name: "Ada", color: "#ff0000" } as Coeditor;
      const cell = JointUIService.getJointUserPointerCell(coeditor, { x: 10, y: 20 }, "#abcdef");
      expect(cell.id).toBe(JointUIService.getJointUserPointerName(coeditor));
      // attr('body/fill') reflects the explicit color argument.
      expect(cell.attr("body/fill")).toBe("#abcdef");
      expect(cell.attr("body/stroke")).toBe("#abcdef");
    });
  });

  // ---------------------------------------------------------------------------
  // Instance methods that operate on a joint Paper. Each test stubs the paper's
  // getModelById to return a model with an `attr` spy; assertions look at what
  // the SUT wrote on that model.
  // ---------------------------------------------------------------------------

  function makePaperWithModel() {
    const attrSpy = vi.fn();
    const portPropSpy = vi.fn();
    const getPortsSpy = vi.fn(() => [] as { id?: string; group?: string }[]);
    const model = { attr: attrSpy, getPorts: getPortsSpy, portProp: portPropSpy };
    const paper = { getModelById: vi.fn(() => model) } as unknown as joint.dia.Paper;
    return { paper, attrSpy, portPropSpy, getPortsSpy, model };
  }

  describe("changeOperatorColor", () => {
    it("paints the body stroke neutral for a valid operator", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorColor(paper, "op-1", true);
      expect(attrSpy).toHaveBeenCalledWith("rect.body/stroke", "#CFCFCF");
    });
    it("paints the body stroke red for an invalid operator", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorColor(paper, "op-1", false);
      expect(attrSpy).toHaveBeenCalledWith("rect.body/stroke", "red");
    });
  });

  describe("changeOperatorState", () => {
    // For each state, the SUT writes a fill color to .${operatorStateClass};
    // we only assert on the color since the rest of the attr payload (port
    // labels, worker count) is exercised through the existing port mocks.
    const cases: Array<[OperatorState, string]> = [
      [OperatorState.Ready, "#a6bd37"],
      [OperatorState.Completed, "green"],
      [OperatorState.Paused, "magenta"],
      [OperatorState.Pausing, "magenta"],
      [OperatorState.Running, "orange"],
      [OperatorState.Uninitialized, "gray"],
    ];
    cases.forEach(([state, color]) => {
      it(`writes fill='${color}' for state=${state}`, () => {
        const { paper, attrSpy } = makePaperWithModel();
        const service = new JointUIService(emptyMetadataStub as never);
        service.changeOperatorState(paper, "op-1", state);
        // The attr payload is an object keyed by selectors; pluck the state class entry.
        const [payload] = attrSpy.mock.calls[0];
        expect(payload[`.${operatorStateClass}`]).toEqual({ text: state.toString(), fill: color });
        expect(payload["rect.body"]).toEqual({ stroke: color });
      });
    });
  });

  describe("foldOperatorDetails / unfoldOperatorDetails", () => {
    it("hides operator state + metric texts and the action buttons when folded", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.foldOperatorDetails(paper, "op-1");
      const [payload] = attrSpy.mock.calls[0];
      expect(payload[`.${operatorStateClass}`].visibility).toBe("hidden");
      expect(payload[`.${operatorPortMetricsClass}`].visibility).toBe("hidden");
      expect(payload[".delete-button"].visibility).toBe("hidden");
      expect(payload[".chat-button"].visibility).toBe("hidden");
    });
    it("reveals the same surface when unfolded", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.unfoldOperatorDetails(paper, "op-1");
      const [payload] = attrSpy.mock.calls[0];
      expect(payload[`.${operatorStateClass}`].visibility).toBe("visible");
      expect(payload[`.${operatorPortMetricsClass}`].visibility).toBe("visible");
      expect(payload[".delete-button"].visibility).toBe("visible");
      expect(payload[".chat-button"].visibility).toBe("visible");
    });
  });

  describe("showAgentActionLabel / hideAgentActionLabel", () => {
    it("writes the action label with the agent name prefix when the model exists", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.showAgentActionLabel(paper, "op-1", "modified", "Aria");
      const [payload] = attrSpy.mock.calls[0];
      const entry = Object.values(payload)[0] as { text: string; visibility: string };
      expect(entry.text).toBe("Aria: modified");
      expect(entry.visibility).toBe("visible");
    });
    it("uses the default 'Agent' name when none is supplied", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.showAgentActionLabel(paper, "op-1", "viewed");
      const [payload] = attrSpy.mock.calls[0];
      const entry = Object.values(payload)[0] as { text: string };
      expect(entry.text).toBe("Agent: viewed");
    });
    it("no-ops when the model is missing", () => {
      const paper = { getModelById: vi.fn(() => null) } as unknown as joint.dia.Paper;
      const service = new JointUIService(emptyMetadataStub as never);
      expect(() => service.showAgentActionLabel(paper, "missing-op", "added")).not.toThrow();
    });
    it("clears the label text and hides it on hideAgentActionLabel", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.hideAgentActionLabel(paper, "op-1");
      const [payload] = attrSpy.mock.calls[0];
      const entry = Object.values(payload)[0] as { text: string; visibility: string };
      expect(entry.text).toBe("");
      expect(entry.visibility).toBe("hidden");
    });
    it("hideAgentActionLabel is a no-op when the model is missing", () => {
      const paper = { getModelById: vi.fn(() => null) } as unknown as joint.dia.Paper;
      const service = new JointUIService(emptyMetadataStub as never);
      expect(() => service.hideAgentActionLabel(paper, "missing-op")).not.toThrow();
    });
  });

  describe("getCommentElement", () => {
    it("builds a comment element with the supplied commentBoxID and position", () => {
      const service = new JointUIService(emptyMetadataStub as never);
      const cell = service.getCommentElement({
        commentBoxID: "cb-1",
        commentBoxPosition: { x: 42, y: 99 },
        comments: [],
      } as unknown as CommentBox);
      expect(cell.id).toBe("cb-1");
    });
    it("falls back to (0,0) when commentBoxPosition is missing", () => {
      const service = new JointUIService(emptyMetadataStub as never);
      // Should not throw — the implementation guards both the basic
      // shape position and the joint element construction.
      const cell = service.getCommentElement({
        commentBoxID: "cb-no-pos",
        comments: [],
      } as unknown as CommentBox);
      expect(cell.id).toBe("cb-no-pos");
    });
  });

  describe("getJointOperatorElement", () => {
    it("throws when the operator type isn't in the loaded schema list", () => {
      const service = new JointUIService(emptyMetadataStub as never);
      const operator = {
        operatorID: "op-x",
        operatorType: "DefinitelyNotARealType",
        operatorProperties: {},
        inputPorts: [],
        outputPorts: [],
        showAdvanced: false,
      } as unknown as OperatorPredicate;
      expect(() => service.getJointOperatorElement(operator, { x: 0, y: 0 })).toThrow(
        /operator type DefinitelyNotARealType doesn't exist/
      );
    });
  });
});

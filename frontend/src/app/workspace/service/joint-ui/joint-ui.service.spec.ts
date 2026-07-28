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

  describe("getDefaultLinkCell (static)", () => {
    it("builds a link routed with manhattan and connected with rounded corners", () => {
      const link = JointUIService.getDefaultLinkCell();
      expect(link).toBeInstanceOf(joint.dia.Link);
      expect(link.get("router")).toEqual({ name: "manhattan" });
      expect(link.get("connector")).toEqual({ name: "rounded" });
    });

    it("styles the connection stroke and hides the remove tool by default", () => {
      const link = JointUIService.getDefaultLinkCell();
      expect(link.attr(".connection/stroke")).toBe("#919191");
      expect(link.attr(".connection/stroke-width")).toBe("2px");
      // the delete affordance is present in the markup but hidden until hover.
      expect(link.attr(".tool-remove/display")).toBe("none");
      expect(link.attr(".tool-remove/fill")).toBe("#D8656A");
    });

    it("fills the source and target markers with the handle color", () => {
      const link = JointUIService.getDefaultLinkCell();
      expect(link.attr(".marker-source/fill")).toBe("#919191");
      expect(link.attr(".marker-target/fill")).toBe("#919191");
      expect(link.attr(".marker-source/stroke")).toBe("none");
      expect(link.attr(".marker-target/stroke")).toBe("none");
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
    function buildMetadataWithSchemas(schemas: object[]) {
      return {
        getOperatorMetadata: () =>
          of({
            operators: schemas,
            groups: [],
          }),
      };
    }

    const minimalSchema = (operatorType: string, friendlyName = "Friendly") => ({
      operatorType,
      operatorVersion: "v1",
      jsonSchema: {},
      additionalMetadata: { userFriendlyName: friendlyName },
    });

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

    it("returns an element carrying the predicate's operatorID and z-index 1", () => {
      const schema = minimalSchema("TestOp", "Test Operator");
      const service = new JointUIService(buildMetadataWithSchemas([schema]) as never);
      const predicate = {
        operatorID: "my-op",
        operatorType: "TestOp",
        operatorVersion: "v1",
        operatorProperties: {},
        inputPorts: [{ portID: "in-0" }],
        outputPorts: [{ portID: "out-0" }],
        showAdvanced: false,
        isDisabled: false,
      } as OperatorPredicate;
      const element = service.getJointOperatorElement(predicate, { x: 100, y: 50 });
      expect(element.id).toBe("my-op");
      expect(element.get("z")).toBe(1);
      // Both ports flow through addPort.
      const ports = element.getPorts();
      expect(ports.map(p => p.id).sort()).toEqual(["in-0", "out-0"]);
    });

    it("emits add/remove port buttons in the markup when dynamicInputPorts and dynamicOutputPorts are true", () => {
      const schema = minimalSchema("DynamicOp");
      const service = new JointUIService(buildMetadataWithSchemas([schema]) as never);
      const predicate = {
        operatorID: "dyn",
        operatorType: "DynamicOp",
        operatorVersion: "v1",
        operatorProperties: {},
        inputPorts: [],
        outputPorts: [],
        showAdvanced: false,
        isDisabled: false,
        dynamicInputPorts: true,
        dynamicOutputPorts: true,
      } as unknown as OperatorPredicate;
      const element = service.getJointOperatorElement(predicate, { x: 0, y: 0 });
      // The markup is stamped onto the element's attributes; both port-button
      // classes only appear when their dynamic-ports flag is true.
      const markup = (element as unknown as { attributes: { markup: string } }).attributes.markup;
      expect(markup).toContain("add-input-port-button");
      expect(markup).toContain("add-output-port-button");
      expect(markup).toContain("remove-input-port-button");
      expect(markup).toContain("remove-output-port-button");
    });

    it("renders customDisplayName in the operator name attr when supplied", () => {
      const schema = minimalSchema("Named", "Friendly");
      const service = new JointUIService(buildMetadataWithSchemas([schema]) as never);
      const predicate = {
        operatorID: "named",
        operatorType: "Named",
        operatorVersion: "v1",
        operatorProperties: {},
        inputPorts: [],
        outputPorts: [],
        showAdvanced: false,
        isDisabled: false,
        customDisplayName: "Custom-Display",
      } as unknown as OperatorPredicate;
      const element = service.getJointOperatorElement(predicate, { x: 0, y: 0 });
      // The display name lands in `.texera-operator-name/text` via
      // getCustomOperatorStyleAttrs. truncateOperatorDisplayName is the no-op
      // identity for short names so the value comes through verbatim.
      expect(element.attr(`.${operatorNameClass}/text`)).toBe("Custom-Display");
    });
  });

  // ---------------------------------------------------------------------------
  // Port-iteration paths — the original tests stubbed getPorts() to return [],
  // which skipped the forEach branches inside changeOperatorState and the
  // entire body of changeOperatorStatistics. The tests below cover those.
  // ---------------------------------------------------------------------------

  describe("changeOperatorState — port label re-coloring", () => {
    it("re-paints every input and output port label to the state's fill color", () => {
      const attrSpy = vi.fn();
      const portPropSpy = vi.fn();
      // changeOperatorState iterates getPorts() splitting by group; both
      // groups must be present so the in/out filters each yield matches.
      const getPortsSpy = vi.fn(() => [
        { id: "in-0", group: "in" },
        { id: "out-0", group: "out" },
      ]);
      const model = { attr: attrSpy, getPorts: getPortsSpy, portProp: portPropSpy };
      const paper = { getModelById: vi.fn(() => model) } as unknown as joint.dia.Paper;
      const service = new JointUIService(emptyMetadataStub as never);

      service.changeOperatorState(paper, "op-1", OperatorState.Running);

      // Running → orange. Both ports get the same color through portProp.
      expect(portPropSpy).toHaveBeenCalledWith("in-0", "attrs/.port-label/fill", "orange");
      expect(portPropSpy).toHaveBeenCalledWith("out-0", "attrs/.port-label/fill", "orange");
    });
  });

  describe("changeOperatorDisableStatus", () => {
    it("paints the body fill with the disabled grey color for a disabled operator", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorDisableStatus(paper, { operatorID: "op-1", isDisabled: true } as OperatorPredicate);
      expect(attrSpy).toHaveBeenCalledWith("rect.body/fill", "#E0E0E0");
    });
    it("paints the body fill with the default white color for an enabled operator", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorDisableStatus(paper, { operatorID: "op-1" } as OperatorPredicate);
      expect(attrSpy).toHaveBeenCalledWith("rect.body/fill", "#FFFFFF");
    });
  });

  describe("changeOperatorViewResultStatus", () => {
    it("writes the view-result asset path when viewResult is true", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorViewResultStatus(paper, {
        operatorID: "op-1",
        viewResult: true,
      } as unknown as OperatorPredicate);
      expect(attrSpy).toHaveBeenCalledTimes(1);
      const [selector, value] = attrSpy.mock.calls[0];
      expect(String(selector)).toContain("view-result");
      expect(value).toBe("assets/svg/operator-view-result.svg");
    });
    it("writes the empty asset path when viewResult is missing/false", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorViewResultStatus(paper, { operatorID: "op-1" } as OperatorPredicate);
      expect(attrSpy.mock.calls[0][1]).toBe("");
    });
  });

  describe("changeOperatorReuseCacheStatus", () => {
    it("writes both the reuse-cache icon and the view-result icon when the cache is valid", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorReuseCacheStatus(
        paper,
        { operatorID: "op-1", markedForReuse: true } as unknown as OperatorPredicate,
        "cache valid"
      );
      // Two attr() calls — one for the cache icon, one for the view-result
      // icon — both targeted at the operator's image attrs.
      expect(attrSpy).toHaveBeenCalledTimes(2);
      const allValues = attrSpy.mock.calls.map(c => c[1]).join(" | ");
      expect(allValues).toContain("assets/svg/operator-reuse-cache-valid.svg");
    });
    it("writes the empty path for both icons when the operator isn't marked for reuse", () => {
      const { paper, attrSpy } = makePaperWithModel();
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorReuseCacheStatus(paper, {
        operatorID: "op-1",
        markedForReuse: false,
      } as unknown as OperatorPredicate);
      expect(attrSpy).toHaveBeenCalledTimes(2);
      expect(attrSpy.mock.calls.every(c => c[1] === "")).toBe(true);
    });
  });

  describe("changeOperatorStatistics", () => {
    function makeStatsPaper(getPortsImpl: () => Array<{ id?: string; group?: string; attrs?: unknown }>) {
      const attrSpy = vi.fn();
      const portPropSpy = vi.fn();
      const getPortsSpy = vi.fn(getPortsImpl);
      const model = { attr: attrSpy, getPorts: getPortsSpy, portProp: portPropSpy };
      const paper = { getModelById: vi.fn(() => model) } as unknown as joint.dia.Paper;
      return { paper, attrSpy, portPropSpy };
    }

    it("falls back to the Uninitialized state when statistics is undefined", () => {
      const { paper, attrSpy } = makeStatsPaper(() => []);
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorStatistics(paper, "op-1", undefined, false, false);
      // changeOperatorState writes the state-class fill payload.
      const [payload] = attrSpy.mock.calls[0];
      expect((payload as Record<string, { text: string }>)[`.${operatorStateClass}`].text).toBe(
        OperatorState.Uninitialized.toString()
      );
    });

    it("writes per-port counts derived from inputPortMetrics and outputPortMetrics", () => {
      const { paper, portPropSpy } = makeStatsPaper(() => [
        // Port IDs use the "<group>-<index>" convention; the SUT splits on "-"
        // and uses the suffix to look up the metrics map.
        { id: "in-0", group: "in", attrs: { ".port-label": { text: "data: 0" } } },
        { id: "out-1", group: "out", attrs: { ".port-label": { text: "result: 0" } } },
      ]);
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorStatistics(
        paper,
        "op-1",
        {
          operatorState: OperatorState.Running,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
          inputPortMetrics: { "0": 42 },
          outputPortMetrics: { "1": 7 },
          numWorkers: 3,
        },
        false,
        false
      );
      expect(portPropSpy).toHaveBeenCalledWith("in-0", "attrs/.port-label/text", (42).toLocaleString());
      expect(portPropSpy).toHaveBeenCalledWith("out-1", "attrs/.port-label/text", (7).toLocaleString());
    });

    it("writes the worker count label when statistics include numWorkers", () => {
      const { paper, attrSpy } = makeStatsPaper(() => []);
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorStatistics(
        paper,
        "op-1",
        {
          operatorState: OperatorState.Ready,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
          inputPortMetrics: {},
          outputPortMetrics: {},
          numWorkers: 8,
        },
        false,
        false
      );
      // attr() is called once with the workers selector and the formatted string.
      const valuesWritten = attrSpy.mock.calls.map(c => c[1]);
      expect(valuesWritten).toContain("#workers: 8");
    });

    it("defaults the worker count to 1 when numWorkers is unspecified", () => {
      const { paper, attrSpy } = makeStatsPaper(() => []);
      const service = new JointUIService(emptyMetadataStub as never);
      service.changeOperatorStatistics(
        paper,
        "op-1",
        {
          operatorState: OperatorState.Ready,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
          inputPortMetrics: {},
          outputPortMetrics: {},
        },
        false,
        false
      );
      const valuesWritten = attrSpy.mock.calls.map(c => c[1]);
      expect(valuesWritten).toContain("#workers: 1");
    });
  });
});

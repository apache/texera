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

import {
  buildStructuralVisualTrace,
  extractVisualTraceSelectionFromElement,
  findVisualTraceElement,
  parseVisualTraceMessage,
  parseVisualTracePayloadAttribute,
  parseVisualTraceSelectionMessage,
} from "./visual-trace.utils";

describe("parseVisualTraceMessage", () => {
  it("accepts a valid visual trace message", () => {
    expect(
      parseVisualTraceMessage({
        type: "texera-visual-trace",
        payload: {
          title: "Charizard wins",
          heroImage: "data:image/png;base64,abc",
          steps: [
            {
              title: "Loaded sprite",
              kind: "source",
              metrics: [{ label: "Rows", value: "440" }],
            },
          ],
        },
      })
    ).toEqual({
      title: "Charizard wins",
      heroImage: "data:image/png;base64,abc",
      steps: [
        {
          title: "Loaded sprite",
          kind: "source",
          metrics: [{ label: "Rows", value: "440" }],
        },
      ],
    });
  });

  it("rejects malformed or incomplete trace messages", () => {
    expect(parseVisualTraceMessage(undefined)).toBeUndefined();
    expect(parseVisualTraceMessage({ type: "other", payload: {} })).toBeUndefined();
    expect(parseVisualTraceMessage({ type: "texera-visual-trace", payload: { title: "Missing steps" } })).toBeUndefined();
    expect(
      parseVisualTraceMessage({
        type: "texera-visual-trace",
        payload: {
          title: "Bad step",
          steps: [{ detail: "No title" }],
        },
      })
    ).toBeUndefined();
  });
});

describe("parseVisualTraceSelectionMessage", () => {
  it("accepts a valid fallback selection message", () => {
    expect(
      parseVisualTraceSelectionMessage({
        type: "texera-visual-trace-selection",
        payload: {
          title: "Charizard",
          image: "data:image/png;base64,abc",
          imageAlt: "Charizard sprite",
        },
      })
    ).toEqual({
      title: "Charizard",
      image: "data:image/png;base64,abc",
      imageAlt: "Charizard sprite",
    });
  });

  it("rejects malformed selection messages", () => {
    expect(parseVisualTraceSelectionMessage(undefined)).toBeUndefined();
    expect(parseVisualTraceSelectionMessage({ type: "other", payload: {} })).toBeUndefined();
    expect(parseVisualTraceSelectionMessage({ type: "texera-visual-trace-selection", payload: {} })).toBeUndefined();
  });
});

describe("buildStructuralVisualTrace", () => {
  it("builds an upstream workflow journey when a visualization only reports the clicked image", () => {
    const operators = {
      source: { operatorID: "source", operatorType: "Smart Source", customDisplayName: "Pokemon Images" },
      udf: { operatorID: "udf", operatorType: "Python UDF", customDisplayName: "Map sprites" },
      visualizer: { operatorID: "visualizer", operatorType: "HTML Visualizer" },
    };
    const inputs = {
      source: [],
      udf: ["source"],
      visualizer: ["udf"],
    };

    expect(
      buildStructuralVisualTrace(
        { title: "Charizard", image: "data:image/png;base64,abc", imageAlt: "Charizard sprite" },
        "visualizer",
        {
          hasOperator: (operatorId: string) => operatorId in operators,
          getOperator: (operatorId: string) => operators[operatorId as keyof typeof operators],
          getInputOperatorIds: (operatorId: string) => inputs[operatorId as keyof typeof inputs],
        }
      )
    ).toEqual({
      title: "Charizard",
      subtitle: "Workflow path to HTML Visualizer",
      summary:
        "Auto-built from the upstream workflow graph. Add a trace payload in the visualization for row-level details.",
      heroImage: "data:image/png;base64,abc",
      heroImageAlt: "Charizard sprite",
      heroMetric: { label: "Steps", value: "3" },
      steps: [
        {
          title: "Pokemon Images",
          operatorId: "source",
          operatorLabel: "Pokemon Images",
          kind: "source",
        },
        {
          title: "Map sprites",
          operatorId: "udf",
          operatorLabel: "Map sprites",
          kind: "compute",
        },
        {
          title: "HTML Visualizer",
          operatorId: "visualizer",
          operatorLabel: "HTML Visualizer",
          kind: "render",
          image: "data:image/png;base64,abc",
          imageAlt: "Charizard sprite",
        },
      ],
    });
  });

  it("returns undefined when the visualizer operator is missing", () => {
    expect(
      buildStructuralVisualTrace(
        { title: "Charizard", image: "data:image/png;base64,abc" },
        "missing",
        {
          hasOperator: () => false,
          getOperator: () => {
            throw new Error("should not be called");
          },
          getInputOperatorIds: () => [],
        }
      )
    ).toBeUndefined();
  });
});

describe("visual trace DOM helpers", () => {
  it("reads a rich trace payload from an element attribute", () => {
    expect(
      parseVisualTracePayloadAttribute(
        JSON.stringify({
          title: "Charizard wins",
          steps: [{ title: "Rendered card" }],
        })
      )
    ).toEqual({
      title: "Charizard wins",
      steps: [{ title: "Rendered card" }],
    });
  });

  it("finds an image-bearing ancestor and extracts a fallback selection", () => {
    const card = document.createElement("div");
    card.className = "pokemon-side";
    card.innerHTML = `
      <div class="winner-badge">WINNER</div>
      <img src="data:image/png;base64,abc" alt="Charizard" />
      <div class="pokemon-name">Charizard</div>
    `;
    const badge = card.querySelector(".winner-badge");
    expect(badge).not.toBeNull();
    const traceElement = findVisualTraceElement(badge);

    expect(traceElement).toBe(card);
    expect(extractVisualTraceSelectionFromElement(traceElement as Element)).toEqual({
      title: "Charizard",
      image: "data:image/png;base64,abc",
      imageAlt: "Charizard",
    });
  });

  it("accepts element-like click targets from iframe documents", () => {
    const frame = document.createElement("iframe");
    document.body.appendChild(frame);
    const frameDocument = frame.contentDocument as Document;
    const card = frameDocument.createElement("div");
    card.innerHTML = `
      <div class="winner-badge">WINNER</div>
      <img src="data:image/png;base64,abc" alt="Charizard" />
    `;
    frameDocument.body.appendChild(card);

    const badge = card.querySelector(".winner-badge");
    expect(findVisualTraceElement(badge)).toBe(card);

    frame.remove();
  });
});

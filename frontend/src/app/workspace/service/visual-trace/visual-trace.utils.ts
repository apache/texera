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
  VisualTrace,
  VisualTraceMetric,
  VisualTraceSelection,
  VisualTraceStep,
  VisualTraceStepKind,
} from "../../types/visual-trace.interface";

const TRACE_MESSAGE_TYPE = "texera-visual-trace";
const TRACE_SELECTION_MESSAGE_TYPE = "texera-visual-trace-selection";
const VALID_STEP_KINDS = new Set<VisualTraceStepKind>(["source", "match", "compute", "render"]);

export interface VisualTraceGraphOperator {
  operatorID: string;
  operatorType: string;
  customDisplayName?: string;
}

export interface VisualTraceGraphReader {
  hasOperator(operatorId: string): boolean;
  getOperator(operatorId: string): VisualTraceGraphOperator;
  getInputOperatorIds(operatorId: string): string[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isElementLike(value: EventTarget | null): value is Element {
  return (
    typeof value === "object" &&
    value !== null &&
    "nodeType" in value &&
    value.nodeType === 1 &&
    "matches" in value &&
    typeof value.matches === "function" &&
    "querySelector" in value &&
    typeof value.querySelector === "function"
  );
}

function parseMetric(value: unknown): VisualTraceMetric | undefined {
  if (!isRecord(value) || typeof value.label !== "string" || typeof value.value !== "string") {
    return undefined;
  }
  return {
    label: value.label,
    value: value.value,
  };
}

function parseStep(value: unknown): VisualTraceStep | undefined {
  if (!isRecord(value) || typeof value.title !== "string") {
    return undefined;
  }

  const kind: VisualTraceStepKind | undefined =
    typeof value.kind === "string" && VALID_STEP_KINDS.has(value.kind as VisualTraceStepKind)
      ? (value.kind as VisualTraceStepKind)
      : undefined;
  const metrics = Array.isArray(value.metrics) ? value.metrics.map(parseMetric).filter(Boolean) : undefined;

  return {
    title: value.title,
    detail: typeof value.detail === "string" ? value.detail : undefined,
    operatorId: typeof value.operatorId === "string" ? value.operatorId : undefined,
    operatorLabel: typeof value.operatorLabel === "string" ? value.operatorLabel : undefined,
    image: typeof value.image === "string" ? value.image : undefined,
    imageAlt: typeof value.imageAlt === "string" ? value.imageAlt : undefined,
    kind,
    metrics: metrics as VisualTraceMetric[] | undefined,
  };
}

export function parseVisualTraceMessage(message: unknown): VisualTrace | undefined {
  if (!isRecord(message) || message.type !== TRACE_MESSAGE_TYPE || !isRecord(message.payload)) {
    return undefined;
  }

  const payload = message.payload;
  if (typeof payload.title !== "string" || !Array.isArray(payload.steps)) {
    return undefined;
  }

  const steps = payload.steps.map(parseStep);
  if (steps.length === 0 || steps.some(step => step === undefined)) {
    return undefined;
  }

  return {
    title: payload.title,
    subtitle: typeof payload.subtitle === "string" ? payload.subtitle : undefined,
    summary: typeof payload.summary === "string" ? payload.summary : undefined,
    heroImage: typeof payload.heroImage === "string" ? payload.heroImage : undefined,
    heroImageAlt: typeof payload.heroImageAlt === "string" ? payload.heroImageAlt : undefined,
    heroMetric: parseMetric(payload.heroMetric),
    steps: steps as VisualTraceStep[],
  };
}

export function parseVisualTraceSelectionMessage(message: unknown): VisualTraceSelection | undefined {
  if (!isRecord(message) || message.type !== TRACE_SELECTION_MESSAGE_TYPE || !isRecord(message.payload)) {
    return undefined;
  }

  const payload = message.payload;
  const selection = {
    title: typeof payload.title === "string" ? payload.title : undefined,
    image: typeof payload.image === "string" ? payload.image : undefined,
    imageAlt: typeof payload.imageAlt === "string" ? payload.imageAlt : undefined,
  };

  return selection.title || selection.image ? selection : undefined;
}

export function buildStructuralVisualTrace(
  selection: VisualTraceSelection,
  targetOperatorId: string,
  graph: VisualTraceGraphReader
): VisualTrace | undefined {
  if (!graph.hasOperator(targetOperatorId)) {
    return undefined;
  }

  const visited = new Set<string>();
  const operatorIds: string[] = [];
  const visit = (operatorId: string): void => {
    if (visited.has(operatorId) || !graph.hasOperator(operatorId)) {
      return;
    }
    visited.add(operatorId);
    graph.getInputOperatorIds(operatorId).forEach(visit);
    operatorIds.push(operatorId);
  };
  visit(targetOperatorId);

  const targetOperator = graph.getOperator(targetOperatorId);
  const targetLabel = targetOperator.customDisplayName ?? targetOperator.operatorType;
  const steps = operatorIds.map(operatorId => {
    const operator = graph.getOperator(operatorId);
    const operatorLabel = operator.customDisplayName ?? operator.operatorType;
    const inputIds = graph.getInputOperatorIds(operatorId);
    const kind: VisualTraceStepKind =
      operatorId === targetOperatorId ? "render" : inputIds.length === 0 ? "source" : "compute";

    return {
      title: operatorLabel,
      operatorId,
      operatorLabel,
      kind,
      image: operatorId === targetOperatorId ? selection.image : undefined,
      imageAlt: operatorId === targetOperatorId ? selection.imageAlt : undefined,
    };
  });

  return {
    title: selection.title ?? "Selected result",
    subtitle: `Workflow path to ${targetLabel}`,
    summary: "Auto-built from the upstream workflow graph. Add a trace payload in the visualization for row-level details.",
    heroImage: selection.image,
    heroImageAlt: selection.imageAlt,
    heroMetric: {
      label: "Steps",
      value: String(steps.length),
    },
    steps,
  };
}

export function parseVisualTracePayloadAttribute(value: string | null): VisualTrace | undefined {
  if (!value) {
    return undefined;
  }
  try {
    return parseVisualTraceMessage({
      type: TRACE_MESSAGE_TYPE,
      payload: JSON.parse(value),
    });
  } catch {
    return undefined;
  }
}

export function findVisualTraceElement(target: EventTarget | null): Element | undefined {
  let element = isElementLike(target) ? target : undefined;
  while (element && element !== document.body) {
    if (element.hasAttribute("data-texera-trace") || element.matches("img") || element.querySelector("img")) {
      return element;
    }
    element = element.parentElement ?? undefined;
  }
  return undefined;
}

export function extractVisualTraceSelectionFromElement(element: Element): VisualTraceSelection | undefined {
  const image = element.matches("img") ? element : element.querySelector("img");
  if (!image || image.tagName !== "IMG") {
    return undefined;
  }
  const titleElement = element.querySelector("[data-texera-trace-title], .pokemon-name");
  const imageAlt = image.getAttribute("alt") ?? undefined;
  const title = titleElement?.textContent?.trim() || imageAlt || undefined;
  const selection = {
    title,
    image: image.getAttribute("src") ?? undefined,
    imageAlt: imageAlt || title,
  };
  return selection.title || selection.image ? selection : undefined;
}

export function buildVisualTraceBridgeScript(): string {
  return `
(() => {
  const TRACE_MESSAGE_TYPE = "texera-visual-trace";
  const TRACE_SELECTION_MESSAGE_TYPE = "texera-visual-trace-selection";
  const emitTrace = payload => window.parent.postMessage({ type: TRACE_MESSAGE_TYPE, payload }, "*");
  const emitSelection = payload => window.parent.postMessage({ type: TRACE_SELECTION_MESSAGE_TYPE, payload }, "*");
  const parseTrace = value => {
    try {
      return JSON.parse(value);
    } catch {
      return undefined;
    }
  };

  const findFallbackElement = target => {
    let element = target instanceof Element ? target : null;
    while (element && element !== document.body) {
      if (element.hasAttribute("data-texera-trace")) {
        return element;
      }
      if (element.matches("img") || element.querySelector("img")) {
        return element;
      }
      element = element.parentElement;
    }
    return null;
  };

  const buildFallbackSelection = element => {
    const image = element.matches("img") ? element : element.querySelector("img");
    if (!image) {
      return undefined;
    }
    const titleElement = element.querySelector("[data-texera-trace-title], .pokemon-name");
    const title = titleElement?.textContent?.trim() || image.getAttribute("alt") || undefined;
    return {
      title,
      image: image.getAttribute("src") || undefined,
      imageAlt: image.getAttribute("alt") || title,
    };
  };

  document.addEventListener("click", event => {
    const element = findFallbackElement(event.target);
    if (!element) {
      return;
    }
    const payload = parseTrace(element.getAttribute("data-texera-trace"));
    if (payload) {
      emitTrace(payload);
      return;
    }
    const selection = buildFallbackSelection(element);
    if (selection) {
      emitSelection(selection);
    }
  });

  window.texera = window.texera || {};
  window.texera.showTrace = emitTrace;
})();
`;
}

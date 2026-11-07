/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { v4 as uuid } from "uuid";
import type {
  CommentBox,
  OperatorLink,
  OperatorPredicate,
  Point,
} from "../../core/workspace/types/workflow-common.interface";
import { WorkflowSession } from "./workflow-session";
interface Selection {
  operators: OperatorPredicate[];
  operatorPositions: Record<string, Point>;
  links: OperatorLink[];
  commentBoxes: CommentBox[];
}
export function copySelection(session: WorkflowSession, ids: readonly string[]): string {
  const selected = new Set(ids);
  const { content } = session.getSnapshot().workflow;
  return JSON.stringify({
    operators: content.operators.filter(op => selected.has(op.operatorID)),
    operatorPositions: Object.fromEntries(Object.entries(content.operatorPositions).filter(([id]) => selected.has(id))),
    links: content.links.filter(link => selected.has(link.linkID)),
    commentBoxes: content.commentBoxes.filter(box => selected.has(box.commentBoxID)),
  } satisfies Selection);
}
/** Accepts the same JSON shape as the original editor clipboard. Validate before touching shared state. */
export function pasteSelection(session: WorkflowSession, text: string, newId: () => string = uuid): string[] {
  const value = readSelection(text);
  const positions = [
    ...Object.values(session.getSnapshot().workflow.content.operatorPositions),
    ...session.graph.getAllCommentBoxes().map(box => box.commentBoxPosition),
  ];
  const offset = (point: Point) => {
    let result = { x: point.x + 20, y: point.y + 20 };
    while (positions.some(pos => pos.x === result.x && pos.y === result.y))
      result = { x: result.x + 20, y: result.y + 20 };
    positions.push(result);
    return result;
  };
  const identities = new Map(value.operators.map(op => [op.operatorID, `${op.operatorType}-operator-${newId()}`]));
  const operators = value.operators.map(op => ({
    operator: { ...op, operatorID: identities.get(op.operatorID)! },
    position: offset(value.operatorPositions[op.operatorID]),
  }));
  const links = value.links
    .filter(link => identities.has(link.source.operatorID) && identities.has(link.target.operatorID))
    .map(link => ({
      ...link,
      linkID: `link-${newId()}`,
      source: { ...link.source, operatorID: identities.get(link.source.operatorID)! },
      target: { ...link.target, operatorID: identities.get(link.target.operatorID)! },
    }));
  const comments = value.commentBoxes.map(box => ({
    ...box,
    commentBoxID: `comment-${newId()}`,
    commentBoxPosition: offset(box.commentBoxPosition),
  }));
  session.transaction(() => {
    operators.forEach(({ operator, position }) => session.addOperator(operator, position));
    links.forEach(link => session.addLink(link));
    comments.forEach(box => session.addComment(box));
  });
  return [...operators.map(({ operator }) => operator.operatorID), ...comments.map(box => box.commentBoxID)];
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function position(value: unknown): value is Point {
  return (
    object(value) &&
    typeof value.x === "number" &&
    Number.isFinite(value.x) &&
    typeof value.y === "number" &&
    Number.isFinite(value.y)
  );
}
function readSelection(text: string): Selection {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error("The clipboard does not contain workflow elements.");
  }
  if (
    !object(value) ||
    !Array.isArray(value.operators) ||
    !Array.isArray(value.links) ||
    !object(value.operatorPositions) ||
    !Array.isArray(value.commentBoxes)
  )
    throw new Error("The clipboard does not contain workflow elements.");
  const identities = new Set<string>();
  for (const op of value.operators) {
    if (
      !object(op) ||
      typeof op.operatorID !== "string" ||
      !op.operatorID ||
      identities.has(op.operatorID) ||
      typeof op.operatorType !== "string" ||
      !object(op.operatorProperties)
    )
      throw new Error("Invalid operator in the clipboard.");
    identities.add(op.operatorID);
    if (!position(value.operatorPositions[op.operatorID]))
      throw new Error("Invalid operator position in the clipboard.");
    for (const ports of [op.inputPorts, op.outputPorts])
      if (
        !Array.isArray(ports) ||
        ports.some(port => !object(port) || typeof port.portID !== "string" || !port.portID) ||
        new Set(ports.map(port => port.portID)).size !== ports.length
      )
        throw new Error("Invalid operator ports in the clipboard.");
  }
  for (const link of value.links) {
    if (
      !object(link) ||
      typeof link.linkID !== "string" ||
      ![link.source, link.target].every(
        endpoint => object(endpoint) && typeof endpoint.operatorID === "string" && typeof endpoint.portID === "string"
      )
    )
      throw new Error("Invalid link in the clipboard.");
    for (const [endpoint, input] of [
      [link.source, false],
      [link.target, true],
    ] as const) {
      const end = endpoint as { operatorID: string; portID: string };
      const op = value.operators.find(op => op.operatorID === end.operatorID);
      if (
        op &&
        !(input ? op.inputPorts : op.outputPorts).some((port: { portID: string }) => port.portID === end.portID)
      )
        throw new Error("Invalid link port in the clipboard.");
    }
  }
  for (const box of value.commentBoxes)
    if (
      !object(box) ||
      typeof box.commentBoxID !== "string" ||
      !position(box.commentBoxPosition) ||
      !Array.isArray(box.comments) ||
      box.comments.some(
        comment =>
          !object(comment) ||
          typeof comment.content !== "string" ||
          typeof comment.creatorID !== "number" ||
          typeof comment.creatorName !== "string" ||
          typeof comment.creationTime !== "string"
      )
    )
      throw new Error("Invalid comment in the clipboard.");
  return value as unknown as Selection;
}

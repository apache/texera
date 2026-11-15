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

import * as Y from "yjs";

/**
 * The operator description: a free-form markdown note on an operator instance (OperatorPredicate.description).
 * It is untrusted text: any editor of the workflow, an imported file, or an agent can write it.
 */

/** Longest description that can be saved from the editor. Matches the agent-service tool limit. */
export const MAX_OPERATOR_DESCRIPTION_LENGTH = 4000;

/**
 * Reads a stored description value as text. Anything that is not a string or a Y.Text (a hand-edited or
 * corrupted workflow file can hold a number or an object) reads as no description rather than throwing.
 */
export function descriptionText(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Y.Text) return value.toString();
  return "";
}

/**
 * Gives an operator an empty description when it has none, before it is put into the shared model, so it
 * enters with a Y.Text description that every editor then edits. If each editor created its own Y.Text on a
 * first save, Yjs would keep one of the two map values and drop the other author's text.
 */
export function withSharedDescription<T extends { description?: unknown }>(operator: T): T {
  return operator.description === undefined ? { ...operator, description: "" } : operator;
}

/**
 * Drops an empty description from an operator read out of the shared model, so operators nobody described
 * keep the shape they had before the field existed (exports, saves and diffs stay unchanged).
 */
export function withoutEmptyDescription<T extends { description?: unknown }>(operator: T): T {
  if (operator.description === "") delete operator.description;
  return operator;
}

/** Cuts text to at most maxUnits UTF-16 units without leaving half of a surrogate pair. */
export function clampDescription(text: string, maxUnits: number = MAX_OPERATOR_DESCRIPTION_LENGTH): string {
  if (text.length <= maxUnits) return text;
  let end = maxUnits;
  const code = text.charCodeAt(end - 1);
  if (end > 0 && code >= 0xd800 && code <= 0xdbff) end -= 1;
  return text.slice(0, end);
}

/**
 * Turns `ytext` into `next` by replacing only the span between their common prefix and suffix. A
 * delete-all/insert-all would make two concurrent saves keep both full texts ("AB"); touching only the
 * changed span keeps everything both sides agree on, so concurrent edits merge like typing does.
 * Must be called inside a transaction for the change to be one undo step and one event.
 */
export function applyMinimalTextEdit(ytext: Y.Text, next: string): void {
  const current = ytext.toString();
  if (current === next) return;

  let prefix = 0;
  const maxPrefix = Math.min(current.length, next.length);
  while (prefix < maxPrefix && current.charCodeAt(prefix) === next.charCodeAt(prefix)) prefix++;
  // never split a surrogate pair at the edit boundary
  if (prefix > 0 && isHighSurrogate(current.charCodeAt(prefix - 1))) prefix--;

  let suffix = 0;
  const maxSuffix = Math.min(current.length, next.length) - prefix;
  while (
    suffix < maxSuffix &&
    current.charCodeAt(current.length - 1 - suffix) === next.charCodeAt(next.length - 1 - suffix)
  ) {
    suffix++;
  }
  if (suffix > 0 && isLowSurrogate(current.charCodeAt(current.length - suffix))) suffix--;

  const deleteCount = current.length - prefix - suffix;
  const insertText = next.slice(prefix, next.length - suffix);
  if (deleteCount > 0) ytext.delete(prefix, deleteCount);
  if (insertText.length > 0) ytext.insert(prefix, insertText);
}

function isHighSurrogate(code: number): boolean {
  return code >= 0xd800 && code <= 0xdbff;
}

function isLowSurrogate(code: number): boolean {
  return code >= 0xdc00 && code <= 0xdfff;
}

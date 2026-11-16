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

// What the cases check an answer for. Each check reads plain text and says why it passed or failed.

export interface Verdict {
  pass: boolean;
  reason: string;
}

export const pass = (reason: string): Verdict => ({ pass: true, reason });
export const fail = (reason: string): Verdict => ({ pass: false, reason });

/** Every name appears, and they first appear in the given order. */
export function mentionsInOrder(text: string, names: string[]): Verdict {
  const lower = text.toLowerCase();
  const positions = names.map(name => lower.indexOf(name.toLowerCase()));
  const missing = names.filter((_, i) => positions[i] < 0);
  if (missing.length > 0) return fail(`does not mention ${missing.join(", ")}`);
  const ordered = positions.every((p, i) => i === 0 || p > positions[i - 1]);
  return ordered ? pass(`names ${names.join(", ")} in order`) : fail(`names ${names.join(", ")} out of order`);
}

/** The number appears on its own, written with or without thousands separators. */
export function mentionsNumber(text: string, value: number): Verdict {
  const forms = [String(value), value.toLocaleString("en-US")];
  // Digits and commas only, so the forms need no escaping. A neighbouring digit, or a decimal or
  // thousands separator followed by one, means the match is part of a different number.
  const found = forms.some(form => new RegExp(`(^|[^\\d.,])${form}(?!\\d|[.,]\\d)`).test(text));
  return found ? pass(`says ${value}`) : fail(`does not say ${value}`);
}

/** Every phrase appears, ignoring case. */
export function mentionsAll(text: string, phrases: string[]): Verdict {
  const lower = text.toLowerCase();
  const missing = phrases.filter(phrase => !lower.includes(phrase.toLowerCase()));
  return missing.length === 0 ? pass(`mentions ${phrases.join(", ")}`) : fail(`does not mention ${missing.join(", ")}`);
}

/** Every check passes; the first that fails gives the reason. */
export function allOf(...verdicts: Verdict[]): Verdict {
  const failed = verdicts.find(v => !v.pass);
  return failed ?? pass(verdicts.map(v => v.reason).join("; "));
}

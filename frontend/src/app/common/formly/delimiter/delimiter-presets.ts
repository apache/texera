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

/**
 * What a delimiter property holds: one literal character (the CSV scans, whose readers take a
 * single Char) or a regular expression (Unnest String, which splits with `delimiter.r`).
 */
export type DelimiterMode = "char" | "regex";

export interface DelimiterPreset {
  label: string;
  value: string;
  /** Other stored values that mean the same delimiter, so an older workflow still shows it by name. */
  aliases?: readonly string[];
}

/** The select option for "type your own". Only ever the select's value, never the stored one. */
export const CUSTOM_DELIMITER = "__custom__";

const CHAR_PRESETS: readonly DelimiterPreset[] = [
  { label: "Comma ( , )", value: "," },
  { label: "Tab ( \\t )", value: "\t" },
  { label: "Semicolon ( ; )", value: ";" },
  { label: "Pipe ( | )", value: "|" },
  { label: "Space", value: " " },
];

// Regex values are written as patterns: a bare `|` is an empty alternation that splits between
// every character, so the pipe preset escapes it.
const REGEX_PRESETS: readonly DelimiterPreset[] = [
  { label: "Comma ( , )", value: "," },
  { label: "Tab ( \\t )", value: "\\t", aliases: ["\t"] },
  { label: "Semicolon ( ; )", value: ";" },
  { label: "Pipe ( \\| )", value: "\\|" },
  { label: "Whitespace ( \\s+ )", value: "\\s+" },
  { label: "New line ( \\n )", value: "\\n", aliases: ["\n"] },
];

export function delimiterPresets(mode: DelimiterMode): readonly DelimiterPreset[] {
  return mode === "regex" ? REGEX_PRESETS : CHAR_PRESETS;
}

/** The preset a stored value is, or undefined when it is a custom value (or empty). */
export function matchDelimiterPreset(value: unknown, mode: DelimiterMode): DelimiterPreset | undefined {
  if (typeof value !== "string" || value.length === 0) {
    return undefined;
  }
  return delimiterPresets(mode).find(preset => preset.value === value || preset.aliases?.includes(value));
}

// Escapes someone may type into a single-character box, meaning the character they name.
const CHAR_ESCAPES: Readonly<Record<string, string>> = { "\\t": "\t", "\\\\": "\\" };

/** The character a typed escape such as `\t` names, or the typed text unchanged. */
export function unescapeCharDelimiter(typed: string): string {
  return CHAR_ESCAPES[typed] ?? typed;
}

// Java syntax the browser can't judge. JavaScript throws on possessive quantifiers, atomic groups and
// inline flags, and (without the `u` flag) misreads Java's escapes: `\p{L}` becomes the literal text
// "p{L}", `\x{2C}` becomes "x{2C}", `\e`/`\a`/`\h` become letters, `\Q…\E` and the `\A`/`\z` anchors
// too. An escape only counts when an odd number of backslashes precedes it, so `\\p` (a literal
// backslash, then p) is still checked.
const JAVA_ONLY_SYNTAX = /[*+?}]\+|\(\?>|\(\?[a-zA-Z-]+[):]|(?<!\\)(?:\\\\)*\\(?:[pPxN]\{|[QhHvVzAZGRXea0])/;

// Line terminators, which `.` leaves out. Mirrors UnnestStringOpDesc.LineBreaks.
const LINE_BREAKS = [0x0a, 0x0d, 0x85, 0x2028, 0x2029];

// Line breaks are tested separately; lone surrogate halves are not characters on their own.
function isLineBreakOrSurrogate(code: number): boolean {
  return LINE_BREAKS.includes(code) || (code >= 0xd800 && code <= 0xdfff);
}

/**
 * Whether the pattern matches every character of the Basic Multilingual Plane other than line breaks,
 * each tested on its own. This is exact rather than a sample, and cheap: a pattern that leaves any
 * character behind stops at the first one it misses, and only a real match-everything pattern tests
 * all ~63,000. Mirrored by UnnestStringOpDesc.matchesEveryCharacter.
 */
function matchesEveryCharacter(pattern: RegExp): boolean {
  for (let code = 0; code <= 0xffff; code++) {
    if (!isLineBreakOrSurrogate(code) && !pattern.test(String.fromCharCode(code))) {
      return false;
    }
  }
  return true;
}

/**
 * Why a delimiter cannot be used, or undefined when it can. Emptiness is left to `required` and to
 * the operator's own default; a char delimiter's length is left to the schema's `maxLength`.
 *
 * The pattern is checked with the browser's regex engine, while the operator runs it with Java's. A
 * pattern using Java-specific syntax (see JAVA_ONLY_SYNTAX) is not checked here at all, since the
 * browser would judge a different pattern. The operator makes the same checks with Java semantics when
 * the workflow compiles (UnnestStringOpDesc.validateDelimiter), so nothing is left unchecked.
 */
export function delimiterError(value: unknown, mode: DelimiterMode): string | undefined {
  if (mode !== "regex" || typeof value !== "string" || value.length === 0) {
    return undefined;
  }
  if (JAVA_ONLY_SYNTAX.test(value)) {
    return undefined;
  }
  let pattern: RegExp;
  try {
    pattern = new RegExp(value);
  } catch (e) {
    return e instanceof Error ? e.message : "Invalid regular expression";
  }
  if (pattern.test("")) {
    return "This pattern matches an empty string, so it would split between every character";
  }
  if (matchesEveryCharacter(pattern)) {
    const literal = value.length === 1 ? ` To split on a literal "${value}", use \\${value}` : "";
    // A value's line breaks survive a pattern that leaves them out, so say what is left.
    if (LINE_BREAKS.every(code => pattern.test(String.fromCharCode(code)))) {
      return `This pattern matches every character, so nothing would be left.${literal}`;
    }
    return `This pattern matches every character except line breaks, so only line breaks would be left.${literal}`;
  }
  return undefined;
}

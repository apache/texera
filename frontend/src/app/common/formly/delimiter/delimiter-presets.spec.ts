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

import { delimiterError, delimiterPresets, matchDelimiterPreset, unescapeCharDelimiter } from "./delimiter-presets";

describe("char delimiter presets", () => {
  // name shown -> the exact character a CSV scan's reader receives
  const expected: [string, string][] = [
    ["Comma", ","],
    ["Tab", "\t"],
    ["Semicolon", ";"],
    ["Pipe", "|"],
    ["Space", " "],
  ];

  it("offers exactly the common CSV delimiters, in order", () => {
    expect(delimiterPresets("char").map(p => p.value)).toEqual(expected.map(([, value]) => value));
  });

  it.each(expected)("stores %s as the single character itself", (name, value) => {
    const preset = matchDelimiterPreset(value, "char")!;
    expect(preset.label).toContain(name);
    expect(preset.value).toHaveLength(1);
  });

  it("shows the tab escape in its label, since the character itself is invisible", () => {
    expect(matchDelimiterPreset("\t", "char")!.label).toContain("\\t");
  });

  it.each([["#"], ["~"], ["§"], ["a"], [",;"], ["\\t"]])("treats %j as a custom value", value => {
    expect(matchDelimiterPreset(value, "char")).toBeUndefined();
  });

  it.each([
    ["\\t", "\t"],
    ["\\\\", "\\"],
  ])("reads the typed escape %j as the character it names", (typed, char) => {
    expect(unescapeCharDelimiter(typed)).toBe(char);
  });

  it("leaves any other typed text as it is", () => {
    expect(unescapeCharDelimiter("\\")).toBe("\\");
    expect(unescapeCharDelimiter("#")).toBe("#");
    expect(unescapeCharDelimiter("\\x")).toBe("\\x");
  });

  it("never reads a char delimiter as a pattern", () => {
    for (const value of ["(", "[", "*", "|", "\\", "."]) {
      expect(delimiterError(value, "char")).toBeUndefined();
    }
  });
});

describe("regex delimiter presets", () => {
  // name shown -> stored pattern -> a value it must split into a, b, c
  const expected: [string, string, string][] = [
    ["Comma", ",", "a,b,c"],
    ["Tab", "\\t", "a\tb\tc"],
    ["Semicolon", ";", "a;b;c"],
    ["Pipe", "\\|", "a|b|c"],
    ["Whitespace", "\\s+", "a  b\t c"],
    ["New line", "\\n", "a\nb\nc"],
  ];

  it("offers exactly the common delimiters, in order", () => {
    expect(delimiterPresets("regex").map(p => p.value)).toEqual(expected.map(([, value]) => value));
  });

  it.each(expected)("stores %s as a pattern that splits on it", (name, pattern, sample) => {
    const preset = matchDelimiterPreset(pattern, "regex")!;
    expect(preset.label).toContain(name);
    expect(delimiterError(pattern, "regex")).toBeUndefined();
    expect(sample.split(new RegExp(pattern))).toEqual(["a", "b", "c"]);
  });

  it.each([
    ["a literal tab", "\t", "\\t"],
    ["a literal new line", "\n", "\\n"],
  ])("names %s an older workflow stored as its preset", (_, stored, preset) => {
    expect(matchDelimiterPreset(stored, "regex")!.value).toBe(preset);
  });

  it("does not show a bare pipe as the Pipe preset, since the two split differently", () => {
    expect(matchDelimiterPreset("|", "regex")).toBeUndefined();
  });

  it("treats a space as a custom pattern rather than as Whitespace", () => {
    expect(matchDelimiterPreset(" ", "regex")).toBeUndefined();
  });
});

describe("custom regex validation", () => {
  it.each([
    ["a character class", "[,;]", "a,b;c"],
    ["a comma with optional spaces", "\\s*,\\s*", "a , b,c"],
    ["a multi-character delimiter", "::", "a::b::c"],
    ["an escaped dot", "\\.", "a.b.c"],
    ["an alternation", "(?:,|;)", "a,b;c"],
    ["a run of digits", "\\d+", "a1b22c"],
    ["a non-ASCII character", "•", "a•b•c"],
    ["a space", " ", "a b c"],
  ])("accepts %s", (_, pattern, sample) => {
    expect(delimiterError(pattern, "regex")).toBeUndefined();
    expect(sample.split(new RegExp(pattern))).toEqual(["a", "b", "c"]);
  });

  it.each([
    ["an unclosed group", "("],
    ["an unmatched closing parenthesis", ")"],
    ["an unclosed character class", "["],
    ["a quantifier with nothing to repeat", "*"],
    ["a leading plus", "+"],
    ["a reversed repetition range", "a{2,1}"],
    ["a trailing backslash", "\\"],
    ["an unfinished named group", "(?<"],
    ["a reversed character range", "[z-a]"],
  ])("rejects %s and says it is not a valid regular expression", (_, pattern) => {
    expect(delimiterError(pattern, "regex")).toMatch(/Invalid regular expression/);
  });

  it.each([
    ["a bare pipe", "|"],
    ["an optional run of whitespace", "\\s*"],
    ["a starred character", "a*"],
    ["an empty group", "(?:)"],
    ["a start anchor", "^"],
    ["an end anchor", "$"],
    ["a match-anything pattern", ".*"],
  ])("rejects %s, which matches an empty string and would split every character apart", (_, pattern) => {
    expect(delimiterError(pattern, "regex")).toMatch(/matches an empty string/);
  });

  it("accepts a lookahead, which only matches before the delimiter and so keeps it on the next piece", () => {
    expect(delimiterError("(?=,)", "regex")).toBeUndefined();
    expect("a,b".split(/(?=,)/)).toEqual(["a", ",b"]);
  });

  it("leaves an empty or unset value to the required check", () => {
    expect(delimiterError("", "regex")).toBeUndefined();
    expect(delimiterError(null, "regex")).toBeUndefined();
    expect(delimiterError(undefined, "regex")).toBeUndefined();
  });

  it("ignores a value that is not text", () => {
    expect(delimiterError(44, "regex")).toBeUndefined();
    expect(matchDelimiterPreset(44, "regex")).toBeUndefined();
  });

  // The browser's engine is not the operator's: Java accepts these, JavaScript throws on them. They are
  // left to the backend, which compiles the pattern with Java (UnnestStringOpDescSpec) when the workflow
  // compiles, rather than shown as a false error.
  it.each([
    ["a possessive quantifier", ",++"],
    ["a possessive star", "\\s*+,"],
    ["an atomic group", "(?>,|;)"],
    ["an inline case-insensitive flag", "(?i)and"],
    ["an inline comments flag", "(?x) , "],
  ])("leaves %s, which only Java accepts, to the operator", (_, pattern) => {
    expect(delimiterError(pattern, "regex")).toBeUndefined();
  });

  // JavaScript (without the `u` flag) parses these, but reads them as different patterns: `\\p{L}` as the
  // literal text "p{L}", `\\h` as "h". Checking them in the browser gives wrong answers either way, so
  // they are left to the operator, which checks them with Java's engine when the workflow compiles.
  it.each([
    ["a negated Unicode class, which JavaScript misreads as matching every character", "[^\\p{L}]"],
    ["a Unicode letter class", "\\p{L}+"],
    ["a POSIX class", "[^\\p{Alnum}]+"],
    ["a Java character class", "\\p{javaWhitespace}+"],
    ["a quoted literal", "\\Q|\\E"],
    ["horizontal whitespace", "\\h*,\\h*"],
    ["an end-of-input anchor", "\\z"],
    ["a start-of-input anchor", "\\A,"],
    ["a code-point escape", "\\x{1F600}"],
    ["a negated code-point escape, which JavaScript misreads as matching every character", "[^\\x{2C}]"],
    ["an escape-character escape, which JavaScript misreads as the letter e", "[^\\e]"],
    ["a bell-character escape", "\\a"],
    ["a named character", "\\N{COMMA}"],
    ["vertical whitespace", "\\v+"],
    ["an octal escape", "\\011"],
  ])("leaves %s to the operator", (_, pattern) => {
    expect(delimiterError(pattern, "regex")).toBeUndefined();
  });

  it("still checks an escaped backslash followed by p, which is not a Java class", () => {
    expect(delimiterError("\\\\p|", "regex")).toMatch(/matches an empty string/);
  });

  it("still rejects a broken pattern that happens to contain Java-only syntax", () => {
    expect(delimiterError("(?i)(", "regex")).toBeUndefined();
    expect(delimiterError("(", "regex")).toMatch(/Invalid regular expression/);
  });

  it.each([
    ["an unescaped dot", "."],
    ["anything but a line feed", "[^\\n]"],
    ["an unescaped dot, repeated", ".+"],
  ])("rejects %s, which matches every character except line breaks, leaving only those", (_, pattern) => {
    expect(delimiterError(pattern, "regex")).toMatch(
      /matches every character except line breaks, so only line breaks would be left/
    );
  });

  it("rejects a pattern matching every character, line breaks included, as leaving nothing", () => {
    const error = delimiterError("[\\s\\S]", "regex");
    expect(error).toMatch(/matches every character, so nothing would be left/);
    expect(error).not.toContain("line breaks");
  });

  // Every character is tested, not a sample, so a pattern leaving any character behind is a working
  // delimiter, however unusual.
  it.each([
    ["a class of many common characters, which still leaves the rest", '[aZé中09 \\t,;:|/._\\-"]', "a~b"],
    ["a class of exactly the old probe characters", "[a0-]", "Hello, a-0 x"],
    ["word characters and hyphens", "[\\w-]", "Hello, a-0 x"],
    ["a run of lowercase letters, digits and hyphens", "[a-z0-9-]+", "Hello, a-0 x"],
    ["everything but a comma", "[^,]", "a,b"],
    ["any non-space character", "\\S", "a b"],
  ])("accepts %s, which leaves some text", (_, pattern, sample) => {
    expect(delimiterError(pattern, "regex")).toBeUndefined();
    expect(sample.split(new RegExp(pattern)).some(piece => piece.length > 0)).toBe(true);
  });

  it("suggests the escape when a lone dot was probably meant literally", () => {
    expect(delimiterError(".", "regex")).toContain("\\.");
  });
});

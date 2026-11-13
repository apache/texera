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
  UiUdfParameter,
  ATTRIBUTE_TYPES_BY_TOKEN,
  INPUT_TYPES_BY_RESOURCE_TOKEN,
  createUiParameter,
} from "./ui-udf-parameter";

type Token = Readonly<{ text: string; kind: "name" | "string" | "symbol" }>;
const R_KEYWORDS = new Set(
  "if else repeat while function for in next break TRUE FALSE NULL Inf NaN NA NA_integer_ NA_real_ NA_complex_ NA_character_".split(
    " "
  )
);

const R_ESCAPES: Readonly<Record<string, string>> = {
  n: "\n",
  r: "\r",
  t: "\t",
  b: "\b",
  f: "\f",
  a: "\x07",
  v: "\v",
  "\\": "\\",
  "'": "'",
  '"': '"',
  "`": "`",
};

function decodeRString(text: string): string {
  let valid = true;
  const decoded = text.replace(
    /\\(u(?:\{[\da-fA-F]{1,4}\}|[\da-fA-F]{4})|U(?:\{[\da-fA-F]{1,8}\}|[\da-fA-F]{8})|x[\da-fA-F]{1,2}|[0-7]{1,3}|[\s\S])/g,
    (_, escaped: string) => {
      if (escaped in R_ESCAPES) return R_ESCAPES[escaped];
      const octal = /^[0-7]/.test(escaped);
      const point = parseInt(escaped.replace(/^[uUx]\{?|\}$/g, ""), octal ? 8 : 16);
      if ((octal || /^[uUx]/.test(escaped)) && (octal || point > 0) && point <= 0x10ffff)
        return String.fromCodePoint(point);
      valid = false;
      return "";
    }
  );
  return valid ? decoded : "";
}

/** Tokenize only the R syntax needed to recognize declarations, never evaluating user code. */
function tokenize(code: string): Token[] {
  const tokens: Token[] = [];
  for (let index = 0; index < code.length; ) {
    const rest = code.slice(index);
    if (/\s/.test(code[index])) {
      index++;
      continue;
    }
    if (code[index] === "#") {
      const end = code.indexOf("\n", index);
      index = end < 0 ? code.length : end + 1;
      continue;
    }
    // R raw strings may use paired (), [], or {} and any number of dashes.
    const raw = /^[rR](["'])(-*)([([{])/.exec(rest);
    if (raw) {
      const close = ({ "(": ")", "[": "]", "{": "}" } as Record<string, string>)[raw[3]] + raw[2] + raw[1];
      const end = code.indexOf(close, index + raw[0].length);
      if (end < 0) break;
      tokens.push({ text: code.slice(index + raw[0].length, end), kind: "string" });
      index = end + close.length;
      continue;
    }
    const quote = code[index];
    if (quote === '"' || quote === "'" || quote === "`") {
      const literal = /^(["'`])((?:\\[\s\S]|(?!\1)[^\\])*)\1/.exec(rest);
      if (!literal) break;
      tokens.push({ text: decodeRString(literal[2]), kind: quote === "`" ? "name" : "string" });
      index += literal[0].length;
      continue;
    }
    const name = /^[\p{L}_.][\p{L}\p{N}_.]*/u.exec(rest);
    if (name) {
      tokens.push({ text: name[0], kind: "name" });
      index += name[0].length;
      continue;
    }
    const symbol = /^(:::|::|<<-|<-|->>|->|.)/s.exec(rest)![0];
    tokens.push({ text: symbol, kind: "symbol" });
    index += symbol.length;
  }
  return tokens;
}

function member(tokens: Token[], receiver: string): string | undefined {
  return tokens.length === 3 &&
    tokens[0].kind === "name" &&
    tokens[0].text === receiver &&
    tokens[1].text === "$" &&
    tokens[2].kind === "name"
    ? tokens[2].text
    : undefined;
}

function readParameter(arguments_: Token[][]): UiUdfParameter | undefined {
  const args = new Map<string, Token[]>();
  const formalNames = ["name", "type", "value", "attr_type"];
  // R matches exact named arguments first, then unmatched positional arguments.
  for (const argument of arguments_) {
    if (argument[0]?.kind !== "name" || argument[1]?.text !== "=") continue;
    const key = argument[0].text;
    if (!formalNames.includes(key) || args.has(key)) return undefined;
    args.set(key, argument.slice(2));
  }
  for (const argument of arguments_) {
    if (argument[0]?.kind === "name" && argument[1]?.text === "=") continue;
    const key = formalNames.find(name => !args.has(name));
    if (!key) return undefined;
    args.set(key, argument);
  }
  const name = args.get("name");
  const attributeName = name?.length === 1 && name[0].kind === "string" ? name[0].text.trim() : "";
  if (args.has("type") && args.has("attr_type")) return undefined;
  const token = member(args.get("type") ?? args.get("attr_type") ?? [], "AttributeType");
  const attributeType = token ? ATTRIBUTE_TYPES_BY_TOKEN[token] : undefined;
  const resource = args.has("value") ? member(args.get("value")!, "Resource") : undefined;
  const inputType = resource ? INPUT_TYPES_BY_RESOURCE_TOKEN[resource] : undefined;
  if (args.has("value") && !inputType) return undefined;
  return createUiParameter(attributeName, attributeType, inputType);
}

export function parseRUiParameters(code: string): UiUdfParameter[] {
  const tokens = tokenize(code);
  const result: UiUdfParameter[] = [];
  for (let index = 0; index < tokens.length; index++) {
    if (
      tokens[index].kind !== "name" ||
      tokens[index].text !== "UiParameter" ||
      tokens[index + 1]?.text !== "(" ||
      ["$", "@", "::", ":::"].includes(tokens[index - 1]?.text)
    )
      continue;
    const arguments_: Token[][] = [[]];
    const stack = [")"];
    let end = index + 2;
    for (; end < tokens.length; end++) {
      const token = tokens[end];
      if (token.kind === "symbol") {
        const close = ({ "(": ")", "[": "]", "{": "}" } as Record<string, string>)[token.text];
        if (close) stack.push(close);
        else if ([")", "]", "}"].includes(token.text)) {
          if (stack.pop() !== token.text) break;
          if (!stack.length) break;
        } else if (token.text === "," && stack.length === 1) {
          arguments_.push([]);
          continue;
        }
      }
      arguments_[arguments_.length - 1].push(token);
    }
    if (stack.length || tokens[end]?.text !== ")") continue;
    const parameter = readParameter(arguments_);
    if (!parameter) continue;
    result.push(parameter);
  }
  return result;
}

export function computeRParameterInsertion(
  code: string,
  attributeName: string,
  token: string
): Readonly<{ offset: number; text: string }> {
  let identifier = attributeName.replace(/[^\p{L}\p{N}_.]/gu, "_");
  if (!/^(?:\p{L}|\.(?!\d))/u.test(identifier)) identifier = `parameter_${identifier}`;
  if (R_KEYWORDS.has(identifier)) identifier += "_";
  if (
    ["...", "UiParameter", "AttributeType", "Resource"].includes(identifier) ||
    /^\.\.[0-9]+$/.test(identifier) ||
    identifier.startsWith(".texera_")
  )
    identifier = `parameter_${identifier}`;
  const tokens = tokenize(code);
  const names = new Set<string>();
  let depth = 0;
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index];
    if (token.kind === "symbol") {
      if (["(", "[", "{"].includes(token.text)) depth++;
      else if ([")", "]", "}"].includes(token.text)) depth--;
    }
    if (
      depth === 0 &&
      token.kind === "name" &&
      (["<-", "<<-", "="].includes(tokens[index + 1]?.text) || ["->", "->>"].includes(tokens[index - 1]?.text))
    )
      names.add(token.text);
  }
  const base = identifier;
  for (let suffix = 2; names.has(identifier); suffix++) identifier = `${base}_${suffix}`;
  return {
    offset: 0,
    text: `${identifier} <- UiParameter(name=${JSON.stringify(attributeName)}, type=AttributeType$${token})$value\n`,
  };
}

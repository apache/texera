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

import { parser } from "@lezer/java";
type SyntaxNode = ReturnType<typeof parser.parse>["topNode"];
import {
  createUiParameter,
  JAVA_UI_PARAMETER_TYPES,
  UiUdfParameter,
  UiUdfParametersEditError,
  UiUdfParametersParseError,
} from "./ui-udf-parameter";

const ANNOTATION = "org.apache.texera.amber.operator.udf.java.UiParameter";
const JAVA_KEYWORDS = new Set(
  "abstract assert boolean break byte case catch char class const continue default do double else enum extends final finally float for goto if implements import instanceof int interface long native new package private protected public return short static strictfp super switch synchronized this throw throws transient try void volatile while true false null _ var yield record sealed permits".split(
    " "
  )
);
const JAVA_ESCAPES: Readonly<Record<string, string>> = {
  b: "\b",
  t: "\t",
  n: "\n",
  f: "\f",
  r: "\r",
  '"': '"',
  "'": "'",
  "\\": "\\",
  s: " ",
};

function children(node: SyntaxNode): SyntaxNode[] {
  const result: SyntaxNode[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) result.push(child);
  return result;
}

function udfBody(code: string): SyntaxNode | undefined {
  const classes = children(parser.parse(code).topNode).filter(
    node =>
      node.name === "ClassDeclaration" &&
      code.slice(node.getChild("Definition")?.from, node.getChild("Definition")?.to) === "JavaUDFOpExec"
  );
  if (classes.length > 1)
    throw new UiUdfParametersParseError("Only one JavaUDFOpExec class can declare UiParameter values.");
  return classes[0]?.getChild("ClassBody") ?? undefined;
}

function javaString(text: string): string | undefined {
  if (!/^"(?:[^"\\\r\n]|\\[\s\S])*"$/.test(text)) return undefined;
  let valid = true;
  const value = text
    .slice(1, -1)
    .replace(/\\(u+[\da-fA-F]{4}|[0-3][0-7]{0,2}|[4-7][0-7]?|[\s\S])/g, (_, escape: string) => {
      if (escape.startsWith("u")) return String.fromCharCode(parseInt(escape.replace(/^u+/, ""), 16));
      if (/^[0-7]/.test(escape)) return String.fromCharCode(parseInt(escape, 8));
      if (Object.hasOwn(JAVA_ESCAPES, escape)) return JAVA_ESCAPES[escape];
      valid = false;
      return "";
    });
  return valid ? value : undefined;
}

export function parseJavaUiParameters(code: string): UiUdfParameter[] {
  const body = udfBody(code);
  const result: UiUdfParameter[] = [];
  for (const field of body ? children(body) : []) {
    if (field.name !== "FieldDeclaration") continue;
    const modifiers = field.getChild("Modifiers");
    if (modifiers?.getChild("static")) continue;
    const declarations = field.getChildren("VariableDeclarator");
    if (declarations.length !== 1 || children(declarations[0]).some(node => node.name !== "Definition")) continue;
    const annotations =
      modifiers?.getChildren("Annotation").filter(annotation => {
        const name = annotation.firstChild;
        return name && ["UiParameter", ANNOTATION].includes(code.slice(name.from, name.to));
      }) ?? [];
    if (annotations.length !== 1) continue;
    const args = annotations[0].getChild("AnnotationArgumentList");
    const values = new Map<string, string>();
    let invalid = !args;
    for (const argument of args ? children(args) : []) {
      if (["(", ")", ","].includes(argument.name)) continue;
      const key =
        argument.name === "StringLiteral" ? "value" : code.slice(argument.firstChild?.from, argument.firstChild?.to);
      const literal = argument.name === "StringLiteral" ? argument : argument.getChild("StringLiteral");
      const value = literal ? javaString(code.slice(literal.from, literal.to)) : undefined;
      if (!["value", "resource"].includes(key) || value === undefined || values.has(key)) invalid = true;
      else values.set(key, value);
    }
    if (invalid) continue;
    const typeNode = children(field).find(node => ["PrimitiveType", "TypeName", "ScopedTypeName"].includes(node.name));
    const nativeType = typeNode ? code.slice(typeNode.from, typeNode.to).replace(/\s/g, "") : "";
    const type = Object.entries(JAVA_UI_PARAMETER_TYPES).find(([, names]) =>
      (names as string[]).includes(nativeType)
    )?.[0] as keyof typeof JAVA_UI_PARAMETER_TYPES | undefined;
    const parameter = createUiParameter(values.get("value")?.trim(), type, values.get("resource"));
    if (parameter) result.push(parameter);
  }
  return result;
}

export function computeJavaParameterInsertion(
  code: string,
  name: string,
  token: string
): Readonly<{ offset: number; text: string }> {
  const body = udfBody(code);
  if (!body || body.lastChild?.name !== "}")
    throw new UiUdfParametersEditError("A complete JavaUDFOpExec class is required to add UI parameters.");
  let identifier = name.replace(/[^\p{L}\p{N}_$]/gu, "_");
  if (!/^[\p{L}_$]/u.test(identifier)) identifier = `parameter_${identifier}`;
  // Generated initializers refer to the java package.
  if (JAVA_KEYWORDS.has(identifier) || ["UiParameter", "java"].includes(identifier)) identifier += "_";
  // Reserve every identifier in the class, including locals, to avoid generating shadowed fields.
  const names = new Set<string>();
  body.cursor().iterate(node => {
    if (["Definition", "Identifier"].includes(node.name)) names.add(code.slice(node.from, node.to));
  });
  const base = identifier;
  for (let suffix = 2; names.has(identifier); suffix++) identifier = `${base}_${suffix}`;
  const nativeType = JAVA_UI_PARAMETER_TYPES[token as keyof typeof JAVA_UI_PARAMETER_TYPES][0];
  const newline = code.includes("\r\n") ? "\r\n" : "\n";
  return {
    offset: body.from + 1,
    text: `${newline}    @${ANNOTATION}(${JSON.stringify(name)})${newline}    private ${nativeType} ${identifier};${newline}`,
  };
}

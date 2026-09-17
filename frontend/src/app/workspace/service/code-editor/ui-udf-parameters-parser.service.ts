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

import { Injectable } from "@angular/core";
import { parser } from "@lezer/python";
import { AttributeType, SchemaAttribute } from "../../types/workflow-compiling.interface";

// Keep in sync with Python UDF template class names in PythonUDFOpDescV2, DualInputPortsPythonUDFOpDescV2, and PythonUDFSourceOpDescV2.
const SUPPORTED_CLASS_NAMES = new Set([
  "ProcessTupleOperator",
  "ProcessBatchOperator",
  "ProcessTableOperator",
  "GenerateOperator",
]);

const PYTHON_NODE = {
  ARG_LIST: "ArgList",
  ASSIGN_OP: "AssignOp",
  BODY: "Body",
  CALL_EXPRESSION: "CallExpression",
  CLASS_DEFINITION: "ClassDefinition",
  EXPRESSION_STATEMENT: "ExpressionStatement",
  FUNCTION_DEFINITION: "FunctionDefinition",
  MEMBER_EXPRESSION: "MemberExpression",
  PROPERTY_NAME: "PropertyName",
  STRING: "String",
  VARIABLE_NAME: "VariableName",
} as const;
const ARGUMENT_DELIMITER_NODES = new Set(["(", ")", ","]);
// "⚠" is lezer's error node: a body whose real statements are still commented out ends in one.
const NON_STATEMENT_BODY_NODES = new Set([":", "Comment", "⚠"]);

const UI_PARAMETER_CALLEE = ["self", "UiParameter"];
const ATTRIBUTE_TYPE_RECEIVER = "AttributeType";
const ARGUMENT_NAME = "name";
const ARGUMENT_TYPE = "type";
const ARGUMENT_ATTR_TYPE = "attr_type";
const POSITIONAL_ARGUMENT_KEYS = [ARGUMENT_NAME, ARGUMENT_TYPE] as const;

type ParserSyntaxNode = ReturnType<typeof parser.parse>["topNode"];
type ParsedArgument = Readonly<{ key?: string; value: ParserSyntaxNode }>;
type UiParameterArgument =
  | Readonly<{ kind: typeof ARGUMENT_NAME; value: string }>
  | Readonly<{ kind: typeof ARGUMENT_TYPE; value: AttributeType }>;

/** UI parameter row inferred from Python code, with backend-compatible attribute metadata and an editable value. */
export type UiUdfParameter = Readonly<{ attribute: SchemaAttribute; value: string }>;

/** Raised when supported Python UDF code declares UI parameters that cannot be represented safely in the UI. */
export class UiUdfParametersParseError extends Error {}

/** Raised when a new UiParameter declaration cannot be inserted into the Python UDF code. */
export class UiUdfParametersEditError extends Error {}

// Accept Java enum names (INTEGER, BOOLEAN) and Python enum aliases (INT, BOOL).
const ATTRIBUTE_TYPES_BY_TOKEN: Readonly<Record<string, AttributeType>> = {
  STRING: "string",
  INTEGER: "integer",
  INT: "integer",
  LONG: "long",
  DOUBLE: "double",
  BOOLEAN: "boolean",
  BOOL: "boolean",
  TIMESTAMP: "timestamp",
};

// Python AttributeType member emitted when generating declarations; matches the pytexera enum and template style.
const PYTHON_TOKENS_BY_ATTRIBUTE_TYPE: Readonly<Partial<Record<AttributeType, string>>> = {
  string: "STRING",
  integer: "INT",
  long: "LONG",
  double: "DOUBLE",
  boolean: "BOOL",
  timestamp: "TIMESTAMP",
};

// Hard keywords only; soft keywords (match, case, type) are valid attribute names.
const PYTHON_KEYWORDS = new Set(
  (
    "False None True and as assert async await break class continue def del elif else except finally " +
    "for from global if import in is lambda nonlocal not or pass raise return try while with yield"
  ).split(" ")
);

/** Parses Python UDF source code and infers supported self.UiParameter(...) declarations for the property panel. */
@Injectable({ providedIn: "root" })
export class UiUdfParametersParserService {
  /**
   * Returns UI parameters from the single supported Python UDF class in the source.
   * Throws UiUdfParametersParseError for duplicate parameter names or multiple supported UDF classes.
   */
  parse(code: string): UiUdfParameter[] {
    if (!code) return [];

    const result: UiUdfParameter[] = [];
    const supportedClass = findSupportedClass(code);
    if (supportedClass)
      forEachUiParameterCall(supportedClass, code, parameter => {
        if (result.some(existing => existing.attribute.attributeName === parameter.attribute.attributeName))
          throw new UiUdfParametersParseError(
            `UiParameter name '${parameter.attribute.attributeName}' is declared more than once.`
          );
        result.push(parameter);
      });
    return result;
  }

  /**
   * Computes the text insertion that declares a new self.UiParameter(...) inside open() of the
   * supported Python UDF class, creating open() when the class does not define one.
   * Throws UiUdfParametersEditError when the declaration cannot be placed.
   */
  computeParameterInsertion(
    code: string,
    name: string,
    attributeType: AttributeType
  ): Readonly<{ offset: number; text: string }> {
    const attributeName = name.trim();
    const pythonToken = PYTHON_TOKENS_BY_ATTRIBUTE_TYPE[attributeType];
    if (!attributeName) throw new UiUdfParametersEditError("UiParameter name is required.");
    if (!pythonToken) throw new UiUdfParametersEditError(`UiParameter type '${attributeType}' is not supported.`);
    if (this.parse(code).some(parameter => parameter.attribute.attributeName === attributeName))
      throw new UiUdfParametersEditError(`UiParameter name '${attributeName}' is declared already.`);

    const declaration =
      `self.${toPythonIdentifier(attributeName)} = ` +
      `self.UiParameter(name=${JSON.stringify(attributeName)}, type=AttributeType.${pythonToken}).value`;
    const supportedClass = findSupportedClass(code);
    if (!supportedClass)
      throw new UiUdfParametersEditError(
        "No supported Python UDF class (such as ProcessTupleOperator) was found in the code."
      );

    const openMethod = findOpenMethod(supportedClass, code);
    if (openMethod) return insertIntoBody(code, openMethod, [declaration]);
    return insertIntoBody(code, supportedClass, [
      ...(/^\s*@overrides\b/m.test(code) ? ["@overrides"] : []),
      "def open(self) -> None:",
      `    ${declaration}`,
    ]);
  }
}

/** Returns the single supported UDF class; throws when several declare UiParameter-capable classes. */
function findSupportedClass(code: string): ParserSyntaxNode | undefined {
  const classes: ParserSyntaxNode[] = [];
  parser.parse(code).iterate({
    enter: ({ name, node }) => {
      const className = name === PYTHON_NODE.CLASS_DEFINITION ? node.getChild(PYTHON_NODE.VARIABLE_NAME) : null;
      if (!className || !SUPPORTED_CLASS_NAMES.has(code.slice(className.from, className.to))) return;
      classes.push(node);
      return false;
    },
  });
  if (classes.length > 1)
    throw new UiUdfParametersParseError("Only one Python UDF class can declare UiParameter values.");
  return classes[0];
}

function forEachUiParameterCall(
  supportedClass: ParserSyntaxNode,
  code: string,
  visit: (parameter: UiUdfParameter, call: ParserSyntaxNode) => void
): void {
  supportedClass.cursor().iterate(cursorReference => {
    if (cursorReference.name !== PYTHON_NODE.CALL_EXPRESSION) return;
    const parameter = readCall(cursorReference.node, code);
    if (parameter) visit(parameter, cursorReference.node);
    return false;
  });
}

function findOpenMethod(supportedClass: ParserSyntaxNode, code: string): ParserSyntaxNode | undefined {
  const body = supportedClass.getChild(PYTHON_NODE.BODY);
  for (const statement of body ? getChildren(body) : []) {
    const definition =
      statement.name === PYTHON_NODE.FUNCTION_DEFINITION
        ? statement
        : statement.getChild(PYTHON_NODE.FUNCTION_DEFINITION);
    const definitionName = definition?.getChild(PYTHON_NODE.VARIABLE_NAME);
    if (definition && definitionName && code.slice(definitionName.from, definitionName.to) === "open")
      return definition;
  }
  return undefined;
}

/**
 * Inserts lines at the start of a class or def body while preserving a leading docstring.
 * When the body has no real statement yet (for example a template whose statements are all
 * commented out), the lines go right after the header.
 */
function insertIntoBody(
  code: string,
  definition: ParserSyntaxNode,
  lines: string[]
): Readonly<{ offset: number; text: string }> {
  const body = definition.getChild(PYTHON_NODE.BODY);
  const statements = body ? getChildren(body).filter(child => !NON_STATEMENT_BODY_NODES.has(child.name)) : [];
  const first = statements[0];
  if (body && first && code.slice(body.from, first.from).includes("\n")) {
    const indent = lineIndentation(code, first.from);
    if (isDocstringStatement(first)) {
      const block = lines.map(line => `${indent}${line}`).join("\n");
      const leadingSeparator = lines.length > 1 ? "\n\n" : "\n";
      const trailingSeparator = lines.length > 1 && statements.length > 1 ? "\n" : "";
      return {
        offset: lineEnd(code, Math.max(first.from, first.to - 1)),
        text: `${leadingSeparator}${block}${trailingSeparator}`,
      };
    }
    const block = lines.map(line => `${indent}${line}\n`).join("");
    // A synthesized open() gets a blank separator line before the statement that follows it.
    return { offset: lineStart(code, first.from), text: lines.length > 1 ? `${block}\n` : block };
  }
  if (first || !body)
    throw new UiUdfParametersEditError(
      "The Python UDF class and open() need an indented block body to declare UiParameter values."
    );
  const indent = `${lineIndentation(code, definition.from)}    `;
  return { offset: lineEnd(code, body.from), text: lines.map(line => `\n${indent}${line}`).join("") };
}

function isDocstringStatement(statement: ParserSyntaxNode): boolean {
  return statement.name === PYTHON_NODE.EXPRESSION_STATEMENT && statement.getChild(PYTHON_NODE.STRING) !== null;
}

function toPythonIdentifier(name: string): string {
  const identifier = name.replace(/\W/g, "_").replace(/^(?=\d)/, "_") || "parameter";
  return PYTHON_KEYWORDS.has(identifier) ? `${identifier}_` : identifier;
}

function lineStart(code: string, position: number): number {
  return code.lastIndexOf("\n", position - 1) + 1;
}

function lineEnd(code: string, position: number): number {
  const newline = code.indexOf("\n", position);
  return newline === -1 ? code.length : newline;
}

function lineIndentation(code: string, position: number): string {
  return code.slice(lineStart(code, position), position).match(/^[ \t]*/)?.[0] ?? "";
}

function readCall(call: ParserSyntaxNode, code: string): UiUdfParameter | undefined {
  const argumentList = call.getChild(PYTHON_NODE.ARG_LIST);
  const callee = call.getChild(PYTHON_NODE.MEMBER_EXPRESSION);
  if (!argumentList || !isMemberPath(callee, code, UI_PARAMETER_CALLEE)) return undefined;

  let attributeName: string | undefined;
  let attributeType: AttributeType | undefined;
  const uiParameterArguments = readUiParameterArguments(argumentList, code);
  if (!uiParameterArguments) return undefined;

  for (const argument of uiParameterArguments) {
    if (argument.kind === ARGUMENT_NAME && !attributeName) attributeName = argument.value;
    else if (argument.kind === ARGUMENT_TYPE && !attributeType) attributeType = argument.value;
    else return undefined;
  }

  return attributeName && attributeType ? { attribute: { attributeName, attributeType }, value: "" } : undefined;
}

function readUiParameterArguments(argumentList: ParserSyntaxNode, code: string): UiParameterArgument[] | undefined {
  const result: UiParameterArgument[] = [];
  let positionalIndex = 0;
  let sawNamedArgument = false;

  for (const argument of readArguments(argumentList, code)) {
    if (argument.key) sawNamedArgument = true;
    else if (sawNamedArgument) return undefined;

    const key = argument.key ?? POSITIONAL_ARGUMENT_KEYS[positionalIndex++];
    const parsedArgument = readUiParameterArgument(key, argument.value, code);
    if (!parsedArgument) return undefined;
    result.push(parsedArgument);
  }

  return result;
}

function readUiParameterArgument(
  key: string | undefined,
  value: ParserSyntaxNode,
  code: string
): UiParameterArgument | undefined {
  if (key === ARGUMENT_NAME) {
    const attributeName = readName(value, code);
    return attributeName ? { kind: ARGUMENT_NAME, value: attributeName } : undefined;
  }
  if (key === ARGUMENT_TYPE || key === ARGUMENT_ATTR_TYPE) {
    const attributeType = readType(value, code);
    return attributeType ? { kind: ARGUMENT_TYPE, value: attributeType } : undefined;
  }
  return undefined;
}

function readArguments(argumentList: ParserSyntaxNode, code: string): ParsedArgument[] {
  const result: ParsedArgument[] = [];
  const children = getChildren(argumentList).filter(node => !ARGUMENT_DELIMITER_NODES.has(node.name));

  for (let index = 0; index < children.length; index++) {
    const node = children[index];

    if (node.name === PYTHON_NODE.VARIABLE_NAME && children[index + 1]?.name === PYTHON_NODE.ASSIGN_OP) {
      const value = children[index + 2];
      if (!value) return [];
      result.push({ key: code.slice(node.from, node.to), value });
      index += 2;
    } else if (node.name !== PYTHON_NODE.ASSIGN_OP) {
      result.push({ value: node });
    } else {
      return [];
    }
  }

  return result;
}

function getChildren(node: ParserSyntaxNode): ParserSyntaxNode[] {
  const children: ParserSyntaxNode[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) children.push(child);
  return children;
}

function readName(value: ParserSyntaxNode, code: string): string | undefined {
  const name = value.name === PYTHON_NODE.STRING ? readString(code.slice(value.from, value.to))?.trim() : undefined;
  return name || undefined;
}

function readType(value: ParserSyntaxNode, code: string): AttributeType | undefined {
  const parts = readMemberPath(value, code);
  if (parts?.length !== 2 || parts[0] !== ATTRIBUTE_TYPE_RECEIVER) return undefined;
  const token = parts[1].toUpperCase();
  return token ? ATTRIBUTE_TYPES_BY_TOKEN[token] : undefined;
}

function isMemberPath(node: ParserSyntaxNode | null, code: string, expectedParts: string[]): boolean {
  const parts = node ? readMemberPath(node, code) : undefined;
  return parts?.length === expectedParts.length && parts.every((part, index) => part === expectedParts[index]);
}

function readMemberPath(node: ParserSyntaxNode, code: string): string[] | undefined {
  if (node.name !== PYTHON_NODE.MEMBER_EXPRESSION) return undefined;
  const parts = getChildren(node)
    .filter(child => child.name === PYTHON_NODE.VARIABLE_NAME || child.name === PYTHON_NODE.PROPERTY_NAME)
    .map(child => code.slice(child.from, child.to));
  return parts.length ? parts : undefined;
}

function readString(input: string): string | undefined {
  return input
    .trim()
    .match(/^[rRuU]*(?:"""([\s\S]*)"""|'''([\s\S]*)'''|"((?:\\.|[^"\\])*)"|'((?:\\.|[^'\\])*)')$/)
    ?.slice(1)
    .find(value => value !== undefined);
}

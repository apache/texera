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

import { CustomJSONSchema7 } from "../types/custom-json-schema.interface";

/**
 * A loop-variable reference: a dollar sign and a name, as the whole value. The same grammar the backend
 * matches when it rewrites the property at parse time (issue #8635).
 */
const LOOP_VARIABLE_REFERENCE_PATTERN = /^\$[A-Za-z_][A-Za-z0-9_]*$/;

/** A complete integer numeral: an optional sign and digits. */
const COMPLETE_INTEGER = /^[+-]?\d+$/;

/**
 * A complete decimal numeral: an optional sign, digits with an optional fraction or a bare fraction, and
 * an optional exponent. A numeral still being typed ("7.", "-", "1e-") does not match.
 */
const COMPLETE_NUMBER = /^[+-]?(\d+(\.\d+)?|\.\d+)([eE][+-]?\d+)?$/;

/** The formly type that renders a primitive property able to take a reference; see LoopVariableInputComponent. */
export const LOOP_VARIABLE_INPUT_TYPE = "loopvariableinput";

/** The JSON-schema types whose property may hold a reference. */
export type PrimitiveSchemaType = "string" | "integer" | "number" | "boolean";
const PRIMITIVE_SCHEMA_TYPES: ReadonlySet<string> = new Set(["string", "integer", "number", "boolean"]);

export interface LoopVariableReference {
  /** JSON pointer of the property holding the reference, e.g. "/limit" or "/items/0/count". */
  pointer: string;
  /** The variable name without the dollar sign. */
  name: string;
}

/**
 * Whether the value is a reference. Deliberately not a type predicate: `string` values that are not
 * references must stay `string` in the else branch, which a predicate would narrow to `never`.
 */
export function isReference(value: unknown): boolean {
  return typeof value === "string" && LOOP_VARIABLE_REFERENCE_PATTERN.test(value);
}

/** "$K" -> "K"; undefined for anything that is not a reference. */
export function referenceName(value: unknown): string | undefined {
  return typeof value === "string" && isReference(value) ? value.slice(1) : undefined;
}

/**
 * The primitive a schema `type` names, reading through a nullable list (["integer", "null"] -> integer).
 * Undefined for containers, ambiguous lists and a missing type.
 */
export function primitiveSchemaType(type: unknown): PrimitiveSchemaType | undefined {
  const candidates = (Array.isArray(type) ? type : [type]).filter(candidate => candidate !== "null");
  if (candidates.length !== 1) {
    return undefined;
  }
  const candidate = candidates[0];
  return typeof candidate === "string" && PRIMITIVE_SCHEMA_TYPES.has(candidate)
    ? (candidate as PrimitiveSchemaType)
    : undefined;
}

/**
 * What the text typed into a reference-taking field stores in the operator properties: a reference stays
 * the literal string ("$K"), a complete numeral becomes a number on an integer/number field, true/false
 * text a boolean on a boolean field, empty text unsets a non-string field, and any other text is kept as
 * typed so that the field's type validator can flag it. A numeral still being typed ("7.", "-0.", "1e")
 * is such other text, so the key just typed is never taken away. A value that is not text is already
 * typed and passes.
 */
export function coerceOrReference(text: unknown, schemaType: PrimitiveSchemaType): unknown {
  if (typeof text !== "string" || schemaType === "string") {
    return text;
  }
  const trimmed = text.trim();
  if (isReference(trimmed)) {
    return trimmed;
  }
  if (trimmed === "") {
    return undefined;
  }
  switch (schemaType) {
    case "integer":
      return COMPLETE_INTEGER.test(trimmed) ? Number(trimmed) : text;
    case "number": {
      const parsed = COMPLETE_NUMBER.test(trimmed) ? Number(trimmed) : NaN;
      return Number.isFinite(parsed) ? parsed : text;
    }
    case "boolean": {
      const lower = trimmed.toLowerCase();
      return lower === "true" ? true : lower === "false" ? false : text;
    }
  }
}

/**
 * Judges a value against the declared loop variables: the warning for a reference to a name none of them
 * declares, undefined for a declared one and for anything that is not a reference. A warning, not an
 * error: the names are read from the Loop Starts' code, which can set a variable in a way the reading
 * misses, and a name no Loop Start sets stops the run with the backend's own message.
 */
export function unknownReferenceWarning(names: ReadonlyArray<string>): (value: unknown) => string | undefined {
  return value => {
    const name = referenceName(value);
    if (name === undefined || names.includes(name)) {
      return undefined;
    }
    return `${value} is not a variable of an enclosing block; the run will fail if no Loop Start sets it`;
  };
}

/** The autocomplete options: "$" + name for each declared variable. */
export function loopVariableOptions(names: ReadonlyArray<string>): string[] {
  return names.map(name => `$${name}`);
}

/** Every reference in an operator's properties, at any depth, in document order. */
export function collectReferences(properties: unknown): LoopVariableReference[] {
  const references: LoopVariableReference[] = [];
  const visit = (value: unknown, pointer: string): void => {
    const name = referenceName(value);
    if (name !== undefined) {
      references.push({ pointer, name });
    } else if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, `${pointer}/${index}`));
    } else if (value !== null && typeof value === "object") {
      for (const [key, item] of Object.entries(value)) {
        visit(item, `${pointer}/${escapePointerToken(key)}`);
      }
    }
  };
  visit(properties, "");
  return references;
}

/** The value a JSON pointer (RFC 6901, as ajv reports it) addresses in `data`; undefined when absent. */
export function valueAtPointer(data: unknown, pointer: string): unknown {
  if (pointer === "") {
    return data;
  }
  let current: unknown = data;
  for (const token of pointer.split("/").slice(1)) {
    if (current === null || typeof current !== "object") {
      return undefined;
    }
    current = (current as Record<string, unknown>)[token.replace(/~1/g, "/").replace(/~0/g, "~")];
  }
  return current;
}

function escapePointerToken(key: string): string {
  return key.replace(/~/g, "~0").replace(/\//g, "~1");
}

/**
 * The schema keyword the backend sets on each operator property the plan is built from: the compiler
 * rejects a reference there inside a block, since nothing binds it before the plan is built.
 */
const NO_LOOP_VARIABLE_KEYWORD = "noLoopVariable";

/** The keywords whose value is a schema or a list of schemas. */
const SUBSCHEMA_KEYWORDS = [
  "items",
  "additionalItems",
  "additionalProperties",
  "contains",
  "propertyNames",
  "not",
  "if",
  "then",
  "else",
  "allOf",
  "anyOf",
  "oneOf",
] as const;

/** The keywords whose value maps names to schemas. */
const SUBSCHEMA_MAP_KEYWORDS = ["properties", "patternProperties", "dependencies", "definitions"] as const;

type SchemaNode = Record<string, unknown>;

const isSchemaNode = (value: unknown): value is SchemaNode =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/**
 * A copy of an operator's schema in which every schema nested under a property marked `noLoopVariable`
 * carries the mark as well. The keyword sits on a top-level property only, but the property panel meets
 * each nested schema on its own (a Projection's `attributes[].alias` as the row's `alias`), so it is
 * spread down to them for the panel to see. A `$ref` under a marked property is pointed at a marked copy
 * of the schema it names, kept beside the original under `definitions`, so a definition that an
 * unmarked property shares stays unmarked; the copy is made once per `$ref`, so a definition that names
 * itself ends. Values a schema holds rather than is made of (`default`, `enum`, `valueRules`, ...) are
 * left as they are, and the schema given is not changed.
 */
export function inheritNoLoopVariable(schema: CustomJSONSchema7): CustomJSONSchema7 {
  const markedDefinitions: SchemaNode = {};
  const markedRefs = new Map<string, string>();

  const markedRef = (ref: string): string => {
    const known = markedRefs.get(ref);
    if (known !== undefined) {
      return known;
    }
    const target = ref.startsWith("#/") ? valueAtPointer(schema, ref.slice(1)) : undefined;
    if (!isSchemaNode(target)) {
      return ref;
    }
    const name = `${NO_LOOP_VARIABLE_KEYWORD}:${ref.slice(2).replace(/\//g, ".")}`;
    const copyRef = `#/definitions/${name}`;
    // recorded before the copy is made, so a definition that names itself points at the copy and ends
    markedRefs.set(ref, copyRef);
    markedDefinitions[name] = visit(target, true);
    return copyRef;
  };

  const visit = (node: unknown, marked: boolean): unknown => {
    if (Array.isArray(node)) {
      return node.map(item => visit(item, marked));
    }
    if (!isSchemaNode(node)) {
      return node;
    }
    const isMarked = marked || node[NO_LOOP_VARIABLE_KEYWORD] === true;
    const copy: SchemaNode = { ...node };
    if (isMarked) {
      copy[NO_LOOP_VARIABLE_KEYWORD] = true;
      if (typeof node["$ref"] === "string") {
        copy["$ref"] = markedRef(node["$ref"]);
      }
    }
    for (const keyword of SUBSCHEMA_KEYWORDS) {
      if (keyword in node) {
        copy[keyword] = visit(node[keyword], isMarked);
      }
    }
    for (const keyword of SUBSCHEMA_MAP_KEYWORDS) {
      const schemas = node[keyword];
      if (isSchemaNode(schemas)) {
        copy[keyword] = Object.fromEntries(
          Object.entries(schemas).map(([name, subschema]) => [name, visit(subschema, isMarked)])
        );
      }
    }
    return copy;
  };

  const result = visit(schema, false) as SchemaNode;
  if (markedRefs.size > 0) {
    result["definitions"] = { ...(result["definitions"] as SchemaNode | undefined), ...markedDefinitions };
  }
  return result as CustomJSONSchema7;
}

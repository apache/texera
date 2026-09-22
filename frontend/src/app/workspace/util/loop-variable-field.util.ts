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

import { AbstractControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";

/**
 * A loop-variable reference: a dollar sign and a name, as the whole value. The same grammar the backend
 * matches when it rewrites the property at parse time (issue #8635).
 */
export const LOOP_VARIABLE_REFERENCE_PATTERN = /^\$[A-Za-z_][A-Za-z0-9_]*$/;

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
 * Judges a value against the declared loop variables: the error message for a reference to a name none
 * of them declares, undefined for a declared one and for anything that is not a reference.
 */
export function referenceValidator(names: ReadonlyArray<string>): (value: unknown) => string | undefined {
  return value => {
    const name = referenceName(value);
    if (name === undefined || names.includes(name)) {
      return undefined;
    }
    return `${value} is not a variable of an enclosing block`;
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

/** The message shown when the text is neither the field's primitive nor a reference. */
export function typeMismatchMessage(schemaType: PrimitiveSchemaType): string {
  const expected = { string: "text", integer: "an integer", number: "a number", boolean: "true or false" }[schemaType];
  return `should be ${expected} or a $variable of an enclosing block`;
}

/**
 * Turns a primitive property's formly field into one that takes a loop-variable reference: the field
 * renders as the reference-taking text input offering "$" + name for each declared variable, typed text
 * is parsed with {@link coerceOrReference} (a string field keeps the mapper's own parsers and the ""
 * default of its plain control), the schema's type validator lets a reference through, and a reference
 * to an undeclared name is flagged with "$name is not a variable of an enclosing block". Errors show at
 * once rather than after a first edit.
 */
export function applyLoopVariableField(
  field: FormlyFieldConfig,
  schemaType: PrimitiveSchemaType,
  names: ReadonlyArray<string>
): void {
  field.type = LOOP_VARIABLE_INPUT_TYPE;
  field.props = { ...field.props, loopVariableOptions: loopVariableOptions(names) };
  if (schemaType === "string" && field.defaultValue === undefined) {
    // what the "string" formly type's defaultOptions give its plain control, lost with the type
    field.defaultValue = "";
  }
  if (schemaType !== "string") {
    field.parsers = [
      (value: unknown, fieldConfig?: FormlyFieldConfig) => {
        const parsed = coerceOrReference(value, schemaType);
        // formly sets the control to what a parser returns and, doing so, redraws the box with it: "1.0"
        // would read "1" and the next key would make it "15". Setting the control first, without the
        // redraw, leaves formly nothing to set. Formly's own number parser does the same.
        const control = fieldConfig?.formControl;
        if (control !== undefined && !Object.is(parsed, control.value)) {
          control.setValue(parsed, { emitModelToViewChange: false });
        }
        return parsed;
      },
    ];
  }

  const validators: NonNullable<FormlyFieldConfig["validators"]> = { ...(field.validators ?? {}) };
  const schemaTypeCheck = validators["type"];
  const checkSchemaType: unknown =
    typeof schemaTypeCheck === "function" ? schemaTypeCheck : schemaTypeCheck?.expression;
  // Formly's JSON-schema type check has no boolean case (it passes any value), so a boolean is checked here.
  const holdsSchemaType = (control: AbstractControl, fieldConfig: FormlyFieldConfig): boolean =>
    schemaType === "boolean"
      ? control.value === undefined || control.value === null || typeof control.value === "boolean"
      : typeof checkSchemaType === "function"
        ? Boolean(checkSchemaType(control, fieldConfig))
        : true;
  validators["type"] = {
    ...(schemaTypeCheck !== null && typeof schemaTypeCheck === "object" ? schemaTypeCheck : {}),
    expression: (control: AbstractControl, fieldConfig: FormlyFieldConfig) =>
      isReference(control.value) || holdsSchemaType(control, fieldConfig),
    message: typeMismatchMessage(schemaType),
  };
  const validate = referenceValidator(names);
  validators["loopVariableReference"] = {
    expression: (control: AbstractControl) => validate(control.value) === undefined,
    message: (_error: unknown, fieldConfig: FormlyFieldConfig) => validate(fieldConfig.formControl?.value) ?? "",
  };
  field.validators = validators;
  field.validation = { ...field.validation, show: true };
}

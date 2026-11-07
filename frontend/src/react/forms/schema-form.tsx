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

import { useEffect, useId, useMemo, useState, type ComponentType, type ReactNode } from "react";
import {
  applySchemaDefaults,
  asSchema,
  fieldIsHidden,
  getAtPath,
  itemSchema,
  matchingValueRule,
  newSchemaValue,
  resolveSchemaReference,
  schemaType,
  setAtPath,
  sortedProperties,
  validateSchemaValue,
  type OperatorPropertySchema,
  type PropertyIssue,
  type PropertyPath,
} from "./schema";

export interface PropertyWidgetProps {
  schema: OperatorPropertySchema;
  value: unknown;
  path: PropertyPath;
  disabled: boolean;
  required: boolean;
  issues: readonly PropertyIssue[];
  onChange: (value: unknown) => void;
}

export type PropertyWidgets = Readonly<Record<string, ComponentType<PropertyWidgetProps>>>;

export interface SchemaFormProps<T = unknown> {
  schema: OperatorPropertySchema;
  value: T;
  onChange: (value: T, issues: readonly PropertyIssue[]) => void;
  onValidationChange?: (issues: readonly PropertyIssue[]) => void;
  disabled?: boolean;
  widgets?: PropertyWidgets;
  label?: string;
}

/** A controlled operator form: saved values remain owned by the workflow store. */
export function SchemaForm<T>({
  schema,
  value,
  onChange,
  onValidationChange,
  disabled = false,
  widgets = {},
  label = "Operator properties",
}: SchemaFormProps<T>) {
  const model = useMemo(() => applySchemaDefaults(schema, value), [schema, value]);
  const issues = useMemo(() => validateSchemaValue(schema, model), [schema, model]);
  useEffect(() => {
    onValidationChange?.(issues);
  }, [issues, onValidationChange]);
  const update = (path: PropertyPath, next: unknown) => {
    if (disabled) return;
    const changed = setAtPath(model, path, next);
    onChange(changed as T, validateSchemaValue(schema, changed));
  };
  return (
    <div
      className="property-form"
      role="group"
      aria-label={label}>
      <PropertyField
        schema={schema}
        rootSchema={schema}
        model={model}
        path={[]}
        label={schema.title ?? ""}
        disabled={disabled}
        required={false}
        widgets={widgets}
        issues={issues}
        update={update}
      />
    </div>
  );
}

interface PropertyFieldProps {
  schema: OperatorPropertySchema;
  rootSchema: OperatorPropertySchema;
  model: unknown;
  path: PropertyPath;
  label: string;
  disabled: boolean;
  required: boolean;
  widgets: PropertyWidgets;
  issues: readonly PropertyIssue[];
  update: (path: PropertyPath, value: unknown) => void;
  selectedVariant?: boolean;
}

function samePath(left: PropertyPath, right: PropertyPath): boolean {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}

function PropertyField(props: PropertyFieldProps) {
  const { rootSchema, model, path, label, required, widgets, issues, update } = props;
  const schema = resolveSchemaReference(props.schema, rootSchema);
  const disabled = props.disabled || schema.readOnly === true;
  const value = getAtPath(model, path);
  const id = useId();
  const currentIssues = issues.filter(issue => samePath(issue.path, path));
  const variantSchemas = schema.oneOf ?? schema.anyOf;
  const [chosenVariant, setChosenVariant] = useState<number | undefined>();
  const variant =
    chosenVariant ??
    variantSchemas?.findIndex(candidate => validateSchemaValue(asSchema(candidate), value).length === 0) ??
    0;
  const title = `${label}${required ? " *" : ""}`;
  const errorId = `${id}-errors`;
  const descriptionId = `${id}-description`;
  const describedBy =
    [schema.description ? descriptionId : undefined, currentIssues.length ? errorId : undefined]
      .filter(Boolean)
      .join(" ") || undefined;
  const errors = currentIssues.length > 0 && (
    <ul
      id={errorId}
      role="alert"
      className="property-errors">
      {currentIssues.map((issue, index) => (
        <li key={`${issue.keyword}-${index}`}>{issue.message}</li>
      ))}
    </ul>
  );
  const description = schema.description && (
    <p
      id={descriptionId}
      className="property-description">
      {schema.description}
    </p>
  );
  const common = {
    id,
    disabled,
    required,
    "aria-invalid": currentIssues.length > 0 || undefined,
    "aria-describedby": describedBy,
  };
  const change = (next: unknown) => {
    if (!disabled) update(path, next);
  };
  const children = (content: ReactNode, showLabel = true) => (
    <div
      className="property-field"
      data-property-path={JSON.stringify(path)}>
      {showLabel && label && <label htmlFor={id}>{title}</label>}
      {content}
      {description}
      {errors}
    </div>
  );

  if (schema["x-widget"]) {
    const Widget = widgets[schema["x-widget"]];
    if (!Widget)
      return (
        <div role="alert">
          Unsupported field widget "{schema["x-widget"]}" for {label || "property"}
        </div>
      );
    return children(
      <Widget
        schema={schema}
        value={value}
        path={path}
        disabled={disabled}
        required={required}
        issues={currentIssues}
        onChange={change}
      />,
      false
    );
  }

  if (Array.isArray(schema.type) && schema.type.includes("null") && schema.type.length > 1) {
    const nonNullSchema = { ...schema, type: schema.type.filter(type => type !== "null") };
    return (
      <fieldset
        disabled={disabled}
        className="property-nullable">
        {label && <legend>{title}</legend>}
        <label htmlFor={id}>{label || "Value"} value type</label>
        <select
          id={id}
          disabled={disabled}
          value={value === null ? "null" : "value"}
          onChange={event => {
            change(event.currentTarget.value === "null" ? null : newSchemaValue(nonNullSchema));
          }}>
          <option value="value">{schemaType(schema) ?? "Value"}</option>
          <option value="null">Null</option>
        </select>
        {value !== null && (
          <PropertyField
            {...props}
            schema={nonNullSchema}
          />
        )}
        {value === null && errors}
      </fieldset>
    );
  }

  if (variantSchemas?.length && !props.selectedVariant) {
    const selection = variant >= 0 ? variant : 0;
    const branch = asSchema(variantSchemas[selection]);
    const merged = { ...schema, ...branch, oneOf: undefined, anyOf: undefined };
    return (
      <fieldset
        disabled={disabled}
        className="property-variant">
        {label && <legend>{title}</legend>}
        <label htmlFor={id}>{label || "Value"} type</label>
        <select
          {...common}
          value={selection}
          onChange={event => {
            const selected = Number(event.currentTarget.value);
            setChosenVariant(selected);
            change(newSchemaValue(asSchema(variantSchemas[selected])));
          }}>
          {variantSchemas.map((candidate, index) => (
            <option
              key={index}
              value={index}>
              {asSchema(candidate).title ?? schemaType(asSchema(candidate)) ?? `Option ${index + 1}`}
            </option>
          ))}
        </select>
        <PropertyField
          {...props}
          schema={merged}
          selectedVariant
        />
      </fieldset>
    );
  }

  const type = schemaType(schema);
  if (type === "object" || schema.properties) {
    const object = value !== null && typeof value === "object" ? value : {};
    return (
      <fieldset
        disabled={disabled}
        className="property-object"
        aria-describedby={describedBy}>
        {label && <legend>{title}</legend>}
        {description}
        {sortedProperties(schema)
          .filter(([key]) => !fieldIsHidden(key, schema, object))
          .map(([key, property]) => (
            <PropertyField
              key={key}
              {...props}
              schema={property}
              path={[...path, key]}
              label={property.title ?? key}
              disabled={disabled}
              required={isRequired(schema, key, object)}
              selectedVariant={false}
            />
          ))}
        {errors}
      </fieldset>
    );
  }

  if (type === "array" || schema.items) {
    const array = Array.isArray(value) ? value : [];
    const canAdd = !disabled && (schema.maxItems === undefined || array.length < schema.maxItems);
    const canRemove = !disabled && (schema.minItems === undefined || array.length > schema.minItems);
    const listLabel = label || "List";
    return (
      <fieldset
        disabled={disabled}
        className="property-array"
        aria-describedby={describedBy}>
        {label && <legend>{title}</legend>}
        {description}
        {array.map((_, index) => (
          <div
            key={index}
            className="property-array-item">
            <PropertyField
              {...props}
              schema={itemSchema(schema, index)}
              path={[...path, index]}
              label={`${listLabel} item ${index + 1}`}
              disabled={disabled}
              required={false}
              selectedVariant={false}
            />
            <div className="property-array-actions">
              <button
                type="button"
                disabled={disabled || index === 0}
                aria-label={`Move ${listLabel} item ${index + 1} up`}
                onClick={() => {
                  const next = [...array];
                  [next[index - 1], next[index]] = [next[index], next[index - 1]];
                  change(next);
                }}>
                Move up
              </button>
              <button
                type="button"
                disabled={disabled || index === array.length - 1}
                aria-label={`Move ${listLabel} item ${index + 1} down`}
                onClick={() => {
                  const next = [...array];
                  [next[index + 1], next[index]] = [next[index], next[index + 1]];
                  change(next);
                }}>
                Move down
              </button>
              <button
                type="button"
                disabled={!canRemove}
                aria-label={`Remove ${listLabel} item ${index + 1}`}
                onClick={() => change(array.filter((_, row) => row !== index))}>
                Remove
              </button>
            </div>
          </div>
        ))}
        <button
          type="button"
          disabled={!canAdd}
          aria-label={`Add ${listLabel} item`}
          onClick={() => change([...array, newSchemaValue(itemSchema(schema, array.length))])}>
          Add item
        </button>
        {errors}
      </fieldset>
    );
  }

  const valueRule = matchingValueRule(schema, getAtPath(model, path.slice(0, -1)));
  const options = valueRule?.enum ?? schema.enum;
  if (options) {
    const selected = options.findIndex(option => option === value);
    return children(
      <select
        {...common}
        value={selected < 0 ? "" : String(selected)}
        onChange={event =>
          change(event.currentTarget.value === "" ? undefined : options[Number(event.currentTarget.value)])
        }>
        <option value="">Select a value</option>
        {options.map((option, index) => (
          <option
            key={index}
            value={String(index)}>
            {typeof option === "object" ? JSON.stringify(option) : String(option)}
          </option>
        ))}
      </select>
    );
  }

  if (type === "boolean")
    return children(
      <input
        {...common}
        type="checkbox"
        checked={value === true}
        onChange={event => change(event.currentTarget.checked)}
      />
    );
  if (type === "null") return children(<output id={id}>null</output>);
  if (schema.const !== undefined) return children(<output id={id}>{String(schema.const)}</output>);
  if (type === "integer" || type === "number")
    return children(
      <input
        {...common}
        type="number"
        value={typeof value === "number" ? value : ""}
        min={schema.minimum}
        max={schema.maximum}
        step={schema.multipleOf ?? (type === "integer" ? 1 : "any")}
        onChange={event => change(event.currentTarget.value === "" ? undefined : event.currentTarget.valueAsNumber)}
      />
    );

  return children(
    <input
      {...common}
      type={schema.format === "password" ? "password" : "text"}
      value={typeof value === "string" ? value : value === undefined || value === null ? "" : String(value)}
      minLength={schema.minLength}
      maxLength={schema.maxLength}
      pattern={schema.pattern}
      onChange={event => change(event.currentTarget.value)}
    />
  );
}

function isRequired(schema: OperatorPropertySchema, key: string, model: unknown): boolean {
  if (schema.required?.includes(key)) return true;
  for (const branch of schema.allOf ?? []) {
    const condition = asSchema(branch);
    if (!condition.if) continue;
    const matched = validateSchemaValue(asSchema(condition.if), model).length === 0;
    const selected = asSchema(matched ? condition.then : condition.else);
    if (selected.required?.includes(key)) return true;
  }
  if (schema.if) {
    const matched = validateSchemaValue(asSchema(schema.if), model).length === 0;
    return asSchema(matched ? schema.then : schema.else).required?.includes(key) ?? false;
  }
  return false;
}

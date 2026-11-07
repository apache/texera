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
import { acceptsLoopReference } from "./loop-variables";
import { OperatorFormProvider, useOperatorFormContext, type OperatorFormContext } from "./operator-context";
import { BUILTIN_PROPERTY_WIDGETS, resolveOperatorWidget } from "./operator-widgets";
import { PresetControls } from "./presets";
import { huggingFaceFieldHidden } from "./hugging-face-rules";
import { applyInputAttributeSchema } from "./input-attributes";
import "./forms.css";
import {
  applySchemaDefaults,
  asSchema,
  fieldIsHidden,
  getAtPath,
  itemSchema,
  matchingValueRule,
  newSchemaValue,
  resolveSchemaReference,
  schemaWithReferenceRoot,
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
  parentValue?: unknown;
  onParentChange?: (updates: Record<string, unknown>) => void;
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
  context?: OperatorFormContext;
  referenceSchema?: OperatorPropertySchema;
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
  context,
  referenceSchema,
}: SchemaFormProps<T>) {
  const inherited = useOperatorFormContext();
  const formContext = useMemo(() => ({ ...inherited, ...context }), [inherited, context]);
  schema = useMemo(
    () => applyInputAttributeSchema(schema, formContext.inputAttributes),
    [schema, formContext.inputAttributes]
  );
  const referenceRoot = useMemo(
    () => applyInputAttributeSchema(referenceSchema ?? schema, formContext.inputAttributes),
    [referenceSchema, schema, formContext.inputAttributes]
  );
  const validationSchema = useMemo(() => schemaWithReferenceRoot(schema, referenceRoot), [schema, referenceRoot]);
  const model = useMemo(() => applySchemaDefaults(schema, value, referenceRoot), [schema, value, referenceRoot]);
  const issues = useMemo(
    () => validateSchemaValue(validationSchema, model, formContext),
    [validationSchema, model, formContext]
  );
  useEffect(() => {
    onValidationChange?.(issues);
  }, [issues, onValidationChange]);
  const update = (path: PropertyPath, next: unknown) => {
    if (disabled) return;
    const changed = setAtPath(model, path, next);
    onChange(changed as T, validateSchemaValue(validationSchema, changed, formContext));
  };
  return (
    <OperatorFormProvider value={formContext}>
      <div
        className="property-form"
        role="group"
        aria-label={label}>
        <PropertyField
          schema={schema}
          rootSchema={referenceRoot}
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
    </OperatorFormProvider>
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
  referenceBlocked?: boolean;
}

function samePath(left: PropertyPath, right: PropertyPath): boolean {
  return left.length === right.length && left.every((part, index) => part === right[index]);
}

function PropertyField(props: PropertyFieldProps) {
  if (props.schema.$ref) {
    const resolved = resolveSchemaReference(props.schema, props.rootSchema);
    props = { ...props, schema: resolved, label: resolved.title ?? props.label };
  }
  const context = useOperatorFormContext();
  const presentation = context.fieldPresentation;
  const lastKey = props.path.at(-1);
  if (lastKey === "dummyPropertyList" && context.operatorType !== "Dummy") return null;
  if (context.operatorType === "FileScanOp" && lastKey === "outputFileName") {
    const parent = getAtPath(props.model, props.path.slice(0, -1)) as Record<string, unknown> | undefined;
    if (
      parent?.extract !== true &&
      !["single string", "binary", "large binary"].includes(String(parent?.attributeType))
    )
      return null;
  }
  if (["dummyOperator", "dummyProperty", "dummyValue", "dummyPropertyList"].includes(String(lastKey)))
    props = { ...props, disabled: true };
  const key = props.path.filter(part => typeof part === "string").join(".");
  const override = presentation?.overrides?.[key];
  if (override?.hidden && !presentation?.authoring) return null;
  const originalLabel = props.label;
  const label = override?.displayName || originalLabel;
  const author = presentation?.authoring && key && !props.path.some(part => typeof part === "number" && part > 0);
  const expose =
    presentation?.showExposure &&
    props.path.length === 1 &&
    resolveOperatorWidget(props.schema, props.path[0]) !== "codearea";
  return (
    <div className={override?.hidden ? "property-presentation property-hidden-for-readers" : "property-presentation"}>
      {author && (
        <div className="property-author-controls">
          <label>
            Rename {originalLabel}
            <input
              value={override?.displayName ?? ""}
              placeholder={originalLabel}
              disabled={props.disabled}
              onChange={event => presentation?.onRename?.(key, event.currentTarget.value)}
            />
          </label>
          {props.path.length > 0 && presentation?.canHide?.(key) !== false && (
            <button
              type="button"
              disabled={props.disabled}
              aria-label={`${override?.hidden ? "Show" : "Hide"} ${originalLabel}`}
              onClick={() => presentation?.onHiddenChange?.(key, !override?.hidden)}>
              {override?.hidden ? "Show" : "Hide"}
            </button>
          )}
        </div>
      )}
      {expose && (
        <label>
          <input
            type="checkbox"
            disabled={props.disabled}
            checked={presentation?.exposedPaths?.includes(key) ?? false}
            onChange={event => presentation?.onExposedChange?.(key, event.currentTarget.checked)}
          />
          Expose {originalLabel} on form
        </label>
      )}
      <PropertyFieldControl
        {...props}
        label={label}
        schema={props.schema}
      />
    </div>
  );
}

function PropertyFieldControl(props: PropertyFieldProps) {
  const { rootSchema, model, path, label, required, widgets, issues, update } = props;
  let schema = resolveSchemaReference(props.schema, rootSchema);
  const context = useOperatorFormContext();
  if (schema.dependOn && context.inputAttributes) {
    const parent = getAtPath(model, path.slice(0, -1));
    const selected =
      parent && typeof parent === "object" ? (parent as Record<string, unknown>)[schema.dependOn] : undefined;
    const timestamp = Object.values(context.inputAttributes)
      .flat()
      .some(attribute => attribute?.attributeName === selected && attribute?.attributeType === "timestamp");
    schema = { ...schema, description: timestamp ? "Input a datetime string" : "Input a positive number" };
  }
  const referenceBlocked = props.referenceBlocked || schema.noLoopVariable === true;
  const disabled = props.disabled || schema.readOnly === true;
  const value = getAtPath(model, path);
  const id = useId();
  const currentIssues = issues.filter(issue => samePath(issue.path, path));
  const variantSchemas = schema.oneOf ?? schema.anyOf;
  const [chosenVariant, setChosenVariant] = useState<number | undefined>();
  const [draggedRow, setDraggedRow] = useState<number>();
  const [optionQuery, setOptionQuery] = useState("");
  const variant =
    chosenVariant ??
    variantSchemas?.findIndex(candidate => validateSchemaValue(asSchema(candidate), value).length === 0) ??
    0;
  const title = `${label}${required ? " *" : ""}`;
  const errorId = `${id}-errors`;
  const descriptionId = `${id}-description`;
  const describedBy =
    [
      schema.description && !context.fieldPresentation?.suppressDescriptions ? descriptionId : undefined,
      currentIssues.length ? errorId : undefined,
    ]
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
  const description = schema.description && !context.fieldPresentation?.suppressDescriptions && (
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
      {schema["enable-presets"] !== undefined &&
        context.userPresetEnabled &&
        context.presetRepository &&
        context.operatorType &&
        typeof path.at(-1) === "string" && (
          <PresetControls
            repository={context.presetRepository}
            operatorType={context.operatorType}
            field={String(path.at(-1))}
            schema={resolveSchemaReference(parentSchemaAtPath(rootSchema, path.slice(0, -1)), rootSchema)}
            model={getAtPath(model, path.slice(0, -1))}
            disabled={disabled}
            onApply={preset => {
              const parent = getAtPath(model, path.slice(0, -1));
              update(path.slice(0, -1), { ...(parent && typeof parent === "object" ? parent : {}), ...preset });
            }}
          />
        )}
    </div>
  );

  let widgetType = resolveOperatorWidget(schema, path.at(-1), context.operatorType);
  if (!widgetType && schema.valueRules) widgetType = "constrainedvalue";
  if (
    context.loopVariableNames !== undefined &&
    acceptsLoopReference(schema, path.at(-1), context.operatorType, referenceBlocked)
  )
    widgetType = "loopvariableinput";
  if (widgetType === "repeat-section-dnd" && (schemaType(schema) === "array" || schema.items)) widgetType = undefined;
  if (widgetType && ["string", "integer", "number", "boolean", "enum", "null", "array", "object"].includes(widgetType))
    widgetType = undefined;
  if (widgetType) {
    const Widget = widgets[widgetType] ?? BUILTIN_PROPERTY_WIDGETS[widgetType];
    if (!Widget)
      return (
        <div role="alert">
          Unsupported field widget "{widgetType}" for {label || "property"}
        </div>
      );
    return children(
      <Widget
        schema={{ ...schema, title: label }}
        value={value}
        path={path}
        disabled={disabled}
        required={required}
        issues={currentIssues}
        onChange={change}
        parentValue={getAtPath(model, path.slice(0, -1))}
        onParentChange={updates => {
          const parent = getAtPath(model, path.slice(0, -1));
          update(path.slice(0, -1), { ...(parent && typeof parent === "object" ? parent : {}), ...updates });
        }}
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
          .filter(
            ([key]) =>
              !fieldIsHidden(key, schema, object) &&
              !(context.operatorType === "HuggingFace" && huggingFaceFieldHidden(key, object))
          )
          .map(([key, property]) => (
            <PropertyField
              key={key}
              {...props}
              schema={property}
              referenceBlocked={referenceBlocked}
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
    const draggableRows = resolveOperatorWidget(schema, path.at(-1), context.operatorType) === "repeat-section-dnd";
    const moveRow = (from: number, to: number) => {
      if (disabled || !Number.isSafeInteger(from) || from < 0 || from >= array.length || from === to) return;
      const next = [...array];
      const [row] = next.splice(from, 1);
      next.splice(to, 0, row);
      change(next);
      context.onArrayReorder?.(path, from, to);
    };
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
            className="property-array-item"
            aria-label={draggableRows ? `Drop before ${listLabel} item ${index + 1}` : undefined}
            onDragOver={event => {
              if (draggableRows && !disabled) event.preventDefault();
            }}
            onDrop={event => {
              if (!draggableRows || disabled) return;
              event.preventDefault();
              try {
                const data = event.dataTransfer.getData("application/x-texera-array-row");
                if (data) {
                  const row = JSON.parse(data);
                  if (JSON.stringify(row.path) === JSON.stringify(path)) moveRow(row.from, index);
                } else if (draggedRow !== undefined) moveRow(draggedRow, index);
              } catch {
                /* An unrelated drag cannot reorder this list. */
              }
              setDraggedRow(undefined);
            }}>
            {draggableRows && (
              <button
                type="button"
                disabled={disabled}
                draggable={!disabled}
                aria-label={`Drag ${listLabel} item ${index + 1}`}
                onDragStart={event => {
                  setDraggedRow(index);
                  event.dataTransfer.setData("application/x-texera-array-row", JSON.stringify({ path, from: index }));
                }}
                onDragEnd={() => setDraggedRow(undefined)}>
                Drag
              </button>
            )}
            <PropertyField
              {...props}
              schema={itemSchema(schema, index)}
              referenceBlocked={referenceBlocked}
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
                  moveRow(index, index - 1);
                }}>
                Move up
              </button>
              <button
                type="button"
                disabled={disabled || index === array.length - 1}
                aria-label={`Move ${listLabel} item ${index + 1} down`}
                onClick={() => {
                  moveRow(index, index + 1);
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
      <>
        <label htmlFor={`${id}-search`}>
          Search {label || "value"} options
          <input
            id={`${id}-search`}
            type="search"
            disabled={disabled}
            value={optionQuery}
            onChange={event => setOptionQuery(event.currentTarget.value)}
          />
        </label>
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
              hidden={!!optionQuery && !String(option).toLowerCase().includes(optionQuery.toLowerCase())}
              value={String(index)}>
              {typeof option === "object" ? JSON.stringify(option) : String(option)}
            </option>
          ))}
        </select>
      </>
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
      type={
        schema.format === "password" || schema.widget?.formlyConfig?.templateOptions?.type === "password"
          ? "password"
          : "text"
      }
      value={typeof value === "string" ? value : value === undefined || value === null ? "" : String(value)}
      minLength={schema.minLength}
      maxLength={schema.maxLength}
      pattern={schema.pattern}
      onChange={event => change(event.currentTarget.value)}
    />
  );
}

function parentSchemaAtPath(root: OperatorPropertySchema, path: PropertyPath): OperatorPropertySchema {
  let schema = root;
  for (const part of path)
    schema = resolveSchemaReference(
      typeof part === "number" ? itemSchema(schema, part) : asSchema(schema.properties?.[part]),
      root
    );
  return schema;
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

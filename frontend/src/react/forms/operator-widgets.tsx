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

import { useEffect, useId, useRef, useState } from "react";
import { CUSTOM_DELIMITER, delimiterPresets, matchDelimiterPreset, unescapeCharDelimiter } from "./delimiter";
import { coerceOrReference, primitiveSchemaType, unknownReferenceWarning } from "./loop-variables";
import { useOperatorFormContext } from "./operator-context";
import { matchingValueRule } from "./schema";
import { ResourcePathField } from "./resource-picker";
import type { PropertyWidgetProps, PropertyWidgets } from "./schema-form";
import { HuggingFaceField } from "./hugging-face";
import { MediaUploadField } from "./media-upload";
import { UiUdfParametersField } from "./ui-udf-parameters";

export { resolveOperatorWidget } from "./widget-metadata";

export function DelimiterField({ schema, value, disabled, onChange }: PropertyWidgetProps) {
  const id = useId();
  const label = schema.title ?? "Delimiter";
  const mode = schema.widget?.formlyConfig?.props?.delimiterMode === "regex" ? "regex" : "char";
  const [custom, setCustom] = useState(false);
  const lastWritten = useRef(value);
  useEffect(() => {
    if (value !== lastWritten.current) setCustom(false);
    lastWritten.current = value;
  }, [value]);
  const preset = matchDelimiterPreset(value, mode);
  const selected =
    custom || (!preset && typeof value === "string" && value.length > 0) ? CUSTOM_DELIMITER : preset?.value ?? "";
  const write = (next: unknown) => {
    if (!disabled) {
      lastWritten.current = next;
      onChange(next);
    }
  };
  return (
    <div className="delimiter-field">
      <label htmlFor={id}>{label}</label>
      <select
        id={id}
        disabled={disabled}
        value={selected}
        onChange={event => {
          const choice = event.currentTarget.value;
          if (choice === CUSTOM_DELIMITER) {
            setCustom(true);
            if (preset) write("");
          } else {
            setCustom(false);
            write(choice || undefined);
          }
        }}>
        <option value="">Select a delimiter</option>
        {delimiterPresets(mode).map(option => (
          <option
            key={option.value}
            value={option.value}>
            {option.label}
          </option>
        ))}
        <option value={CUSTOM_DELIMITER}>{mode === "regex" ? "Custom regex…" : "Custom…"}</option>
      </select>
      {selected === CUSTOM_DELIMITER && (
        <label>
          Custom {label}
          <input
            disabled={disabled}
            value={typeof value === "string" ? value : ""}
            placeholder={mode === "regex" ? "Regular expression, e.g. [,;]\\s*" : "Any single character"}
            onChange={event => {
              const typed = event.currentTarget.value;
              const next = mode === "char" ? unescapeCharDelimiter(typed) : typed;
              setCustom(!(next !== typed && matchDelimiterPreset(next, mode)));
              write(next);
            }}
          />
        </label>
      )}
    </div>
  );
}

export function ConstrainedValueField({ schema, value, disabled, onChange, parentValue }: PropertyWidgetProps) {
  const id = useId();
  const rule = matchingValueRule(schema, parentValue);
  const options = rule?.enum ?? [];
  const text = value === null || value === undefined ? "" : String(value);
  return (
    <label htmlFor={id}>
      {schema.title ?? "Value"}
      {options.length ? (
        <select
          id={id}
          disabled={disabled}
          value={options.indexOf(text) < 0 ? "" : String(options.indexOf(text))}
          onChange={event => {
            if (!disabled) onChange(event.currentTarget.value === "" ? "" : options[Number(event.currentTarget.value)]);
          }}>
          <option value="">Select a value</option>
          {options.map((option, index) => (
            <option
              key={option}
              value={index}>
              {option}
            </option>
          ))}
        </select>
      ) : (
        <input
          id={id}
          disabled={disabled}
          type={rule?.type ? "number" : "text"}
          value={text}
          min={rule?.minimum}
          step={rule?.type === "integer" ? 1 : "any"}
          onChange={event => {
            if (!disabled) onChange(event.currentTarget.value);
          }}
        />
      )}
    </label>
  );
}

export function LoopVariableField({ schema, value, disabled, onChange, parentValue }: PropertyWidgetProps) {
  const id = useId();
  const context = useOperatorFormContext();
  const names = context.loopVariableNames ?? [];
  const [typed, setTyped] = useState(String(value ?? ""));
  const lastWritten = useRef(value);
  useEffect(() => {
    if (!Object.is(lastWritten.current, value)) setTyped(String(value ?? ""));
    lastWritten.current = value;
  }, [value]);
  const references = names.map(name => `$${name}`);
  const accepted = matchingValueRule(schema, parentValue)?.enum ?? [];
  const suggestions =
    typed === ""
      ? [...references, ...accepted]
      : (typed.startsWith("$") ? references : accepted).filter(option =>
          option.toLowerCase().startsWith(typed.toLowerCase())
        );
  const warning = unknownReferenceWarning(value, names);
  return (
    <div className="loop-variable-field">
      <label htmlFor={id}>{schema.title ?? "Value"}</label>
      <input
        id={id}
        type="text"
        list={`${id}-options`}
        disabled={disabled}
        value={typed}
        onChange={event => {
          if (disabled) return;
          const raw = event.currentTarget.value;
          setTyped(raw);
          const next = schema.valueRules ? raw : coerceOrReference(raw, primitiveSchemaType(schema) ?? "string");
          lastWritten.current = next;
          onChange(next);
        }}
      />
      <datalist id={`${id}-options`}>
        {suggestions.map((option, index) => (
          <option
            key={`${option}-${index}`}
            value={option}
          />
        ))}
      </datalist>
      {warning && (
        <p
          role="alert"
          className="property-warning">
          {warning}
        </p>
      )}
    </div>
  );
}

function TextareaField({ schema, value, disabled, onChange }: PropertyWidgetProps) {
  const id = useId();
  return (
    <label htmlFor={id}>
      {schema.title ?? "Value"}
      <textarea
        id={id}
        disabled={disabled}
        value={String(value ?? "")}
        rows={Number(schema.widget?.formlyConfig?.templateOptions?.autosizeMinRows ?? 3)}
        onChange={event => {
          if (!disabled) onChange(event.currentTarget.value);
        }}
      />
    </label>
  );
}

export const BUILTIN_PROPERTY_WIDGETS: PropertyWidgets = {
  huggingface: HuggingFaceField,
  "huggingface-image-upload": props => (
    <MediaUploadField
      {...props}
      kind="image"
    />
  ),
  "huggingface-audio-upload": props => (
    <MediaUploadField
      {...props}
      kind="audio"
    />
  ),
  "ui-udf-parameters": UiUdfParametersField,
  delimiter: DelimiterField,
  constrainedvalue: ConstrainedValueField,
  loopvariableinput: LoopVariableField,
  textarea: TextareaField,
  inputautocomplete: props => (
    <ResourcePathField
      {...props}
      mode="file"
    />
  ),
  datasetversionselector: props => <ResourcePathField {...props} />,
  resourcevalue: props => (
    <ResourcePathField
      {...props}
      compact
      resource={props.schema.widget?.formlyConfig?.props?.resource === "model" ? "model" : "dataset"}
    />
  ),
};

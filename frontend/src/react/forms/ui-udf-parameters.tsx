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

import { useState } from "react";
import contract from "../../../../common/workflow-operator/src/main/resources/udf/ui-parameter-types.json";
import { useOperatorFormContext } from "./operator-context";
import { ResourcePathField } from "./resource-picker";
import type { PropertyWidgetProps } from "./schema-form";

export interface UiUdfParameterValue {
  attribute: { attributeName: string; attributeType: string };
  value: string;
  inputType?: string;
}
export const UI_PARAMETER_TYPES = Object.keys(contract.types);

/** Declaration metadata is immutable here; additions go through the shared code synchronizer. */
export function UiUdfParametersField(props: PropertyWidgetProps) {
  const { value, onChange, disabled } = props;
  const { addUdfParameter } = useOperatorFormContext();
  const rows = Array.isArray(value) ? (value as UiUdfParameterValue[]) : [];
  const [draft, setDraft] = useState(false);
  const [name, setName] = useState("");
  const [type, setType] = useState(UI_PARAMETER_TYPES[0]);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const add = async () => {
    if (disabled || !addUdfParameter || pending) return;
    setError(undefined);
    const candidate = name.trim();
    if (!candidate) {
      setError("UiParameter name is required.");
      return;
    }
    if (rows.some(row => row.attribute?.attributeName === candidate)) {
      setError(`UiParameter name '${candidate}' is declared already.`);
      return;
    }
    setPending(true);
    try {
      await addUdfParameter(candidate, type);
      setDraft(false);
      setName("");
    } catch (reason) {
      setError(`Could not add UDF parameter: ${reason instanceof Error ? reason.message : String(reason)}`);
    } finally {
      setPending(false);
    }
  };
  return (
    <div className="ui-udf-parameters-field">
      {!disabled && addUdfParameter && !draft && (
        <button
          type="button"
          onClick={() => setDraft(true)}>
          Add parameter
        </button>
      )}
      {error && <p role="alert">{error}</p>}
      {draft && (
        <div className="ui-udf-parameter-draft">
          <label>
            Parameter name
            <input
              value={name}
              disabled={pending}
              onChange={event => setName(event.currentTarget.value)}
              onKeyDown={event => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void add();
                }
                if (event.key === "Escape") setDraft(false);
              }}
            />
          </label>
          <label>
            Parameter type
            <select
              value={type}
              disabled={pending}
              onChange={event => setType(event.currentTarget.value)}>
              {UI_PARAMETER_TYPES.map(option => (
                <option
                  key={option}
                  value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            disabled={pending}
            onClick={() => void add()}>
            Insert parameter
          </button>
          <button
            type="button"
            disabled={pending}
            onClick={() => setDraft(false)}>
            Cancel parameter
          </button>
        </div>
      )}
      {rows.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Value</th>
              <th>Name</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => {
              const attributeName = row.attribute?.attributeName ?? `row ${index + 1}`;
              const write = (next: unknown) => {
                if (!disabled)
                  onChange(
                    rows.map((item, position) =>
                      position === index
                        ? { ...item, value: next === null || next === undefined ? "" : String(next) }
                        : item
                    )
                  );
              };
              const resource =
                row.inputType === contract.resources.MODEL
                  ? "model"
                  : row.inputType === contract.resources.DATASET
                    ? "dataset"
                    : undefined;
              return (
                <tr key={attributeName}>
                  <td>
                    {resource ? (
                      <ResourcePathField
                        {...props}
                        schema={{ type: "string", title: `Parameter ${attributeName} value` }}
                        value={row.value}
                        compact
                        resource={resource}
                        onChange={write}
                      />
                    ) : (
                      <input
                        aria-label={`Parameter ${attributeName} value`}
                        disabled={disabled}
                        value={row.value ?? ""}
                        onChange={event => write(event.currentTarget.value)}
                      />
                    )}
                  </td>
                  <td>
                    <input
                      aria-label={`Parameter ${attributeName} name`}
                      disabled
                      value={row.attribute?.attributeName ?? ""}
                    />
                  </td>
                  <td>
                    <input
                      aria-label={`Parameter ${attributeName} type`}
                      disabled
                      value={row.attribute?.attributeType ?? ""}
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

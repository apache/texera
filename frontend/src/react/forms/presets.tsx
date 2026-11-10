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

import { useEffect, useState } from "react";
import Ajv from "ajv";
import type { ApiClient } from "../core/api-client";
import type { OperatorPropertySchema } from "./schema";

export type OperatorPreset = Record<string, string>;
export interface PresetRepository {
  list(operatorType: string, signal?: AbortSignal): Promise<OperatorPreset[]>;
  create(operatorType: string, preset: OperatorPreset): Promise<void>;
  remove(operatorType: string, preset: OperatorPreset): Promise<void>;
}
const isPreset = new Ajv().compile({ type: "object", additionalProperties: { type: "string", pattern: "^\\S.*$" } });
const canonical = (preset: OperatorPreset) =>
  JSON.stringify(Object.fromEntries(Object.entries(preset).sort(([a], [b]) => a.localeCompare(b))));

export function createPresetRepository(api: Pick<ApiClient, "get" | "put" | "delete">): PresetRepository {
  const mutations = new Map<string, Promise<void>>();
  const exclusive = (type: string, action: () => Promise<void>): Promise<void> => {
    const current = (mutations.get(type) ?? Promise.resolve()).catch(() => undefined).then(action);
    mutations.set(type, current);
    void current
      .finally(() => {
        if (mutations.get(type) === current) mutations.delete(type);
      })
      .catch(() => undefined);
    return current;
  };
  const path = (type: string) => {
    if (!type.trim()) throw new Error("Operator type is required");
    return `user/config/${encodeURIComponent(`operator-${type}`)}`;
  };
  const list = async (type: string, signal?: AbortSignal): Promise<OperatorPreset[]> => {
    const text = await api.get<string | null>(path(type), { responseType: "text", signal });
    let rows: unknown;
    try {
      rows = JSON.parse(text || "[]");
    } catch {
      throw new Error("Stored preset data is formatted incorrectly");
    }
    if (
      !Array.isArray(rows) ||
      !rows.every(row => isPreset(row)) ||
      new Set(rows.map(row => canonical(row as OperatorPreset))).size !== rows.length
    )
      throw new Error("Stored preset data is formatted incorrectly");
    return rows as OperatorPreset[];
  };
  const save = async (type: string, rows: OperatorPreset[]) => {
    if (rows.length) await api.put(path(type), JSON.stringify(rows), { headers: { "Content-Type": "text/plain" } });
    else await api.delete(path(type));
  };
  return {
    list,
    async create(type, preset) {
      if (!isPreset(preset) || !Object.keys(preset).length)
        throw new Error("Preset not saved: Fill out all preset fields.");
      await exclusive(type, async () => {
        const rows = await list(type);
        if (rows.some(row => canonical(row) === canonical(preset)))
          throw new Error("attempting to create preset that already exists");
        await save(type, [...rows, preset]);
      });
    },
    async remove(type, preset) {
      await exclusive(type, async () => {
        const rows = await list(type);
        await save(
          type,
          rows.filter(row => canonical(row) !== canonical(preset))
        );
      });
    },
  };
}

export function PresetControls({
  repository,
  operatorType,
  field,
  schema,
  model,
  onApply,
  disabled,
}: {
  repository: PresetRepository;
  operatorType: string;
  field: string;
  schema: OperatorPropertySchema;
  model: unknown;
  onApply: (preset: OperatorPreset) => void;
  disabled: boolean;
}) {
  const [rows, setRows] = useState<OperatorPreset[]>([]);
  const [visible, setVisible] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [reload, setReload] = useState(0);
  const [pending, setPending] = useState(false);
  const object = model && typeof model === "object" ? (model as Record<string, unknown>) : {};
  const [search, setSearch] = useState("");
  useEffect(() => {
    if (!visible) return;
    let active = true;
    const controller = new AbortController();
    setError(undefined);
    void repository
      .list(operatorType, controller.signal)
      .then(presets => {
        if (active) setRows(presets);
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Presets could not be loaded.");
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [repository, operatorType, visible, reload]);
  const perform = async (action: () => Promise<void>, success: string) => {
    if (disabled || pending) return;
    setPending(true);
    setError(undefined);
    setMessage(undefined);
    try {
      await action();
      setReload(reload + 1);
      setMessage(success);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Preset operation failed.");
    } finally {
      setPending(false);
    }
  };
  return (
    <div className="operator-presets">
      <button
        type="button"
        disabled={disabled}
        onClick={() => {
          setVisible(!visible);
          setSearch("");
        }}>
        Presets for {field}
      </button>
      {visible && (
        <div>
          <label>
            Search presets
            <input
              value={search}
              onChange={event => setSearch(event.currentTarget.value)}
            />
          </label>
          <ul>
            {rows
              .filter(row =>
                String(row[field] ?? "")
                  .trim()
                  .toLowerCase()
                  .startsWith(search.toLowerCase())
              )
              .map(row => (
                <li key={canonical(row)}>
                  <button
                    type="button"
                    disabled={disabled || pending}
                    title={Object.entries(row)
                      .filter(([key]) => key !== field)
                      .map(([, value]) => value)
                      .join(", ")}
                    onClick={() => {
                      onApply(row);
                      setVisible(false);
                    }}>
                    {row[field] ?? "Preset"}
                  </button>
                  <button
                    type="button"
                    disabled={disabled || pending}
                    aria-label={`Delete preset ${row[field]}`}
                    onClick={() => void perform(() => repository.remove(operatorType, row), "Preset deleted")}>
                    Delete
                  </button>
                </li>
              ))}
          </ul>
          <button
            type="button"
            disabled={disabled || pending}
            onClick={() =>
              void perform(async () => {
                const preset: OperatorPreset = {};
                for (const [key, property] of Object.entries(schema.properties ?? {})) {
                  if (typeof property === "boolean" || property["enable-presets"] === undefined) continue;
                  if (typeof object[key] !== "string" || !String(object[key]).trim())
                    throw new Error("Preset not saved: Fill out all preset fields.");
                  preset[key] = String(object[key]);
                }
                await repository.create(operatorType, preset);
              }, "Preset saved")
            }>
            Save preset
          </button>
        </div>
      )}
      {error && <p role="alert">{error}</p>}
      {message && <p role="status">{message}</p>}
    </div>
  );
}

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

import { useEffect, useRef, useState } from "react";
import { useOperatorFormContext } from "./operator-context";
import type { PropertyWidgetProps } from "./schema-form";

export interface HuggingFaceTask {
  tag: string;
  label: string;
}
export interface HuggingFaceModel {
  id: string;
  label: string;
  pipeline_tag?: string;
  downloads?: number;
  likes?: number;
}
export interface HuggingFaceCatalog {
  tasks(signal?: AbortSignal): Promise<HuggingFaceTask[]>;
  models(
    task: string,
    search?: string,
    signal?: AbortSignal
  ): Promise<{ models: HuggingFaceModel[]; truncated: boolean }>;
}

export const HUGGING_FACE_TASKS: readonly HuggingFaceTask[] = [
  { tag: "text-generation", label: "Text Generation" },
  { tag: "question-answering", label: "Question Answering" },
  { tag: "table-question-answering", label: "Table Question Answering" },
  { tag: "zero-shot-classification", label: "Zero-Shot Classification" },
  { tag: "sentence-similarity", label: "Sentence Similarity" },
  { tag: "text-ranking", label: "Text Ranking" },
  { tag: "automatic-speech-recognition", label: "Automatic Speech Recognition" },
  { tag: "audio-classification", label: "Audio Classification" },
  { tag: "text-to-speech", label: "Text to Speech" },
  { tag: "image-classification", label: "Image Classification" },
  { tag: "object-detection", label: "Object Detection" },
  { tag: "image-segmentation", label: "Image Segmentation" },
  { tag: "image-to-text", label: "Image to Text" },
  { tag: "image-to-image", label: "Image to Image" },
  { tag: "image-text-to-text", label: "Image-Text to Text" },
  { tag: "visual-question-answering", label: "Visual Question Answering" },
  { tag: "document-question-answering", label: "Document Question Answering" },
  { tag: "zero-shot-image-classification", label: "Zero-Shot Image Classification" },
  { tag: "text-to-image", label: "Text to Image" },
  { tag: "text-to-video", label: "Text to Video" },
];
const supported = new Set(HUGGING_FACE_TASKS.map(task => task.tag));
const defaults = {
  modelId: "",
  promptColumn: "",
  imageInput: "",
  audioInput: "",
  inputImageColumn: "",
  inputAudioColumn: "",
  candidateLabels: "",
  sentencesColumn: "",
  contextColumn: "",
  systemPrompt: "You are a helpful assistant.",
  maxNewTokens: 256,
  temperature: 0.7,
};
const taskKeys = Object.keys(defaults);

/** The proxy is same-origin; identity-provider credentials are never forwarded to the Hub. */
export function createHuggingFaceCatalog(
  options: { fetch?: typeof fetch; getToken?: () => string | null } = {}
): HuggingFaceCatalog {
  const fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  const token = options.getToken ?? (() => localStorage.getItem("access_token"));
  let taskCache: HuggingFaceTask[] | undefined;
  const modelCache = new Map<string, { models: HuggingFaceModel[]; truncated: boolean }>();
  const request = async (path: string, signal?: AbortSignal) => {
    const headers = new Headers({ Accept: "application/json" });
    const credential = token();
    if (credential) headers.set("Authorization", `Bearer ${credential}`);
    const response = await fetcher(`/api/huggingface/${path}`, { headers, signal, credentials: "same-origin" });
    if (!response.ok) throw new Error(`Hugging Face request failed (${response.status})`);
    return response;
  };
  return {
    async tasks(signal) {
      if (taskCache) return taskCache;
      const rows: unknown = await (await request("tasks", signal)).json();
      if (!Array.isArray(rows) || !rows.every(row => typeof row?.tag === "string" && typeof row?.label === "string"))
        throw new Error("Invalid Hugging Face tasks response");
      taskCache = rows as HuggingFaceTask[];
      return taskCache;
    },
    async models(task, search, signal) {
      if (search === undefined && modelCache.has(task)) return modelCache.get(task)!;
      const query = new URLSearchParams({ task });
      if (search !== undefined) query.set("search", search);
      const response = await request(`models?${query}`, signal);
      const rows: unknown = await response.json();
      if (!Array.isArray(rows) || !rows.every(row => typeof row?.id === "string"))
        throw new Error("Invalid Hugging Face models response");
      const result = {
        models: (rows as HuggingFaceModel[]).map(row => ({ ...row, label: row.label ?? row.id })),
        truncated: response.headers.get("X-Texera-Truncated") === "true",
      };
      if (search === undefined) modelCache.set(task, result);
      return result;
    },
  };
}

export function HuggingFaceField({ value, disabled, onChange, parentValue, onParentChange }: PropertyWidgetProps) {
  const catalog = useOperatorFormContext().huggingFaceCatalog;
  const row = parentValue !== null && typeof parentValue === "object" ? (parentValue as Record<string, unknown>) : {};
  const task = typeof row.task === "string" && row.task.trim() ? row.task : "text-generation";
  const [tasks, setTasks] = useState<readonly HuggingFaceTask[]>(HUGGING_FACE_TASKS);
  const [taskError, setTaskError] = useState<string>();
  const [modelError, setModelError] = useState<string>();
  const [models, setModels] = useState<HuggingFaceModel[]>([]);
  const [filtered, setFiltered] = useState<HuggingFaceModel[]>();
  const [truncated, setTruncated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const [taskRetry, setTaskRetry] = useState(0);
  const [modelRetry, setModelRetry] = useState(0);
  const snapshots = useRef(new Map<string, Record<string, unknown>>());

  useEffect(() => {
    if (!catalog) {
      setTaskError("Hugging Face selection is unavailable.");
      return;
    }
    const controller = new AbortController();
    let active = true;
    setTaskError(undefined);
    void catalog
      .tasks(controller.signal)
      .then(rows => {
        const supportedRows = rows.filter(option => supported.has(option.tag));
        if (active) setTasks(supportedRows.length ? supportedRows : HUGGING_FACE_TASKS);
      })
      .catch(() => {
        if (active) {
          setTasks(HUGGING_FACE_TASKS);
          setTaskError("Could not load tasks from Hugging Face. Using default list.");
        }
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [catalog, taskRetry]);

  useEffect(() => {
    if (!catalog) return;
    const controller = new AbortController();
    let active = true;
    setLoading(true);
    setModelError(undefined);
    setModels([]);
    setFiltered(undefined);
    setPage(0);
    void catalog
      .models(task, undefined, controller.signal)
      .then(result => {
        if (active) {
          setModels(result.models);
          setTruncated(result.truncated);
        }
      })
      .catch(reason => {
        if (active) setModelError(reason instanceof Error ? reason.message : "Failed to load models.");
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [catalog, task, modelRetry]);

  useEffect(() => {
    setFiltered(undefined);
    setPage(0);
    if (!query.trim() || !truncated || !catalog) return;
    const controller = new AbortController();
    let active = true;
    const timer = setTimeout(() => {
      void catalog
        .models(task, query, controller.signal)
        .then(result => {
          if (active) setFiltered(result.models);
        })
        .catch(reason => {
          if (active) setModelError(reason instanceof Error ? reason.message : "Model search failed.");
        });
    }, 300);
    return () => {
      active = false;
      clearTimeout(timer);
      controller.abort();
    };
  }, [catalog, task, truncated, query]);

  const activeModels =
    filtered ??
    (query.trim() && !truncated
      ? models.filter(model => model.id.toLowerCase().includes(query.toLowerCase()))
      : models);
  const pages = Math.max(1, Math.ceil(activeModels.length / 50));
  const activePage = Math.min(page, pages - 1);
  return (
    <div className="hugging-face-field">
      <label>
        Task
        <select
          disabled={disabled}
          value={task}
          onChange={event => {
            if (disabled) return;
            snapshots.current.set(task, Object.fromEntries(taskKeys.map(key => [key, row[key]])));
            const next = event.currentTarget.value;
            onParentChange?.({ ...(snapshots.current.get(next) ?? defaults), task: next });
            setQuery("");
            setPage(0);
          }}>
          {tasks.map(option => (
            <option
              key={option.tag}
              value={option.tag}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
      {taskError && (
        <p role="alert">
          {taskError}
          <button
            type="button"
            onClick={() => setTaskRetry(taskRetry + 1)}>
            Retry tasks
          </button>
        </p>
      )}
      <label>
        Search models
        <input
          type="search"
          disabled={disabled}
          value={query}
          onChange={event => {
            setQuery(event.currentTarget.value);
            setPage(0);
          }}
        />
      </label>
      {query && (
        <button
          type="button"
          disabled={disabled}
          onClick={() => setQuery("")}>
          Clear search
        </button>
      )}
      {loading && <p role="status">Loading models…</p>}
      {modelError && (
        <p role="alert">
          <span>{modelError}</span>
          <button
            type="button"
            onClick={() => setModelRetry(modelRetry + 1)}>
            Retry models
          </button>
        </p>
      )}
      {truncated && <p>Results may be incomplete. Use the search bar to find models not shown here.</p>}
      {value ? (
        <p>
          Selected: {String(value)}{" "}
          <button
            type="button"
            disabled={disabled}
            onClick={() => onChange("")}>
            Clear selected model
          </button>
        </p>
      ) : null}
      {!loading && !modelError && (
        <div className="hugging-face-models">
          {activeModels.slice(activePage * 50, (activePage + 1) * 50).map(model => (
            <button
              type="button"
              key={model.id}
              disabled={disabled}
              aria-pressed={value === model.id}
              onClick={() => onChange(model.id)}>
              {model.id}
              {model.downloads !== undefined && <small> · {model.downloads.toLocaleString()} downloads</small>}
              {model.likes !== undefined && <small> · {model.likes.toLocaleString()} likes</small>}
            </button>
          ))}
          {!activeModels.length && <p>{query ? `No models found for "${query}".` : "No models available."}</p>}
        </div>
      )}
      {pages > 1 && (
        <nav aria-label="Models pagination">
          <button
            type="button"
            disabled={activePage === 0}
            onClick={() => setPage(activePage - 1)}>
            Prev
          </button>
          <span>
            Page {activePage + 1} of {pages}
          </span>
          <button
            type="button"
            disabled={activePage >= pages - 1}
            onClick={() => setPage(activePage + 1)}>
            Next
          </button>
        </nav>
      )}
    </div>
  );
}

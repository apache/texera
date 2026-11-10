/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { v4 as uuid } from "uuid";
import { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import { createOperatorFactory } from "../workspace/operator-factory";
import { WorkflowRepository, emptyWorkflow, readFileText, type WorkflowContent } from "./workflow-repository";
import {
  NotebookMigrationLLM,
  LlmRequestTimeoutError,
  LlmResponseTruncatedError,
  type MigrationConfiguration,
  type MigrationLanguage,
  type MigrationOperatorFactory,
  type Notebook,
  type CombinedMapping,
  type SourceConversion,
} from "../../core/workspace/service/notebook-migration/migration-llm";
import {
  buildFolderDocument,
  checkFolderByteSize,
  checkFolderDocument,
  checkFolderFileCount,
  folderRelativePath,
  folderRootName,
  isExcludedPath,
  isMigratablePythonPath,
  type FolderDocument,
} from "../../core/workspace/service/notebook-migration/folder-assembly";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";

export type GenerationSelection =
  | { kind: "notebook"; notebook: Notebook; name: string }
  | { kind: "script"; source: string; name: string }
  | { kind: "folder"; folder: FolderDocument; name: string };
type MigrationEngine = Pick<
  NotebookMigrationLLM,
  | "initialize"
  | "verifyConnection"
  | "convertNotebookToWorkflow"
  | "convertScriptToWorkflow"
  | "convertFolderToWorkflow"
  | "close"
>;
interface GeneratorOptions {
  createLLM?: (config: MigrationConfiguration, factory: MigrationOperatorFactory) => MigrationEngine;
  operatorFactory?: MigrationOperatorFactory;
}
export interface GenerationResult {
  wid: number;
  warning?: string;
}
function refuse(message: string | null) {
  if (message !== null) throw new Error(message);
}
function fileBase(name: string): string {
  const index = name.lastIndexOf(".");
  return (index < 0 ? name : name.slice(0, index)).trim() || "Untitled workflow";
}

export async function parseGenerationFiles(files: readonly File[], folder = false): Promise<GenerationSelection> {
  if (!files.length) throw new Error("Choose a notebook, Python file or folder.");
  if (folder) {
    const picked = files.map(file => ({ file, path: folderRelativePath(file) }));
    const selected = picked.filter(item => isMigratablePythonPath(item.path));
    refuse(checkFolderFileCount(selected.length));
    refuse(checkFolderByteSize(selected.reduce((sum, item) => sum + item.file.size, 0)));
    const sources = await Promise.all(
      selected.map(async item => ({ path: item.path, source: await readFileText(item.file) }))
    );
    const name = folderRootName(files[0].webkitRelativePath) ?? "project";
    const document = buildFolderDocument(sources, {
      rootName: name,
      otherPaths: picked
        .filter(item => !isMigratablePythonPath(item.path) && !isExcludedPath(item.path))
        .map(item => item.path),
    });
    refuse(checkFolderDocument(document));
    return { kind: "folder", folder: document, name };
  }
  if (files.length !== 1) throw new Error("Choose one notebook or Python file, or use folder import.");
  const file = files[0],
    extension = file.name.split(".").pop()?.toLowerCase(),
    source = await readFileText(file);
  if (extension === "py") {
    if (!source.trim()) throw new Error("The Python file is empty.");
    return { kind: "script", source, name: fileBase(file.name) };
  }
  if (extension !== "ipynb") throw new Error("Choose a Jupyter notebook (.ipynb) or Python (.py) file.");
  let notebook: Notebook;
  try {
    notebook = JSON.parse(source);
  } catch {
    throw new Error("The notebook is not valid JSON.");
  }
  if (!notebook || !Array.isArray(notebook.cells))
    throw new Error("Invalid notebook structure: a cells array is required.");
  for (const cell of notebook.cells) {
    if (
      !cell ||
      typeof cell !== "object" ||
      !(
        typeof cell.source === "string" ||
        (Array.isArray(cell.source) && cell.source.every(line => typeof line === "string"))
      )
    )
      throw new Error("Invalid notebook cell source.");
    cell.metadata = { ...(cell.metadata ?? {}), uuid: uuid() };
  }
  return { kind: "notebook", notebook, name: fileBase(file.name) };
}

/** Reuses the existing conversion engine, segmentation and mapping semantics through pure interfaces. */
export class NotebookGenerator {
  constructor(
    private readonly api: ApiClient,
    private readonly workflows: WorkflowRepository,
    private readonly config: Readonly<GuiConfig>,
    private readonly options: GeneratorOptions = {}
  ) {}

  async models(signal?: AbortSignal): Promise<string[]> {
    if (!this.config.pythonNotebookMigrationEnabled) return [];
    const result = await this.api.get<{ data: { id: string }[] }>("models", { signal });
    return Array.isArray(result?.data) ? result.data.map(model => model.id).filter(Boolean) : [];
  }

  async generate(
    selection: GenerationSelection,
    model: string,
    language: MigrationLanguage = "python"
  ): Promise<GenerationResult> {
    if (!this.config.pythonNotebookMigrationEnabled) throw new Error("Notebook migration feature is disabled");
    if (selection.kind !== "folder" && language === "r" && !this.config.rNotebookMigrationEnabled) {
      throw new Error("R notebook migration is disabled");
    }
    if (!model.trim()) throw new Error("Choose a model for generation.");
    const settings = emptyWorkflow(this.config).settings;
    const runtime: MigrationConfiguration = {
      env: {
        pythonNotebookMigrationEnabled: true,
        rNotebookMigrationEnabled: this.config.rNotebookMigrationEnabled ?? false,
        pythonNotebookMigrationTimeoutMinutes: this.config.pythonNotebookMigrationTimeoutMinutes ?? 10,
        defaultDataTransferBatchSize: settings.dataTransferBatchSize,
        defaultExecutionMode: settings.executionMode,
      },
    };
    const factory =
      this.options.operatorFactory ??
      createOperatorFactory(await this.api.get<OperatorMetadata>("resources/operator-metadata"));
    const engine = this.options.createLLM?.(runtime, factory) ?? new NotebookMigrationLLM(runtime, factory);
    let content: WorkflowContent, notebook: Notebook, mapping: CombinedMapping;
    try {
      engine.initialize(model);
      if (!(await engine.verifyConnection())) throw new Error("Unable to authenticate with or reach the LLM backend");
      if (selection.kind === "notebook") {
        const converted = JSON.parse(await engine.convertNotebookToWorkflow(selection.notebook, language));
        content = converted.workflowJSON;
        mapping = converted.workflowNotebookMapping;
        notebook = selection.notebook;
      } else {
        const converted: SourceConversion =
          selection.kind === "script"
            ? await engine.convertScriptToWorkflow(selection.source, language)
            : await engine.convertFolderToWorkflow(selection.folder);
        content = converted.workflowJSON;
        mapping = converted.workflowNotebookMapping;
        notebook = converted.notebook;
      }
    } finally {
      engine.close();
    }
    const suffix = "_GENERATED_BY_LLM";
    const created = await this.workflows.create(content, selection.name.slice(0, 128 - suffix.length) + suffix);
    const wid = created.workflow.wid;
    if (!wid) throw new Error("The generated workflow was not assigned an ID.");
    try {
      const result = await this.api.post<{ success?: boolean }>("notebook-migration/store-notebook-and-mapping", {
        wid,
        mapping,
        notebook,
      });
      if (result?.success === false) throw new Error("Notebook attachment was refused.");
      return { wid };
    } catch {
      return {
        wid,
        warning: "Workflow created, but the notebook could not be attached; the Jupyter panel may not open.",
      };
    }
  }
}

export function NotebookGenerationDialog({
  api,
  config,
  onCreated,
  onClose,
  onWarning,
}: {
  api: ApiClient;
  config: Readonly<GuiConfig>;
  onCreated(wid: number): void;
  onClose(): void;
  onWarning?(message: string): void;
}) {
  const generator = useMemo(() => new NotebookGenerator(api, new WorkflowRepository(api), config), [api, config]);
  const [models, setModels] = useState<string[]>([]);
  const [model, setModel] = useState("");
  const [folder, setFolder] = useState(false);
  const [files, setFiles] = useState<File[]>([]);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    const abort = new AbortController();
    void generator
      .models(abort.signal)
      .then(values => {
        if (!abort.signal.aborted) {
          setModels(values);
          setModel(values[0] ?? "");
        }
      })
      .catch(reason => {
        if (!abort.signal.aborted) setError(reason instanceof Error ? reason.message : "Models could not be loaded.");
      });
    return () => {
      mounted.current = false;
      abort.abort();
    };
  }, [generator]);
  async function generate() {
    setPending(true);
    setError(undefined);
    try {
      const selection = await parseGenerationFiles(files, folder);
      const result = await generator.generate(selection, model);
      if (mounted.current) {
        if (result.warning) onWarning?.(result.warning);
        onCreated(result.wid);
      }
    } catch (reason) {
      if (!mounted.current) return;
      setError(
        reason instanceof LlmRequestTimeoutError
          ? `Generation timed out after ${reason.minutes} minutes. Choose a faster model or simplify the input.`
          : reason instanceof LlmResponseTruncatedError
            ? `${reason.message} This input may be too large to convert.`
            : reason instanceof Error
              ? reason.message
              : "Workflow generation failed."
      );
    } finally {
      if (mounted.current) setPending(false);
    }
  }
  return (
    <div className="workflow-modal-backdrop">
      <section
        className="workflow-modal"
        role="dialog"
        aria-modal="true"
        aria-label="Generate a workflow from Python">
        <h2>Generate a workflow from Python</h2>
        <p>
          Convert a notebook, Python script or project folder into a visual workflow and retain its notebook mapping.
        </p>
        {error && <p role="alert">{error}</p>}
        {pending && <p role="status">Generating and saving your workflow. This can take several minutes…</p>}
        <label>
          Source type
          <select
            value={folder ? "folder" : "file"}
            disabled={pending}
            onChange={event => {
              setFolder(event.target.value === "folder");
              setFiles([]);
            }}>
            <option value="file">Notebook or Python file</option>
            <option value="folder">Python project folder</option>
          </select>
        </label>
        <label>
          {folder ? "Python folder" : "Notebook or Python file"}
          <input
            key={String(folder)}
            type="file"
            disabled={pending}
            accept={folder ? undefined : ".ipynb,.py"}
            multiple={folder}
            ref={input => {
              if (input && folder) input.setAttribute("webkitdirectory", "");
            }}
            onChange={event => setFiles(Array.from(event.target.files ?? []))}
          />
        </label>
        {files.length > 0 && <p>{folder ? `${files.length} selected files` : files[0].name}</p>}
        <label>
          Generation model
          <select
            value={model}
            disabled={pending || !models.length}
            onChange={event => setModel(event.target.value)}>
            {!models.length && <option value="">No models available</option>}
            {models.map(name => (
              <option
                key={name}
                value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <footer>
          <button
            type="button"
            disabled={pending}
            onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="primary"
            disabled={pending || !model || !files.length}
            onClick={() => void generate()}>
            Generate workflow
          </button>
        </footer>
      </section>
    </div>
  );
}

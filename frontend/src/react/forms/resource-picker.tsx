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
import { useOperatorFormContext } from "./operator-context";
import {
  resourcePathLabel,
  resourcePathParts,
  resourceVersionPath,
  type ResourceCatalog,
  type ResourceFileNode,
  type ResourceKind,
  type ResourceSummary,
  type ResourceVersion,
} from "./resource-catalog";
import type { PropertyWidgetProps } from "./schema-form";

interface ResourcePickerProps {
  catalog: ResourceCatalog;
  kind: ResourceKind;
  fileMode?: boolean;
  value: string;
  onSelect: (path: string) => void;
  onCancel: () => void;
}

/** Resource and version selection use separate cancellable lifetimes, so old responses cannot win. */
export function ResourcePicker({ catalog, kind, fileMode = false, value, onSelect, onCancel }: ResourcePickerProps) {
  const [resources, setResources] = useState<ResourceSummary[]>([]);
  const [versions, setVersions] = useState<ResourceVersion[]>([]);
  const [files, setFiles] = useState<ResourceFileNode[]>([]);
  const [resourceId, setResourceId] = useState<number>();
  const [versionId, setVersionId] = useState<number>();
  const [selectedFile, setSelectedFile] = useState<string>();
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>();
  const [retry, setRetry] = useState(0);
  const container = useRef<HTMLDivElement>(null);
  const saved = useRef(resourcePathParts(value));
  const titleId = useId();
  const resourceLabel = kind === "model" ? "Model" : "Dataset";
  const selectedResource = resources.find(resource => resource.id === resourceId);
  const selectedVersion = versions.find(version => version.id === versionId);
  const candidate =
    !fileMode && selectedResource && selectedVersion
      ? resourceVersionPath(kind, selectedResource, selectedVersion)
      : selectedFile;

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    container.current?.focus();
    return () => {
      previous?.focus();
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    setLoading(true);
    setError(undefined);
    setResources([]);
    setResourceId(undefined);
    void catalog
      .list(kind, controller.signal)
      .then(rows => {
        if (!active) return;
        setResources(rows);
        const selected = rows.find(
          resource => resource.ownerEmail === saved.current.owner && resource.name === saved.current.name
        );
        setResourceId(selected?.id);
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Resources could not be loaded.");
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [catalog, kind, retry]);

  useEffect(() => {
    setVersions([]);
    setVersionId(undefined);
    setFiles([]);
    setSelectedFile(undefined);
    if (resourceId === undefined) return;
    const controller = new AbortController();
    let active = true;
    setError(undefined);
    void catalog
      .versions(kind, resourceId, controller.signal)
      .then(rows => {
        if (!active) return;
        setVersions(rows);
        const resource = resources.find(row => row.id === resourceId);
        const savedVersion =
          resource?.ownerEmail === saved.current.owner && resource?.name === saved.current.name
            ? rows.find(version => version.name === saved.current.version)
            : undefined;
        setVersionId((savedVersion ?? (fileMode ? rows[0] : undefined))?.id);
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Versions could not be loaded.");
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [catalog, kind, resourceId, resources, fileMode]);

  useEffect(() => {
    setFiles([]);
    setSelectedFile(undefined);
    if (resourceId === undefined || versionId === undefined) return;
    const controller = new AbortController();
    let active = true;
    setError(undefined);
    void catalog
      .files(kind, resourceId, versionId, controller.signal)
      .then(rows => {
        if (active) setFiles(rows);
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Files could not be loaded.");
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [catalog, kind, resourceId, versionId]);

  const search = query.trim().toLowerCase();
  const visibleResources = resources.filter(
    resource => resource.name.toLowerCase().includes(search) || String(resource.id).includes(search.replace(/^#/, ""))
  );
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby={titleId}
      tabIndex={-1}
      ref={container}
      className="resource-picker"
      onKeyDown={event => {
        if (event.key === "Escape") onCancel();
        if (event.key === "Tab") {
          const controls = Array.from(
            event.currentTarget.querySelectorAll<HTMLElement>(
              "button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex='0']"
            )
          );
          const first = controls[0];
          const last = controls.at(-1);
          if (event.shiftKey && (document.activeElement === first || document.activeElement === event.currentTarget)) {
            event.preventDefault();
            last?.focus();
          } else if (
            !event.shiftKey &&
            (document.activeElement === last || document.activeElement === event.currentTarget)
          ) {
            event.preventDefault();
            first?.focus();
          }
        }
      }}>
      <h2 id={titleId}>
        Select a {kind} {fileMode ? "file" : "version"}
      </h2>
      {error && (
        <p role="alert">
          {error}{" "}
          <button
            type="button"
            onClick={() => setRetry(retry + 1)}>
            Retry
          </button>
        </p>
      )}
      {loading && <p role="status">Loading {kind}s…</p>}
      {!loading && !error && !resources.length && <p>No accessible {kind}s.</p>}
      <label>
        Search {kind}s
        <input
          type="search"
          value={query}
          onChange={event => setQuery(event.currentTarget.value)}
        />
      </label>
      <label>
        {resourceLabel}
        <select
          value={resourceId ?? ""}
          onChange={event => {
            setResourceId(event.currentTarget.value === "" ? undefined : Number(event.currentTarget.value));
          }}>
          <option value="">Select {kind}</option>
          {visibleResources.map(resource => (
            <option
              key={resource.id}
              value={resource.id}>
              #{resource.id} {resource.name} · {resource.isOwner ? "OWNER" : resource.accessPrivilege ?? "READ"}
            </option>
          ))}
        </select>
      </label>
      <label>
        Version
        <select
          value={versionId ?? ""}
          disabled={resourceId === undefined}
          onChange={event => {
            setVersionId(event.currentTarget.value === "" ? undefined : Number(event.currentTarget.value));
          }}>
          <option value="">Select version</option>
          {versions.map(version => (
            <option
              key={version.id}
              value={version.id}>
              {version.name}
            </option>
          ))}
        </select>
      </label>
      {files.length > 0 && (
        <ul
          aria-label="Files"
          className="resource-file-tree">
          <ResourceFiles
            nodes={files}
            selectable={fileMode}
            selected={selectedFile}
            onSelect={setSelectedFile}
          />
        </ul>
      )}
      {candidate && <p className="resource-selected-path">{candidate}</p>}
      <footer>
        <button
          type="button"
          onClick={onCancel}>
          Cancel
        </button>
        <button
          type="button"
          disabled={!candidate}
          onClick={() => {
            if (candidate) onSelect(candidate);
          }}>
          Select
        </button>
      </footer>
    </div>
  );
}

function ResourceFiles({
  nodes,
  selectable,
  selected,
  onSelect,
}: {
  nodes: readonly ResourceFileNode[];
  selectable: boolean;
  selected?: string;
  onSelect: (path: string) => void;
}) {
  return (
    <>
      {nodes.map(node => {
        const path = `${node.parentDir}/${node.name}`;
        return (
          <li key={path}>
            {selectable ? (
              <button
                type="button"
                aria-pressed={selected === path}
                onClick={() => onSelect(path)}>
                {node.name}
              </button>
            ) : (
              <span>{node.name}</span>
            )}
            {node.children?.length ? (
              <ul>
                <ResourceFiles
                  nodes={node.children}
                  selectable={selectable}
                  selected={selected}
                  onSelect={onSelect}
                />
              </ul>
            ) : null}
          </li>
        );
      })}
    </>
  );
}

export function ResourcePathField({
  schema,
  value,
  disabled,
  onChange,
  mode = "version",
  resource = "dataset",
  compact = false,
}: PropertyWidgetProps & {
  mode?: "file" | "version";
  resource?: ResourceKind;
  compact?: boolean;
}) {
  const context = useOperatorFormContext();
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string>();
  const id = useId();
  const path = typeof value === "string" ? value : "";
  const label = schema.title ?? (mode === "file" ? "File" : "Dataset version");
  const canBrowse = mode !== "file" || context.selectingFilesFromDatasetsEnabled === true;
  const browse = () => {
    if (disabled) return;
    if (!context.resourceCatalog) {
      setError("Resource selection is unavailable.");
      return;
    }
    setError(undefined);
    setOpen(true);
  };
  return (
    <div className="resource-path-field">
      {compact ? (
        <button
          type="button"
          disabled={disabled}
          title={path || `Select a ${resource} version`}
          onClick={browse}>
          {resourcePathLabel(path, resource)}
        </button>
      ) : (
        <>
          <label htmlFor={id}>{label}</label>
          <input
            id={id}
            value={path}
            disabled={disabled}
            onChange={event => onChange(event.currentTarget.value)}
          />
          {canBrowse && (
            <button
              type="button"
              disabled={disabled}
              aria-label={`Browse ${label}`}
              onClick={browse}>
              Browse
            </button>
          )}
        </>
      )}
      {error && <p role="alert">{error}</p>}
      {open && context.resourceCatalog && (
        <ResourcePicker
          catalog={context.resourceCatalog}
          kind={resource}
          fileMode={mode === "file"}
          value={path}
          onCancel={() => setOpen(false)}
          onSelect={selected => {
            if (!disabled) onChange(selected);
            setOpen(false);
          }}
        />
      )}
    </div>
  );
}

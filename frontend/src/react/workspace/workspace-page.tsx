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
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { v4 as uuid } from "uuid";
import { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import type { GuiConfig } from "../core/runtime-config";
import { Version } from "../../environments/version";
import { ExecutionMode } from "../../core/common/type/workflow";
import type { OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
import { ExecutionState } from "../../core/workspace/types/execute-workflow.interface";
import { WorkflowRepository, saveDownload } from "../workflows/workflow-repository";
import { WorkflowShareDialog } from "../workflows/workflow-list-page";
import { ResultsPanel } from "../results/results-panel";
import { WorkspaceController, type WorkspaceSnapshot } from "./workspace-controller";
import { WorkflowCanvas, type CanvasActions } from "./workflow-canvas";
import { CanvasModel } from "./canvas-model";
import { createOperator } from "./operator-factory";
import { OperatorProperties } from "./operator-properties";
import { OperatorControls } from "./operator-controls";
import { FormView } from "./form-view";
import { WorkspaceAssistant } from "../agents/workspace-assistant";
import { OperatorPreview, HeatmapLegend } from "./operator-preview";
import { WorkspaceTimeTravel } from "../results/workspace-time-travel";
import { VersionRestoreDialog } from "../workflows/version-restore";
import { VersionComparison, type VersionHighlights } from "../workflows/version-comparison";
import { extractPerformanceMetrics } from "../../core/workspace/service/workflow-status/performance-metrics";
import { rawMetricForView, formatMetricForView } from "../../core/workspace/service/heatmap/heatmap-scoring";
import { copySelection, pasteSelection } from "./workflow-clipboard";
import { validateWorkflowForRun } from "./workflow-validation";
import { HeatmapView, heatmapViewTitle } from "../../core/workspace/service/heatmap/heatmap-scoring";
import {
  loadPersistedHeatmapView,
  savePersistedHeatmapView,
} from "../../core/workspace/service/heatmap/heatmap-overlay-persistence";
import "./workspace.css";

export interface WorkspacePageProps {
  api: ApiClient;
  user: SessionUser;
  config: Readonly<GuiConfig>;
  getToken: () => string | null;
  workflowId: number;
  versionId?: number;
  autoLayout?: boolean;
  onAutoLayoutApplied?: () => void;
  view: "canvas" | "form";
  onView(view: "canvas" | "form"): void;
  onBack(): void;
  onVersion?(versionId: number): void;
  onOpenWorkflow?(workflowId: number): void;
}
export function WorkspacePage(props: WorkspacePageProps) {
  const [controller, setController] = useState<WorkspaceController>();
  const [error, setError] = useState<string>();
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    const next = new WorkspaceController(props.api, props.user, props.config, {
      workflowId: props.workflowId,
      versionId: props.versionId,
      getToken: props.getToken,
      engineVersion: Version.buildNumber,
    });
    setController(next);
    setError(undefined);
    void next.load().catch(reason => setError(message(reason)));
    return () => {
      void next.dispose().catch(reason => console.error("Workflow save failed during close:", reason));
    };
  }, [props.api, props.user, props.config, props.getToken, props.workflowId, props.versionId, retry]);
  if (error)
    return (
      <section>
        <p role="alert">{error}</p>
        <button onClick={() => setRetry(retry + 1)}>Retry</button>
      </section>
    );
  if (!controller) return <p role="status">Loading workflow…</p>;
  return (
    <ConnectedWorkspace
      {...props}
      controller={controller}
    />
  );
}
function ConnectedWorkspace(props: WorkspacePageProps & { controller: WorkspaceController }) {
  const snapshot = useSyncExternalStore(props.controller.subscribe, props.controller.getSnapshot);
  if (snapshot.loading) return <p role="status">Loading workflow…</p>;
  if (!snapshot.session || !snapshot.metadata)
    return <p role="alert">{snapshot.error ?? "The workflow could not be loaded."}</p>;
  return (
    <WorkspaceEditor
      {...props}
      snapshot={snapshot}
    />
  );
}
export interface WorkspaceEditorProps extends Omit<WorkspacePageProps, "workflowId"> {
  controller: WorkspaceController;
  snapshot: WorkspaceSnapshot;
}
const noopSubscribe = () => () => undefined;
const noRuntime = () => undefined;
export function WorkspaceEditor(props: WorkspaceEditorProps) {
  const { api, user, config, getToken, controller, snapshot, view, onView, onBack } = props;
  const session = snapshot.session!;
  const metadata = snapshot.metadata!;
  const runtime = snapshot.runtime;
  const state = useSyncExternalStore(session.subscribe, session.getSnapshot);
  const execution = useSyncExternalStore(runtime?.subscribe ?? noopSubscribe, runtime?.getSnapshot ?? noRuntime);
  const { workflow, canEdit } = state;
  const [model, setModel] = useState<CanvasModel>();
  const [selected, setSelected] = useState<string[]>([]);
  const [filter, setFilter] = useState("");
  const [error, setError] = useState<string>();
  const [name, setName] = useState(workflow.name);
  const [dialog, setDialog] = useState<
    "settings" | "share" | "versions" | "comment" | "time-travel" | "restore-version"
  >();
  const [historyExecution, setHistoryExecution] = useState<number>();
  const [versionHighlights, setVersionHighlights] = useState<VersionHighlights>();
  const [comment, setComment] = useState("");
  const [emailNotification, setEmailNotification] = useState(false);
  const [showResults, setShowResults] = useState(true);
  const [contextMenu, setContextMenu] = useState<{ id: string; x: number; y: number }>();
  const [commentBoxId, setCommentBoxId] = useState<string>();
  const [heatmap, setHeatmap] = useState<HeatmapView | null>(loadPersistedHeatmapView);
  const [regions, setRegions] = useState(false);
  const [resultPreview, setResultPreview] = useState(true);
  const [hover, setHover] = useState<{ id: string; x: number; y: number }>();
  const [pinned, setPinned] = useState<string[]>([]);
  const hoverTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(
    () => () => {
      if (hoverTimer.current) clearTimeout(hoverTimer.current);
    },
    []
  );
  const actions = useRef<CanvasActions | undefined>(undefined);
  const laidOutDocument = useRef<string | undefined>(undefined);
  const repository = useMemo(() => new WorkflowRepository(api), [api]);
  const fail = useCallback((reason: unknown) => setError(message(reason)), []);
  const act = useCallback(
    (operation: () => unknown) => {
      try {
        setError(undefined);
        void Promise.resolve(operation()).catch(fail);
      } catch (reason) {
        fail(reason);
      }
    },
    [fail]
  );
  useEffect(() => {
    setName(workflow.name);
  }, [workflow.name]);
  useEffect(() => {
    void runtime?.compile().catch(() => undefined);
  }, [runtime]);
  useEffect(() => {
    const next = new CanvasModel(session, metadata, fail);
    setModel(next);
    return () => {
      next.dispose();
    };
  }, [session, metadata, fail, state.documentId]);
  useEffect(() => {
    const beforeUnload = (event: BeforeUnloadEvent) => {
      if (["unsaved", "error", "saving"].includes(session.getSnapshot().saveState)) event.preventDefault();
    };
    window.addEventListener("beforeunload", beforeUnload);
    return () => window.removeEventListener("beforeunload", beforeUnload);
  }, [session]);
  const removeSelection = useCallback(() => {
    selected.filter(id => session.graph.hasLinkWithID(id)).forEach(id => session.deleteLink(id));
    selected.filter(id => session.graph.hasCommentBox(id)).forEach(id => session.deleteCommentBox(id));
    session.deleteOperators(selected.filter(id => session.graph.hasOperator(id)));
    setSelected([]);
  }, [selected, session]);
  const copy = useCallback(async () => {
    if (!navigator.clipboard) throw new Error("Copy is unavailable in this browser.");
    await navigator.clipboard.writeText(copySelection(session, selected));
  }, [session, selected]);
  const paste = useCallback(async () => {
    if (!navigator.clipboard) throw new Error("Paste is unavailable in this browser.");
    setSelected(pasteSelection(session, await navigator.clipboard.readText()));
  }, [session]);
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      if (target.closest("input,textarea,select,[contenteditable=true],[role=dialog],.monaco-editor")) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
        event.preventDefault();
        act(() => session.save());
      }
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "c") {
        event.preventDefault();
        act(copy);
      }
      if (!canEdit) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "v") {
        event.preventDefault();
        act(paste);
      }
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "x") {
        event.preventDefault();
        act(async () => {
          await copy();
          removeSelection();
        });
      }
      if (event.key === "Delete" || event.key === "Backspace") {
        event.preventDefault();
        act(removeSelection);
      }
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "z") {
        event.preventDefault();
        act(() => (event.shiftKey ? session.redo() : session.undo()));
      }
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "a") {
        event.preventDefault();
        setSelected([
          ...session.graph.getAllOperators().map(operator => operator.operatorID),
          ...session.graph.getAllLinks().map(link => link.linkID),
          ...session.graph.getAllCommentBoxes().map(box => box.commentBoxID),
        ]);
      }
    };
    window.addEventListener("keydown", key);
    return () => window.removeEventListener("keydown", key);
  }, [session, canEdit, selected, removeSelection, act, copy, paste]);
  const select = useCallback(
    (id: string | undefined, append: boolean) =>
      setSelected(previous =>
        !id ? [] : append ? (previous.includes(id) ? previous.filter(value => value !== id) : [...previous, id]) : [id]
      ),
    []
  );
  const add = (schema: OperatorSchema, position?: { x: number; y: number }) =>
    act(() => {
      const operator = createOperator(schema);
      const count = session.graph.getAllOperators().length;
      session.addOperator(operator, position ?? { x: 100 + (count % 4) * 180, y: 80 + Math.floor(count / 4) * 130 });
      setSelected([operator.operatorID]);
    });
  const commitName = () => {
    if (name !== workflow.name) act(() => session.rename(name));
  };
  const active =
    execution &&
    ![
      ExecutionState.Uninitialized,
      ExecutionState.Completed,
      ExecutionState.Failed,
      ExecutionState.Killed,
      ExecutionState.Terminated,
    ].includes(execution.state);
  const unit = snapshot.units.find(value => value.computingUnit.cuid === snapshot.selectedCuid);
  const writableUnit = unit && (unit.isOwner || unit.accessPrivilege === "WRITE");
  const validation = useMemo(() => {
    if (!workflow.content.operators.length) return { isValid: false, workflowEmpty: true, errors: {} };
    return validateWorkflowForRun({
      graph: session.graph,
      metadata,
      outputSchemas: execution?.compilation?.operatorOutputSchemas,
    });
  }, [session, metadata, workflow.content, execution?.compilation?.operatorOutputSchemas]);
  const compilationErrors = execution?.compilation?.operatorErrors ?? {};
  const runDisabled =
    !runtime ||
    workflow.readonly ||
    state.autosaveSuspended ||
    !!active ||
    !writableUnit ||
    execution?.connection.status !== "connected" ||
    !validation.isValid ||
    Object.keys(compilationErrors).length > 0;
  const selectedOperatorId = selected.find(id => session.graph.hasOperator(id));
  const results = (operatorIds?: string[]) =>
    runtime ? (
      <ResultsPanel
        runtime={runtime}
        session={session}
        config={config}
        api={api}
        operatorIds={operatorIds}
        selectedOperatorId={selectedOperatorId}
        onSelectOperator={id => setSelected([id])}
        exportContext={
          unit
            ? { computingUnitId: unit.computingUnit.cuid, computingUnitType: unit.computingUnit.type, getToken }
            : undefined
        }
        onOpenExecution={entry => {
          if (config.timetravelEnabled && !session.isAutosaveSuspended("agent")) {
            setHistoryExecution(entry.eId);
            setDialog("time-travel");
          } else props.onVersion?.(entry.vId);
        }}
      />
    ) : (
      <p>Select a computing unit to connect to workflow execution.</p>
    );
  return (
    <section
      className="react-workspace"
      aria-label="Workflow workspace">
      <header className="react-workspace-toolbar">
        <button
          onClick={() =>
            act(async () => {
              if (!workflow.readonly) await session.save();
              onBack();
            })
          }>
          Workflows
        </button>
        <input
          aria-label="Workflow name"
          value={name}
          disabled={!canEdit}
          onChange={event => setName(event.target.value)}
          onBlur={commitName}
          onKeyDown={event => {
            if (event.key === "Enter") event.currentTarget.blur();
          }}
        />
        <span role="status">
          {workflow.readonly ? "Read only" : state.saveState === "saved" ? "All changes saved" : state.saveState}
        </span>
        <button
          disabled={workflow.readonly || state.saveState === "saving" || state.autosaveSuspended}
          onClick={() => act(() => session.save())}>
          Save
        </button>
        <button
          disabled={!canEdit || !state.canUndo}
          onClick={() => act(() => session.undo())}>
          Undo
        </button>
        <button
          disabled={!canEdit || !state.canRedo}
          onClick={() => act(() => session.redo())}>
          Redo
        </button>
        <button onClick={() => setDialog("settings")}>Settings</button>
        <button onClick={() => setDialog("share")}>Share</button>
        <button onClick={() => setDialog("versions")}>Versions</button>
        {props.versionId !== undefined && (
          <button
            disabled={!!active}
            onClick={() => setDialog("restore-version")}>
            Restore this version
          </button>
        )}
        {config.timetravelEnabled && (
          <button
            disabled={!runtime || session.isAutosaveSuspended("agent")}
            onClick={() => {
              setHistoryExecution(undefined);
              setDialog("time-travel");
            }}>
            Time travel
          </button>
        )}
        <button
          onClick={() =>
            act(() =>
              saveDownload(
                JSON.stringify({ ...workflow.content, defaultView: workflow.defaultView }, null, 2),
                `${workflow.name}.json`
              )
            )
          }>
          Export workflow
        </button>
        <button
          onClick={() =>
            act(async () => {
              const blob = await actions.current?.capturePng();
              if (blob) saveDownload(blob, `${workflow.name}.png`, "image/png");
            })
          }
          disabled={view !== "canvas"}>
          Export image
        </button>
        {config.formViewEnabled && (
          <button onClick={() => onView(view === "canvas" ? "form" : "canvas")}>
            {view === "canvas" ? "Form view" : "Canvas view"}
          </button>
        )}
      </header>
      <div
        className="react-workspace-toolbar"
        aria-label="Execution controls">
        <label>
          Computing unit{" "}
          <select
            aria-label="Computing unit"
            value={snapshot.selectedCuid ?? ""}
            onChange={event =>
              act(() => controller.selectComputingUnit(event.target.value ? Number(event.target.value) : undefined))
            }>
            <option value="">Select computing unit</option>
            {snapshot.units.map(value => (
              <option
                key={value.computingUnit.cuid}
                value={value.computingUnit.cuid}>
                {value.computingUnit.name} ({value.status})
              </option>
            ))}
          </select>
        </label>
        {config.warehouseEnabled && (
          <label>
            Warehouse{" "}
            <select
              aria-label="Warehouse"
              value={snapshot.selectedWarehouseId ?? ""}
              onChange={event =>
                act(() => controller.selectWarehouse(event.target.value ? Number(event.target.value) : undefined))
              }>
              <option value="">Select warehouse</option>
              {snapshot.warehouses.map(value => (
                <option
                  key={value.whid}
                  value={value.whid}>
                  {value.name}
                </option>
              ))}
            </select>
          </label>
        )}
        <button
          disabled={runDisabled}
          onClick={() => act(() => runtime!.run({ emailNotificationEnabled: emailNotification }))}>
          Run
        </button>
        <button
          disabled={!runtime || !writableUnit || execution?.state !== ExecutionState.Running}
          onClick={() => act(() => runtime!.pause())}>
          Pause
        </button>
        <button
          disabled={!runtime || !writableUnit || execution?.state !== ExecutionState.Paused}
          onClick={() => act(() => runtime!.resume())}>
          Resume
        </button>
        <button
          disabled={!runtime || !writableUnit || !active}
          onClick={() => act(() => runtime!.kill())}>
          Kill
        </button>
        {config.timetravelEnabled && (
          <button
            disabled={!runtime || !writableUnit || !active}
            onClick={() => act(() => runtime!.checkpoint())}>
            Take checkpoint
          </button>
        )}
        {config.workflowEmailNotificationEnabled && (
          <label>
            <input
              type="checkbox"
              checked={emailNotification}
              onChange={event => setEmailNotification(event.target.checked)}
            />
            Email when complete
          </label>
        )}
        <span>
          {execution?.state ?? "Disconnected"}
          {execution ? ` · ${(execution.duration / 1000).toFixed(1)} s · ${execution.connection.status}` : ""}
        </span>
        <button onClick={() => setShowResults(!showResults)}>{showResults ? "Hide results" : "Show results"}</button>
      </div>
      {(error || state.error || execution?.error || snapshot.error) && (
        <div role="alert">
          {error || state.error || execution?.error || snapshot.error}
          <button
            aria-label="Dismiss error"
            onClick={() => setError(undefined)}>
            ×
          </button>
        </div>
      )}
      {view === "form" ? (
        <FormView
          key={state.documentId}
          session={session}
          metadata={metadata}
          propertyServices={{ api, user, config, getToken, runtime }}
          onCanvas={() => onView("canvas")}
          renderResults={results}
          readOnly={!canEdit}
        />
      ) : (
        <>
          <div
            className="react-workspace-toolbar"
            aria-label="Canvas controls">
            <button
              onClick={() => actions.current?.zoom(0.1)}
              aria-label="Zoom in">
              +
            </button>
            <button
              onClick={() => actions.current?.zoom(-0.1)}
              aria-label="Zoom out">
              −
            </button>
            <button onClick={() => actions.current?.reset()}>Reset zoom</button>
            <button onClick={() => actions.current?.fit()}>Fit</button>
            <button
              disabled={!canEdit}
              onClick={() => act(() => actions.current?.autoLayout())}>
              Auto layout
            </button>
            <button
              disabled={!canEdit || !selected.length}
              onClick={() => act(removeSelection)}>
              Delete selection
            </button>
            <button
              disabled={!canEdit}
              onClick={() => setDialog("comment")}>
              Add comment
            </button>
            <button
              disabled={!selected.length}
              onClick={() => act(copy)}>
              Copy
            </button>
            <button
              disabled={!canEdit || !selected.length}
              onClick={() =>
                act(async () => {
                  await copy();
                  removeSelection();
                })
              }>
              Cut
            </button>
            <button
              disabled={!canEdit}
              onClick={() => act(paste)}>
              Paste
            </button>
            <label>
              Heat map
              <select
                aria-label="Heat map"
                value={heatmap ?? ""}
                onChange={event => {
                  const value = event.target.value as HeatmapView;
                  setHeatmap(value || null);
                  savePersistedHeatmapView(value || null);
                }}>
                <option value="">Off</option>
                {Object.values(HeatmapView).map(value => (
                  <option
                    key={value}
                    value={value}>
                    {heatmapViewTitle(value)}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <input
                type="checkbox"
                checked={regions}
                onChange={event => setRegions(event.target.checked)}
              />
              Regions
            </label>
            <label>
              <input
                type="checkbox"
                checked={resultPreview}
                onChange={event => {
                  setResultPreview(event.target.checked);
                  setHover(undefined);
                }}
              />
              Result preview
            </label>
            <button
              disabled={!selectedOperatorId || !runtime}
              onClick={() => setPinned(previous => [...new Set([...previous, selectedOperatorId!])])}>
              Pin result window
            </button>
          </div>
          <div className="react-workspace-main">
            <aside
              className="react-operator-palette"
              aria-label="Operators">
              <input
                type="search"
                aria-label="Search operators"
                placeholder="Search operators"
                value={filter}
                onChange={event => setFilter(event.target.value)}
              />
              {[...new Set(metadata.operators.map(schema => schema.additionalMetadata.operatorGroupName))].map(
                group => (
                  <section key={group}>
                    <h3>{group}</h3>
                    {metadata.operators
                      .filter(
                        schema =>
                          schema.additionalMetadata.operatorGroupName === group &&
                          `${schema.additionalMetadata.userFriendlyName} ${schema.operatorType}`
                            .toLowerCase()
                            .includes(filter.toLowerCase())
                      )
                      .map(schema => (
                        <button
                          key={schema.operatorType}
                          disabled={!canEdit}
                          draggable={canEdit}
                          onDragStart={event =>
                            event.dataTransfer.setData("application/x-texera-operator", schema.operatorType)
                          }
                          title={schema.additionalMetadata.operatorDescription}
                          aria-label={`Add ${schema.additionalMetadata.userFriendlyName}`}
                          onClick={() => add(schema)}>
                          {schema.additionalMetadata.userFriendlyName}
                        </button>
                      ))}
                  </section>
                )
              )}
            </aside>
            {model && model.documentId === state.documentId ? (
              <WorkflowCanvas
                key={state.documentId}
                model={model}
                session={session}
                metadata={metadata}
                runtimeSnapshot={execution}
                invalidOperatorIds={[...Object.keys(validation.errors), ...Object.keys(compilationErrors)]}
                versionHighlights={props.versionId !== undefined ? versionHighlights : undefined}
                heatmap={heatmap}
                regions={regions}
                selected={selected}
                onSelect={select}
                onEdit={id => {
                  setSelected([id]);
                  if (session.graph.hasCommentBox(id)) setCommentBoxId(id);
                }}
                onReady={value => {
                  actions.current = value;
                  if (value && props.autoLayout && canEdit && laidOutDocument.current !== state.documentId) {
                    laidOutDocument.current = state.documentId;
                    act(() => {
                      value.autoLayout();
                      props.onAutoLayoutApplied?.();
                    });
                  }
                }}
                onError={fail}
                onDropOperator={(type, position) => {
                  const schema = metadata.operators.find(schema => schema.operatorType === type);
                  if (schema) add(schema, position);
                }}
                onHover={(id, anchor) => {
                  if (hoverTimer.current) clearTimeout(hoverTimer.current);
                  if (!id || !anchor) {
                    hoverTimer.current = setTimeout(() => setHover(undefined), 150);
                    return;
                  }
                  hoverTimer.current = setTimeout(() => setHover({ id, ...anchor }), 350);
                }}
                onContextMenu={(id, x, y) => {
                  setSelected(previous => (previous.includes(id) ? previous : [id]));
                  setContextMenu({ id, x, y });
                }}
              />
            ) : (
              <p>Loading canvas…</p>
            )}
            <aside
              className="react-property-panel"
              aria-label="Operator properties">
              {props.versionId !== undefined && (
                <VersionComparison
                  api={api}
                  wid={workflow.wid!}
                  versionId={props.versionId}
                  session={session}
                  onHighlight={setVersionHighlights}
                  onSelectOperator={id => setSelected([id])}
                />
              )}
              <OperatorProperties
                key={state.documentId}
                api={api}
                user={user}
                config={config}
                getToken={getToken}
                session={session}
                runtime={runtime}
                metadata={metadata}
                operatorId={selectedOperatorId ?? ""}
              />
              {selectedOperatorId ? (
                <>
                  <OperatorControls
                    key={state.documentId}
                    session={session}
                    metadata={metadata}
                    operatorId={selectedOperatorId}
                  />
                  <label>
                    <input
                      type="checkbox"
                      disabled={!canEdit}
                      checked={!!session.graph.getOperator(selectedOperatorId).isDisabled}
                      onChange={event => act(() => session.setDisabled(selectedOperatorId, event.target.checked))}
                    />
                    Disabled
                  </label>
                  <label>
                    <input
                      type="checkbox"
                      disabled={!canEdit}
                      checked={!!session.graph.getOperator(selectedOperatorId).viewResult}
                      onChange={event => act(() => session.setViewResult(selectedOperatorId, event.target.checked))}
                    />
                    View result
                  </label>
                  <button
                    disabled={runDisabled}
                    onClick={() => act(() => runtime!.run({ targetOperatorId: selectedOperatorId }))}>
                    Run until this operator
                  </button>
                </>
              ) : (
                <p>Select an operator to edit its properties.</p>
              )}
            </aside>
          </div>
          {showResults && <div className="react-workspace-results">{results()}</div>}
        </>
      )}
      {runtime && dialog !== "time-travel" && (config.copilotEnabled || config.pythonNotebookMigrationEnabled) && (
        <WorkspaceAssistant
          api={api}
          user={user}
          config={config}
          session={session}
          runtime={runtime}
          getToken={getToken}
          getComputingUnitId={() => snapshot.selectedCuid}
          getWarehouseId={() => snapshot.selectedWarehouseId}
          selectedOperatorIds={selected.filter(id => session.graph.hasOperator(id))}
          onHighlight={(operators, links) => setSelected([...operators, ...links])}
        />
      )}
      {view === "canvas" && heatmap && (
        <HeatmapLegend
          view={heatmap}
          snapshot={execution}
        />
      )}
      {view === "canvas" && hover && runtime && (heatmap || resultPreview) && (
        <aside
          className="react-operator-hover"
          style={{
            position: "fixed",
            left: Math.min(hover.x, Math.max(0, window.innerWidth - 350)),
            top: Math.min(hover.y, Math.max(0, window.innerHeight - 280)),
          }}
          onMouseEnter={() => {
            if (hoverTimer.current) clearTimeout(hoverTimer.current);
          }}
          onMouseLeave={() => setHover(undefined)}>
          {heatmap ? (
            <>
              <strong>{heatmapViewTitle(heatmap)}</strong>
              <p>
                {execution?.operators[hover.id]
                  ? formatMetricForView(
                      rawMetricForView(extractPerformanceMetrics(execution.operators[hover.id]), heatmap),
                      heatmap
                    )
                  : "—"}
              </p>
            </>
          ) : (
            <OperatorPreview
              id={hover.id}
              runtime={runtime}
              session={session}
              onPin={() => {
                setPinned(previous => [...new Set([...previous, hover.id])]);
                setHover(undefined);
              }}
            />
          )}
        </aside>
      )}
      {pinned
        .filter(id => session.graph.hasOperator(id))
        .map((id, index) => (
          <FloatingResultWindow
            key={id}
            title={session.graph.getOperator(id).customDisplayName ?? id}
            index={index}
            close={() => setPinned(previous => previous.filter(value => value !== id))}>
            {results([id])}
          </FloatingResultWindow>
        ))}
      {dialog === "share" && (
        <WorkflowShareDialog
          entry={{
            workflow,
            isOwner: false,
            accessLevel: workflow.readonly ? "READ" : "WRITE",
            ownerName: undefined,
            ownerId: 0,
            coverImage: null,
          }}
          repository={repository}
          user={user}
          close={() => setDialog(undefined)}
        />
      )}
      {dialog === "settings" && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Workflow settings">
          <h2>Workflow settings</h2>
          <label>
            Data transfer batch size
            <input
              type="number"
              min="1"
              disabled={!canEdit}
              value={workflow.content.settings.dataTransferBatchSize}
              onChange={event =>
                act(() => {
                  const value = event.currentTarget.valueAsNumber;
                  if (!Number.isSafeInteger(value) || value <= 0)
                    throw new Error("Batch size must be a positive integer.");
                  session.setSettings({ ...workflow.content.settings, dataTransferBatchSize: value });
                })
              }
            />
          </label>
          <label>
            Execution mode
            <select
              disabled={!canEdit}
              value={workflow.content.settings.executionMode}
              onChange={event =>
                act(() =>
                  session.setSettings({
                    ...workflow.content.settings,
                    executionMode: event.target.value as ExecutionMode,
                  })
                )
              }>
              {Object.values(ExecutionMode).map(value => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </label>
          <label>
            Description
            <textarea
              disabled={!canEdit}
              value={workflow.description ?? ""}
              onChange={event => act(() => session.setDescription(event.target.value))}
            />
          </label>
          <button onClick={() => setDialog(undefined)}>Close settings</button>
        </div>
      )}
      {dialog === "comment" && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Add comment">
          <label>
            Comment
            <textarea
              value={comment}
              onChange={event => setComment(event.target.value)}
            />
          </label>
          <button
            disabled={!comment.trim() || !canEdit}
            onClick={() =>
              act(() => {
                session.addComment({
                  commentBoxID: `comment-${uuid()}`,
                  commentBoxPosition: { x: 150, y: 100 },
                  comments: [
                    {
                      content: comment,
                      creationTime: new Date().toISOString(),
                      creatorID: user.uid,
                      creatorName: user.name,
                    },
                  ],
                });
                setComment("");
                setDialog(undefined);
              })
            }>
            Post comment
          </button>
          <button onClick={() => setDialog(undefined)}>Cancel</button>
        </div>
      )}
      {dialog === "versions" && (
        <VersionDialog
          repository={repository}
          wid={workflow.wid!}
          close={() => setDialog(undefined)}
          onOpen={props.onVersion}
          onClone={props.onOpenWorkflow}
        />
      )}
      {dialog === "restore-version" && props.versionId !== undefined && (
        <VersionRestoreDialog
          api={api}
          wid={workflow.wid!}
          versionId={props.versionId}
          onClose={() => setDialog(undefined)}
          onRestored={wid => {
            setDialog(undefined);
            props.onOpenWorkflow?.(wid);
          }}
        />
      )}
      {dialog === "time-travel" && runtime && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Time travel">
          <button onClick={() => setDialog(undefined)}>Close time travel</button>
          <WorkspaceTimeTravel
            api={api}
            session={session}
            runtime={runtime}
            initialExecutionId={historyExecution}
          />
        </div>
      )}
      {commentBoxId && session.graph.hasCommentBox(commentBoxId) && (
        <div
          role="dialog"
          aria-modal="true"
          aria-label="Comments">
          <h2>Comments</h2>
          {session.graph.getCommentBox(commentBoxId).comments.map(item => (
            <article key={`${item.creatorID}:${item.creationTime}`}>
              <strong>{item.creatorName}</strong>
              <p>{item.content}</p>
              {item.creatorID === user.uid && canEdit && (
                <>
                  <button
                    onClick={() => {
                      const content = window.prompt("Edit comment", item.content);
                      if (content !== null)
                        act(() => session.editComment(commentBoxId, item.creatorID, item.creationTime, content));
                    }}>
                    Edit comment
                  </button>
                  <button
                    onClick={() => act(() => session.deleteComment(commentBoxId, item.creatorID, item.creationTime))}>
                    Delete comment
                  </button>
                </>
              )}
            </article>
          ))}
          <label>
            Reply
            <textarea
              value={comment}
              disabled={!canEdit}
              onChange={event => setComment(event.target.value)}
            />
          </label>
          <button
            disabled={!comment.trim() || !canEdit}
            onClick={() =>
              act(() => {
                session.replyToComment(commentBoxId, {
                  content: comment,
                  creationTime: new Date().toISOString(),
                  creatorID: user.uid,
                  creatorName: user.name,
                });
                setComment("");
              })
            }>
            Reply
          </button>
          <button
            onClick={() => {
              setCommentBoxId(undefined);
              setComment("");
            }}>
            Close comments
          </button>
        </div>
      )}
      {contextMenu && (
        <div
          role="menu"
          className="react-canvas-menu"
          style={{ position: "fixed", left: contextMenu.x, top: contextMenu.y }}
          onKeyDown={event => {
            if (event.key === "Escape") setContextMenu(undefined);
          }}>
          <button
            role="menuitem"
            onClick={() => {
              act(copy);
              setContextMenu(undefined);
            }}>
            Copy
          </button>
          <button
            role="menuitem"
            disabled={!canEdit}
            onClick={() => {
              act(paste);
              setContextMenu(undefined);
            }}>
            Paste
          </button>
          <button
            role="menuitem"
            disabled={!canEdit}
            onClick={() => {
              act(removeSelection);
              setContextMenu(undefined);
            }}>
            Delete
          </button>
          {session.graph.hasOperator(contextMenu.id) && (
            <>
              <button
                role="menuitem"
                disabled={!canEdit}
                onClick={() => {
                  act(() => session.setDisabled(contextMenu.id, !session.graph.getOperator(contextMenu.id).isDisabled));
                  setContextMenu(undefined);
                }}>
                Toggle disabled
              </button>
              <button
                role="menuitem"
                disabled={runDisabled}
                onClick={() => {
                  act(() => runtime!.run({ targetOperatorId: contextMenu.id }));
                  setContextMenu(undefined);
                }}>
                Run until this operator
              </button>
            </>
          )}
          <button
            role="menuitem"
            onClick={() => setContextMenu(undefined)}>
            Close menu
          </button>
        </div>
      )}
    </section>
  );
}
function VersionDialog({
  repository,
  wid,
  close,
  onOpen,
  onClone,
}: {
  repository: WorkflowRepository;
  wid: number;
  close(): void;
  onOpen?: (vid: number) => void;
  onClone?: (wid: number) => void;
}) {
  const [versions, setVersions] = useState<Awaited<ReturnType<WorkflowRepository["getVersions"]>>>();
  const [error, setError] = useState<string>();
  useEffect(() => {
    const abort = new AbortController();
    void repository.getVersions(wid, abort.signal).then(setVersions, reason => {
      if (!abort.signal.aborted) setError(message(reason));
    });
    return () => abort.abort();
  }, [repository, wid]);
  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Workflow versions">
      <h2>Workflow versions</h2>
      {error && <p role="alert">{error}</p>}
      {!versions && !error ? (
        <p>Loading versions…</p>
      ) : (
        <ol>
          {versions?.map((version, index) => (
            <li key={version.vId}>
              {new Date(version.creationTime).toLocaleString()} {version.importance ? "★" : ""}
              <button
                onClick={() => {
                  onOpen?.(version.vId);
                  close();
                }}>
                Open version {versions.length - index}
              </button>
              <button
                onClick={() => {
                  void repository.cloneVersion(version.vId, versions.length - index).then(
                    wid => {
                      onClone?.(wid);
                      close();
                    },
                    reason => setError(message(reason))
                  );
                }}>
                Clone version {versions.length - index}
              </button>
            </li>
          ))}
        </ol>
      )}
      <button onClick={close}>Close versions</button>
    </div>
  );
}
function message(reason: unknown): string {
  return reason instanceof Error ? reason.message : "The workflow operation failed.";
}
function FloatingResultWindow({
  title,
  index,
  close,
  children,
}: {
  title: string;
  index: number;
  close(): void;
  children: import("react").ReactNode;
}) {
  const [position, setPosition] = useState({
    x: Math.min(260 + index * 30, window.innerWidth - 300),
    y: Math.min(160 + index * 30, window.innerHeight - 200),
  });
  const drag = useRef<{ x: number; y: number; left: number; top: number } | undefined>(undefined);
  return (
    <section
      role="dialog"
      aria-label={`Results: ${title}`}
      className="react-result-window"
      style={{ left: Math.max(0, position.x), top: Math.max(0, position.y) }}>
      <header
        onPointerDown={event => {
          if ((event.target as HTMLElement).closest("button")) return;
          event.currentTarget.setPointerCapture(event.pointerId);
          drag.current = { x: event.clientX, y: event.clientY, left: position.x, top: position.y };
        }}
        onPointerMove={event => {
          if (drag.current)
            setPosition({
              x: Math.min(window.innerWidth - 100, drag.current.left + event.clientX - drag.current.x),
              y: Math.min(window.innerHeight - 40, drag.current.top + event.clientY - drag.current.y),
            });
        }}
        onPointerUp={() => {
          drag.current = undefined;
        }}
        onPointerCancel={() => {
          drag.current = undefined;
        }}>
        <strong>{title}</strong>
        <button
          aria-label={`Close results: ${title}`}
          onClick={close}>
          ×
        </button>
      </header>
      <div>{children}</div>
    </section>
  );
}

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

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import type { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import type { SessionUser } from "../core/auth";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
import { RichText } from "../common/rich-text";
import { saveDownload } from "../workflows/workflow-repository";
import { AgentClient, visibleAgentSteps, type AgentSettings, type ReActStep } from "./agent-client";
import { NotebookClient, notebookHighlight } from "./notebook-client";
import "./assistant.css";
export interface WorkspaceAssistantProps {
  api: ApiClient;
  user: SessionUser;
  config: Readonly<GuiConfig>;
  session: WorkflowSession;
  runtime: WorkflowRuntime;
  getToken: () => string | null;
  getComputingUnitId?: () => number | undefined;
  getWarehouseId?: () => number | undefined;
  selectedOperatorIds?: readonly string[];
  onHighlight?: (operatorIds: readonly string[], linkIds: readonly string[]) => void;
}
export function WorkspaceAssistant(props: WorkspaceAssistantProps) {
  return (
    <>
      {props.config.copilotEnabled && <AgentPanel {...props} />}{" "}
      {props.config.pythonNotebookMigrationEnabled && <NotebookPanel {...props} />}
    </>
  );
}
function errorMessage(reason: unknown) {
  return reason instanceof Error ? reason.message : "The request failed.";
}
function AgentPanel(props: WorkspaceAssistantProps) {
  const latest = useRef(props);
  latest.current = props;
  const { api, session, runtime, user } = props;
  const client = useMemo(
    () =>
      new AgentClient(api, {
        workflowId: session.getSnapshot().workflow.wid!,
        getComputingUnitId: () => latest.current.getComputingUnitId?.(),
        getWarehouseId: () => latest.current.getWarehouseId?.(),
        onWorkflow: content => session.applyRemoteWorkflow(content),
        onGenerating: generating => session.setAutosaveSuspended(generating, "agent"),
      }),
    [api, session]
  );
  const snapshot = useSyncExternalStore(client.subscribe, client.getSnapshot),
    workspace = useSyncExternalStore(session.subscribe, session.getSnapshot),
    execution = useSyncExternalStore(runtime.subscribe, runtime.getSnapshot);
  const [open, setOpen] = useState(true),
    [message, setMessage] = useState(""),
    [source, setSource] = useState<"chat" | "feedback">("chat"),
    [model, setModel] = useState(""),
    [name, setName] = useState(""),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [deleting, setDeleting] = useState<string>(),
    [clearing, setClearing] = useState(false),
    [settings, setSettings] = useState(false),
    [filtered, setFiltered] = useState<ReActStep[]>(),
    [results, setResults] = useState<Record<string, unknown>>();
  const readonly = !!workspace.workflow.readonly,
    agent = snapshot.agents.find(agent => agent.id === snapshot.activeId);
  const steps = filtered ?? visibleAgentSteps(snapshot.steps, snapshot.headId),
    list = useRef<HTMLDivElement>(null);
  useEffect(() => {
    void client.load().catch(() => undefined);
    return () => client.dispose();
  }, [client]);
  useEffect(() => {
    setFiltered(undefined);
  }, [snapshot.activeId, snapshot.steps]);
  useEffect(() => {
    if (list.current) list.current.scrollTop = list.current.scrollHeight;
  }, [steps.length, snapshot.steps]);
  async function action(task: () => Promise<void>) {
    setBusy(true);
    setError("");
    try {
      await task();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setBusy(false);
    }
  }
  async function send() {
    const content = message.trim();
    if (!content) return;
    await action(async () => {
      await session.save();
      client.send(content, source);
      setMessage("");
    });
  }
  return (
    <aside
      className="workspace-assistant"
      aria-label="Workflow assistant">
      <header>
        <h2>Agents</h2>
        <span>
          {user.name} · Execution {execution.state}
        </span>
        <button onClick={() => setOpen(!open)}>{open ? "Minimize agents" : "Open agents"}</button>
      </header>
      {open && (
        <>
          <label>
            Active agent{" "}
            <select
              value={snapshot.activeId ?? ""}
              onChange={event => {
                try {
                  if (event.target.value) client.activate(event.target.value);
                  else client.deactivate();
                  setError("");
                } catch (reason) {
                  setError(errorMessage(reason));
                }
              }}>
              <option value="">Choose agent</option>
              {snapshot.agents.map(agent => (
                <option
                  key={agent.id}
                  value={agent.id}>
                  {agent.name}
                </option>
              ))}
            </select>
          </label>
          <details>
            <summary>Create agent</summary>
            <form
              onSubmit={event => {
                event.preventDefault();
                void action(async () => {
                  await session.save();
                  await client.create(model || snapshot.models[0]?.id || "", name.trim() || undefined);
                  setName("");
                });
              }}>
              <label>
                Agent name{" "}
                <input
                  value={name}
                  onChange={event => setName(event.target.value)}
                />
              </label>
              <label>
                Model{" "}
                <select
                  value={model || snapshot.models[0]?.id || ""}
                  onChange={event => setModel(event.target.value)}>
                  {snapshot.models.map(model => (
                    <option
                      key={model.id}
                      value={model.id}>
                      {model.name}
                    </option>
                  ))}
                </select>
              </label>
              <button disabled={busy || readonly || !snapshot.models.length}>Create agent</button>
            </form>
          </details>
          {snapshot.loading && <p role="status">Loading agents…</p>}
          {(error || snapshot.error) && <p role="alert">{error || snapshot.error}</p>}
          {agent && (
            <>
              <div className="assistant-toolbar">
                <strong>
                  {agent.name} · {snapshot.state} · {snapshot.connection}
                </strong>
                <button
                  disabled={busy}
                  onClick={() => setSettings(true)}>
                  Agent settings
                </button>
                <button
                  disabled={busy || readonly || snapshot.state === "GENERATING"}
                  onClick={() => setClearing(true)}>
                  Clear conversation
                </button>
                <button
                  disabled={busy || !snapshot.steps.length}
                  onClick={() =>
                    void action(async () => {
                      const steps = await client.steps();
                      const exported = {
                        agentId: agent.id,
                        agentName: agent.name,
                        modelType: agent.modelType,
                        exportedAt: new Date().toISOString(),
                        stepCount: steps.length,
                        steps: steps.map(step => ({
                          ...step,
                          operatorAccess: step.operatorAccess ? Object.fromEntries(step.operatorAccess) : undefined,
                        })),
                      };
                      saveDownload(
                        new Blob([JSON.stringify(exported, null, 2)], { type: "application/json" }),
                        agent.name + "-react-steps.json"
                      );
                    })
                  }>
                  Export conversation
                </button>
                <button
                  disabled={busy || readonly}
                  onClick={() => setDeleting(agent.id)}>
                  Delete agent
                </button>
                <button
                  disabled={busy}
                  onClick={() => void action(async () => setResults((await client.operatorResults()).results))}>
                  Operator results
                </button>
                {!!props.selectedOperatorIds?.length && (
                  <button
                    onClick={() =>
                      void action(async () => setFiltered(await client.stepsByOperators(props.selectedOperatorIds!)))
                    }>
                    Messages for selected operators
                  </button>
                )}
                {filtered && <button onClick={() => setFiltered(undefined)}>All messages</button>}
              </div>
              <div
                className="assistant-chat"
                ref={list}>
                {!steps.length && <p>No messages yet.</p>}
                {steps.map(step => (
                  <article
                    key={step.id}
                    className={"assistant-step " + step.role}
                    onMouseEnter={() => props.onHighlight?.(stepOperators(step), [])}>
                    <header>
                      <strong>{step.role === "user" ? "User" : agent.name}</strong>
                      <time>{step.timestamp.toLocaleString()}</time>
                      {step.isEnd && <small>Complete</small>}
                    </header>
                    <RichText content={step.content} />
                    {(step.toolCalls?.length || step.toolResults?.length || step.usage || step.inputMessages) && (
                      <details>
                        <summary>Step details</summary>
                        {step.toolCalls?.map((tool, index) => (
                          <section key={index}>
                            <h4>{String(tool.toolName ?? "Tool")}</h4>
                            <pre>{JSON.stringify(tool.input ?? tool, null, 2)}</pre>
                          </section>
                        ))}
                        {step.toolResults?.map((result, index) => (
                          <pre key={index}>{JSON.stringify(result.output ?? result.result ?? result, null, 2)}</pre>
                        ))}
                        {step.usage && (
                          <p>
                            Tokens: input {step.usage.inputTokens ?? 0}, output {step.usage.outputTokens ?? 0}, cached{" "}
                            {step.usage.cachedInputTokens ?? 0}, total {step.usage.totalTokens ?? 0}
                          </p>
                        )}
                        {step.inputMessages && (
                          <details>
                            <summary>Input context</summary>
                            <pre>{JSON.stringify(step.inputMessages, null, 2)}</pre>
                          </details>
                        )}
                      </details>
                    )}
                  </article>
                ))}
              </div>
              <form
                onSubmit={event => {
                  event.preventDefault();
                  void send();
                }}>
                <label>
                  Message{" "}
                  <textarea
                    value={message}
                    onChange={event => setMessage(event.target.value)}
                    onKeyDown={event => {
                      if (event.key === "Enter" && !event.shiftKey) {
                        event.preventDefault();
                        if (!busy && !readonly && snapshot.state === "AVAILABLE") void send();
                      }
                    }}
                  />
                </label>
                <label>
                  Message source{" "}
                  <select
                    value={source}
                    onChange={event => setSource(event.target.value as "chat" | "feedback")}>
                    <option value="chat">Chat</option>
                    <option value="feedback">Feedback</option>
                  </select>
                </label>
                <button
                  disabled={
                    busy ||
                    readonly ||
                    !message.trim() ||
                    snapshot.state !== "AVAILABLE" ||
                    snapshot.connection === "disconnected"
                  }>
                  Send message
                </button>
                {["GENERATING", "STOPPING"].includes(snapshot.state) && (
                  <button
                    type="button"
                    disabled={busy || snapshot.state === "STOPPING"}
                    onClick={() => void action(() => client.stop())}>
                    Stop generation
                  </button>
                )}
              </form>
              {results && (
                <details open>
                  <summary>Operator results</summary>
                  <pre>{JSON.stringify(results, null, 2)}</pre>
                  <button onClick={() => setResults(undefined)}>Close results</button>
                </details>
              )}
            </>
          )}
          {clearing && (
            <section
              role="dialog"
              aria-label="Clear conversation">
              <p>Clear this agent's conversation?</p>
              <button
                disabled={busy}
                onClick={() =>
                  void action(async () => {
                    await client.clear();
                    setClearing(false);
                  })
                }>
                Confirm clear
              </button>
              <button onClick={() => setClearing(false)}>Cancel clear</button>
            </section>
          )}
          {deleting && (
            <section
              role="dialog"
              aria-label="Delete agent">
              <p>Delete this agent and conversation?</p>
              <button
                disabled={busy}
                onClick={() =>
                  void action(async () => {
                    await client.remove(deleting);
                    setDeleting(undefined);
                  })
                }>
                Confirm agent deletion
              </button>
              <button onClick={() => setDeleting(undefined)}>Cancel agent deletion</button>
            </section>
          )}
          {settings && (
            <AgentSettingsDialog
              client={client}
              readonly={readonly}
              close={() => setSettings(false)}
            />
          )}
        </>
      )}
    </aside>
  );
}
function stepOperators(step: ReActStep): string[] {
  const ids = new Set<string>();
  step.operatorAccess?.forEach(access => {
    for (const list of [access.viewedOperatorIds, access.addedOperatorIds, access.modifiedOperatorIds])
      if (Array.isArray(list)) for (const id of list) if (typeof id === "string") ids.add(id);
  });
  return [...ids];
}
function AgentSettingsDialog({
  client,
  readonly,
  close,
}: {
  client: AgentClient;
  readonly: boolean;
  close: () => void;
}) {
  const [settings, setSettings] = useState<AgentSettings>(),
    [info, setInfo] = useState<Awaited<ReturnType<AgentClient["systemInfo"]>>>(),
    [types, setTypes] = useState<{ type: string; description: string }[]>([]),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [busy, setBusy] = useState(false);
  useEffect(() => {
    let active = true;
    void Promise.all([client.settings(), client.systemInfo(), client.operatorTypes()]).then(
      ([settings, info, types]) => {
        if (active) {
          setSettings(settings);
          setInfo(info);
          setTypes(types);
        }
      },
      reason => {
        if (active) setError(errorMessage(reason));
      }
    );
    return () => {
      active = false;
    };
  }, [client]);
  const numeric: [keyof AgentSettings, string][] = [
    ["maxOperatorResultCharLimit", "Result character limit"],
    ["maxOperatorResultCellCharLimit", "Cell character limit"],
    ["toolTimeoutSeconds", "Tool timeout (seconds)"],
    ["executionTimeoutMinutes", "Execution timeout (minutes)"],
    ["maxSteps", "Maximum steps"],
  ];
  return (
    <section
      role="dialog"
      aria-modal="true"
      aria-label="Agent settings"
      className="assistant-settings">
      <h3>Agent settings</h3>
      {!settings && !error && <p>Loading settings…</p>}
      {error && <p role="alert">{error}</p>}
      {notice && <p role="status">{notice}</p>}
      {settings && (
        <form
          onSubmit={event => {
            event.preventDefault();
            setBusy(true);
            setError("");
            void client
              .updateSettings(settings)
              .then(
                value => {
                  setSettings(value);
                  setNotice("Agent settings saved.");
                },
                reason => setError(errorMessage(reason))
              )
              .finally(() => setBusy(false));
          }}>
          {numeric.map(([key, label]) => (
            <label key={key}>
              {label}
              <input
                type="number"
                min="1"
                step={key === "maxSteps" ? "1" : "any"}
                disabled={readonly}
                value={String(settings[key] ?? "")}
                onChange={event => setSettings({ ...settings, [key]: Number(event.target.value) })}
              />
            </label>
          ))}
          <fieldset disabled={readonly}>
            <legend>Enabled tools</legend>
            {info?.tools?.map(tool => (
              <label
                key={tool.name}
                title={tool.description}>
                <input
                  type="checkbox"
                  checked={!settings.disabledTools?.includes(tool.name)}
                  onChange={event =>
                    setSettings({
                      ...settings,
                      disabledTools: event.target.checked
                        ? (settings.disabledTools ?? []).filter(name => name !== tool.name)
                        : [...(settings.disabledTools ?? []), tool.name],
                    })
                  }
                />
                {tool.name}
              </label>
            ))}
          </fieldset>
          <details>
            <summary>Allowed operators (empty means all)</summary>
            {types.map(type => (
              <label
                key={type.type}
                title={type.description}>
                <input
                  type="checkbox"
                  disabled={readonly}
                  checked={settings.allowedOperatorTypes?.includes(type.type) ?? false}
                  onChange={event =>
                    setSettings({
                      ...settings,
                      allowedOperatorTypes: event.target.checked
                        ? [...(settings.allowedOperatorTypes ?? []), type.type]
                        : (settings.allowedOperatorTypes ?? []).filter(value => value !== type.type),
                    })
                  }
                />
                {type.type}
              </label>
            ))}
          </details>
          <button disabled={busy || readonly}>Save agent settings</button>
        </form>
      )}
      <details>
        <summary>System prompt and tools</summary>
        <pre>{info?.systemPrompt}</pre>
        {info?.tools?.map(tool => (
          <details key={tool.name}>
            <summary>{tool.name}</summary>
            <p>{tool.description}</p>
            <pre>{JSON.stringify(tool.inputSchema, null, 2)}</pre>
          </details>
        ))}
      </details>
      <button
        disabled={busy}
        onClick={close}>
        Close agent settings
      </button>
    </section>
  );
}
function NotebookPanel(props: WorkspaceAssistantProps) {
  const { api, session } = props,
    latest = useRef(props);
  latest.current = props;
  const client = useMemo(() => new NotebookClient(api, true), [api]),
    snapshot = useSyncExternalStore(client.subscribe, client.getSnapshot),
    workspace = useSyncExternalStore(session.subscribe, session.getSnapshot),
    frame = useRef<HTMLIFrameElement>(null);
  const [deleting, setDeleting] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const wid = workspace.workflow.wid;
  useEffect(() => {
    void client.load(wid).catch(() => undefined);
  }, [client, wid]);
  useEffect(() => () => client.dispose(), [client]);
  useEffect(() => {
    const handle = (event: MessageEvent) => {
      if (
        !snapshot.visible ||
        !frame.current ||
        event.source !== frame.current.contentWindow ||
        event.origin !== snapshot.origin ||
        !snapshot.mapping
      )
        return;
      const data = event.data;
      if (!data || typeof data !== "object" || data.action !== "cellClicked" || typeof data.cellUUID !== "string")
        return;
      const highlight = notebookHighlight(
        snapshot.mapping,
        data.cellUUID,
        latest.current.session.getSnapshot().workflow.content
      );
      latest.current.onHighlight?.(highlight.operators, highlight.links);
    };
    window.addEventListener("message", handle);
    return () => window.removeEventListener("message", handle);
  }, [snapshot]);
  const selectedKey = JSON.stringify(props.selectedOperatorIds ?? []);
  useEffect(() => {
    if (!snapshot.visible || !snapshot.mapping || !snapshot.origin || !frame.current?.contentWindow) return;
    const cells = [
      ...new Set(
        (latest.current.selectedOperatorIds ?? []).flatMap(id =>
          Object.prototype.hasOwnProperty.call(snapshot.mapping!.operator_to_cell, id)
            ? snapshot.mapping!.operator_to_cell[id]
            : []
        )
      ),
    ];
    if (cells.length)
      frame.current.contentWindow.postMessage({ action: "triggerCellClick", operators: cells }, snapshot.origin);
  }, [snapshot, selectedKey]);
  return (
    <aside
      className="workspace-notebook"
      aria-label="Source notebook">
      {snapshot.loading && <p role="status">Loading source notebook…</p>}
      {(error || snapshot.error) && <p role="alert">{error || snapshot.error}</p>}
      {snapshot.error && <button onClick={() => void client.load(wid).catch(() => undefined)}>Retry notebook</button>}
      {snapshot.exists && !snapshot.visible && <button onClick={() => client.open()}>Open uploaded source code</button>}
      {snapshot.visible && (
        <>
          <header>
            <h2>Uploaded Source Code</h2>
            <button onClick={() => client.minimize()}>Minimize source code</button>
            {!workspace.workflow.readonly && (
              <button onClick={() => setDeleting(true)}>Delete uploaded source code</button>
            )}
          </header>
          {snapshot.url && (
            <iframe
              ref={frame}
              title="Uploaded Source Code"
              src={snapshot.url}
            />
          )}
        </>
      )}
      {deleting && (
        <section
          role="dialog"
          aria-label="Delete notebook">
          <p>Delete this workflow's uploaded source code? This cannot be undone.</p>
          <button
            disabled={busy}
            onClick={() => {
              setBusy(true);
              setError("");
              void client
                .delete()
                .then(
                  () => {
                    setDeleting(false);
                    props.onHighlight?.([], []);
                  },
                  reason => setError(errorMessage(reason))
                )
                .finally(() => setBusy(false));
            }}>
            Confirm notebook deletion
          </button>
          <button
            disabled={busy}
            onClick={() => setDeleting(false)}>
            Cancel notebook deletion
          </button>
        </section>
      )}
    </aside>
  );
}

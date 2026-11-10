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
import * as Y from "yjs";
import type { ApiClient } from "../core/api-client";
import type { WorkflowSession } from "../workspace/workflow-session";
import { WorkflowRepository, type WorkflowRecord } from "./workflow-repository";
import { VersionDiff, versionHighlights, versionPropertyChanges, type VersionHighlights } from "./version-diff";
import { CodeEditorDialog } from "../code-editor/code-editor";
import type { CodeEditorOptions } from "../code-editor/runtime";
import type { OperatorPredicate } from "../../core/workspace/types/workflow-common.interface";
export type { VersionHighlights };
export interface VersionComparisonProps {
  api: ApiClient;
  wid: number;
  versionId: number;
  session?: WorkflowSession;
  onHighlight?: (highlights: VersionHighlights | undefined) => void;
  onSelectOperator?: (id: string) => void;
}
const noSubscribe = () => () => undefined,
  emptySnapshot = () => undefined;
function text(value: unknown): string {
  return value === undefined
    ? "Not present"
    : typeof value === "string"
      ? value
      : JSON.stringify(value, null, 2) ?? String(value);
}
function language(operator?: OperatorPredicate): CodeEditorOptions["language"] {
  const type = operator?.operatorType.toLowerCase() ?? "";
  return type.includes("javascript")
    ? "javascript"
    : type.includes("java")
      ? "java"
      : type.startsWith("rudf")
        ? "r"
        : "python";
}
export function VersionComparison({
  api,
  wid,
  versionId,
  session,
  onHighlight,
  onSelectOperator,
}: VersionComparisonProps) {
  const repository = useMemo(() => new WorkflowRepository(api), [api]),
    callbacks = useRef({ onHighlight, onSelectOperator });
  callbacks.current = { onHighlight, onSelectOperator };
  const local = useSyncExternalStore(session?.subscribe ?? noSubscribe, session?.getSnapshot ?? emptySnapshot);
  const [data, setData] = useState<{ current: WorkflowRecord; historical: WorkflowRecord }>(),
    [error, setError] = useState(""),
    [selected, setSelected] = useState<string>(),
    [code, setCode] = useState<{
      id: string;
      before: string;
      after: string;
      language: CodeEditorOptions["language"];
    }>();
  useEffect(() => {
    let active = true;
    setData(undefined);
    setError("");
    setSelected(undefined);
    setCode(undefined);
    if (!Number.isSafeInteger(wid) || wid <= 0 || !Number.isSafeInteger(versionId) || versionId <= 0) {
      setError("Invalid workflow or version identifier.");
      return;
    }
    void Promise.all([repository.get(wid), repository.getVersion(wid, versionId)])
      .then(([current, historical]) => {
        if (!active) return;
        if (current.wid !== wid || historical.wid !== wid)
          throw new Error("The compared version belongs to another workflow.");
        const diff = new VersionDiff();
        const difference = diff.getWorkflowsDifference(current.content, historical.content);
        versionHighlights(difference, current.content, diff.operatorPropertyDiff);
        setData({ current, historical });
      })
      .catch(reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Unable to compare workflow versions.");
      });
    return () => {
      active = false;
    };
  }, [repository, wid, versionId]);
  const comparison = useMemo(() => {
    if (!data) return;
    const current =
      local && !local.workflow.readonly && !local.previewing && local.workflow.wid === wid
        ? local.workflow
        : data.current;
    const helper = new VersionDiff(),
      diff = helper.getWorkflowsDifference(current.content, data.historical.content);
    return {
      current,
      historical: data.historical,
      highlights: versionHighlights(diff, current.content, helper.operatorPropertyDiff),
    };
  }, [data, local, wid]);
  useEffect(() => {
    callbacks.current.onHighlight?.(comparison?.highlights);
    return () => callbacks.current.onHighlight?.(undefined);
  }, [comparison]);
  const ids = comparison
    ? [...comparison.highlights.modified, ...comparison.highlights.added, ...comparison.highlights.deleted]
    : [];
  const selectedId = selected && ids.includes(selected) ? selected : ids[0];
  const before = comparison?.current.content.operators.find(operator => operator.operatorID === selectedId),
    after = comparison?.historical.content.operators.find(operator => operator.operatorID === selectedId),
    changes = versionPropertyChanges(before, after);
  return (
    <section
      aria-label="Workflow version comparison"
      className="workflow-version-comparison">
      <h2>Changes in version {versionId}</h2>
      <p>
        Compared with the current workflow: additions are green, modifications orange, and removed-operator neighbors
        have red brackets.
      </p>
      {error && <p role="alert">{error}</p>}
      {!data && !error && <p role="status">Loading version comparison…</p>}
      {comparison && (
        <>
          <p>
            {comparison.highlights.added.length} added · {comparison.highlights.modified.length} modified ·{" "}
            {comparison.highlights.deleted.length} deleted
          </p>
          {!ids.length ? (
            <p>No operator differences.</p>
          ) : (
            <>
              <nav aria-label="Changed operators">
                {ids.map(id => (
                  <button
                    key={id}
                    aria-label={"Compare operator " + id}
                    aria-pressed={selectedId === id}
                    onClick={() => {
                      setSelected(id);
                      callbacks.current.onSelectOperator?.(id);
                    }}>
                    {id} (
                    {comparison.highlights.added.includes(id)
                      ? "added"
                      : comparison.highlights.deleted.includes(id)
                        ? "deleted"
                        : "modified"}
                    )
                  </button>
                ))}
              </nav>
              <h3>{after?.customDisplayName ?? before?.customDisplayName ?? selectedId}</h3>
              {!changes.length ? (
                <p>Operator metadata or ports changed; property values match.</p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>Property</th>
                      <th>Current workflow</th>
                      <th>Selected version</th>
                      <th>Code</th>
                    </tr>
                  </thead>
                  <tbody>
                    {changes.map(change => (
                      <tr key={change.property}>
                        <th>{change.property}</th>
                        <td>
                          <pre>{text(change.before)}</pre>
                        </td>
                        <td>
                          <pre>{text(change.after)}</pre>
                        </td>
                        <td>
                          {change.property !== "operatorVersion" &&
                            (typeof change.before === "string" || typeof change.after === "string") &&
                            (/code|script/i.test(change.property) ||
                              String(change.before ?? change.after).includes("\n")) && (
                              <button
                                onClick={() =>
                                  setCode({
                                    id: (selectedId ?? "operator") + "-" + change.property,
                                    before: typeof change.before === "string" ? change.before : "",
                                    after: typeof change.after === "string" ? change.after : "",
                                    language: language(after ?? before),
                                  })
                                }>
                                Open code diff
                              </button>
                            )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              <details>
                <summary>Complete operator comparison</summary>
                <h4>Current workflow</h4>
                <pre>{text(before)}</pre>
                <h4>Selected version</h4>
                <pre>{text(after)}</pre>
              </details>
            </>
          )}
        </>
      )}
      {code && (
        <ReadonlyCodeDiff
          {...code}
          close={() => setCode(undefined)}
        />
      )}
    </section>
  );
}
function ReadonlyCodeDiff({
  id,
  before,
  after,
  language,
  close,
}: {
  id: string;
  before: string;
  after: string;
  language: CodeEditorOptions["language"];
  close: () => void;
}) {
  const doc = useMemo(() => {
    const doc = new Y.Doc();
    doc.getText("code").insert(0, after);
    return doc;
  }, [after]);
  useEffect(() => () => doc.destroy(), [doc]);
  return (
    <CodeEditorDialog
      session={{
        operatorId: id,
        title: "Version comparison",
        code: doc.getText("code"),
        originalCode: before,
        language,
        readOnly: true,
      }}
      onClose={close}
    />
  );
}

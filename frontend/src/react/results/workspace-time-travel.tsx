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
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WorkflowRuntime } from "../workspace/workflow-runtime";
import type { ReplayExecutionInfo } from "../../core/workspace/types/workflow-websocket.interface";
import { TimeTravelController } from "./time-travel";
export interface WorkspaceTimeTravelProps {
  api: ApiClient;
  session: WorkflowSession;
  runtime: WorkflowRuntime;
  initialExecutionId?: number;
  onReplayChange?: (point: ReplayExecutionInfo | undefined) => void;
}
export function WorkspaceTimeTravel({
  api,
  session,
  runtime,
  initialExecutionId,
  onReplayChange,
}: WorkspaceTimeTravelProps) {
  const controller = useMemo(() => new TimeTravelController(api, session, runtime), [api, session, runtime]),
    snapshot = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  const [expanded, setExpanded] = useState<number[]>(initialExecutionId !== undefined ? [initialExecutionId] : []),
    [error, setError] = useState("");
  const currentExpanded = useRef(expanded);
  currentExpanded.current = expanded;
  useEffect(() => {
    const refresh = () => {
      void controller
        .refresh()
        .then(() => Promise.all(currentExpanded.current.map(eid => controller.interactions(eid))))
        .catch(reason => setError(reason instanceof Error ? reason.message : "Unable to load execution history."));
    };
    refresh();
    const timer = setInterval(refresh, 5000);
    return () => {
      clearInterval(timer);
      controller.dispose();
    };
  }, [controller]);
  useEffect(() => onReplayChange?.(snapshot.replay), [onReplayChange, snapshot.replay]);
  useEffect(() => {
    if (initialExecutionId !== undefined)
      setExpanded(current => (current.includes(initialExecutionId) ? current : [...current, initialExecutionId]));
  }, [initialExecutionId]);
  function toggle(eid: number) {
    if (expanded.includes(eid)) setExpanded(expanded.filter(id => id !== eid));
    else {
      setExpanded([...expanded, eid]);
      setError("");
      void controller
        .interactions(eid)
        .catch(reason => setError(reason instanceof Error ? reason.message : "Unable to read interactions."));
    }
  }
  return (
    <section
      className="react-results"
      aria-label="Execution time travel">
      <h2>Time travel</h2>
      {snapshot.replay && (
        <div role="status">
          <p>
            Replaying execution {snapshot.replay.eid} to {snapshot.replay.interaction}. The historical graph is
            read-only.
          </p>
          <button
            disabled={snapshot.busy}
            onClick={() => void controller.exit()}>
            Return to current workflow
          </button>
        </div>
      )}
      {(snapshot.error || error) && <p role="alert">{snapshot.error || error}</p>}
      {snapshot.loading && <p role="status">Loading logged executions…</p>}
      {!snapshot.loading && !snapshot.entries.length && <p>No executions with replay logs.</p>}
      <ul>
        {snapshot.entries.map(entry => (
          <li key={entry.eId}>
            <button
              aria-label={"Interactions for execution " + entry.eId}
              aria-expanded={expanded.includes(entry.eId)}
              onClick={() => toggle(entry.eId)}>
              Execution {entry.eId} · {entry.name} · {new Date(entry.startingTime).toLocaleString()}
            </button>
            {expanded.includes(entry.eId) && (
              <div>
                {!snapshot.interactions[entry.eId] ? (
                  <p>Loading interactions…</p>
                ) : !snapshot.interactions[entry.eId].length ? (
                  <p>No interactions recorded.</p>
                ) : (
                  snapshot.interactions[entry.eId].map((interaction, index) => (
                    <button
                      key={index}
                      disabled={
                        snapshot.busy ||
                        (snapshot.replay?.eid === entry.eId && snapshot.replay.interaction === interaction)
                      }
                      onClick={() => {
                        setError("");
                        void controller
                          .replay(entry, interaction)
                          .catch(reason => setError(reason instanceof Error ? reason.message : "Replay failed."));
                      }}>
                      {interaction}
                    </button>
                  ))
                )}
              </div>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}

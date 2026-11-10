/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import * as Y from "yjs";
import type { BreakpointInfo } from "../../core/workspace/types/workflow-common.interface";
import type { WorkflowSession } from "./workflow-session";
import type { WorkflowRuntime } from "./workflow-runtime";
import type { CodeDebugSource, DebugSnapshot } from "../code-editor/debugger";

/** Continue/next remove temporary stops while retaining confirmed breakpoint IDs and conditions. */
export function resetOperatorDebugHits(session: WorkflowSession, operatorId: string): void {
  const debug = session.shared.debugState.get(operatorId);
  if (!debug) return;
  const update = () =>
    debug.forEach((value, key) => {
      if (value.breakpointId === undefined) debug.delete(key);
      else if (value.hit) debug.set(key, { ...value, hit: false });
    });
  if (debug.doc) debug.doc.transact(update);
  else update();
}

/** Same shared breakpoint map and Pdb command/confirmation protocol as the original debugger service. */
export function createOperatorDebugSource(session: WorkflowSession, runtime: WorkflowRuntime, operatorId: string) {
  const listeners = new Set<() => void>();
  let stops: (() => void)[] = [];
  let disposed = false;
  const maps = session.shared.debugState;
  const state = () => runtime.getSnapshot().operators[operatorId]?.operatorState;
  const enabled = () => runtime.canControl() && (state() === "Running" || state() === "Paused");
  const map = () => {
    let result = maps.get(operatorId);
    if (!result) {
      result = new Y.Map<BreakpointInfo>();
      maps.set(operatorId, result);
    }
    return result;
  };
  const notify = () => listeners.forEach(fn => fn());
  const send = (cmd: string, worker?: string) => {
    if (disposed) throw new Error("The debugger is closed.");
    if (!runtime.canControl())
      throw new Error("Write permission on the computing unit is required to control the debugger.");
    if (!enabled()) throw new Error("The operator must be running or paused to debug.");
    const workers = runtime.getSnapshot().workers[operatorId] ?? [];
    if (worker !== undefined && !workers.includes(worker)) throw new Error("The worker is not available.");
    for (const workerId of worker === undefined ? workers : [worker])
      runtime.socket.send("DebugCommandRequest", { operatorId, workerId, cmd });
  };
  const lineNumber = (line: number) => {
    if (!Number.isSafeInteger(line) || line < 1) throw new Error("A breakpoint line must be a positive integer.");
  };
  const resetHit = () => resetOperatorDebugHits(session, operatorId);
  const continueIfClear = () => {
    if (enabled() && !Array.from(map().values()).some(value => value.hit)) {
      resetHit();
      send("continue");
    }
  };
  const source: CodeDebugSource & {
    start(): void;
    dispose(): void;
    continue(workerId: string): void;
    step(workerId: string): void;
  } = {
    getSnapshot(): DebugSnapshot {
      return {
        enabled: enabled(),
        breakpoints: Array.from(maps.get(operatorId)?.entries() ?? [])
          .filter(([, value]) => value.breakpointId !== undefined || value.hit)
          .map(([line, value]) => ({ line: Number(line), condition: value.condition, hit: value.hit })),
      };
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    toggle(line) {
      lineNumber(line);
      const existing = maps.get(operatorId)?.get(String(line));
      send(existing ? `clear ${existing.breakpointId ?? ""}` : `break ${line}`);
    },
    setCondition(line, condition) {
      lineNumber(line);
      const current = maps.get(operatorId)?.get(String(line));
      const clean = condition.trim();
      if (!current || current.breakpointId === undefined) throw new Error("The breakpoint is not available.");
      if (current.condition === clean) return;
      send(`condition ${current.breakpointId} ${clean}`);
      map().set(String(line), { ...current, condition: clean });
    },
    continue(worker) {
      send("continue", worker);
      resetHit();
    },
    step(worker) {
      send("next", worker);
      resetHit();
    },
    start() {
      if (disposed || stops.length) return;
      maps.observeDeep(notify);
      let previous = state();
      stops = [
        runtime.subscribe(() => {
          const current = state();
          if (current !== previous && current === "Uninitialized") maps.get(operatorId)?.clear();
          previous = current;
          notify();
        }),
        runtime.socket.subscribeEvents(event => {
          if (disposed || event.type !== "ConsoleUpdateEvent" || event.operatorId !== operatorId) return;
          for (const message of event.messages) {
            if (message.source !== "(Pdb)" || message.msgType.name !== "DEBUGGER") continue;
            const title = message.title;
            const match = title.match(/(?:Breakpoint|Deleted breakpoint) (\d+) at .+:(\d+)/);
            const lineMatch = title.match(/\.py\((\d+)\)|:(\d+)/);
            const line = match ? Number(match[2]) : lineMatch ? Number(lineMatch[1] || lineMatch[2]) : undefined;
            if (title.startsWith(">") && line !== undefined) {
              const previous = map().get(String(line)) ?? { breakpointId: undefined, condition: "", hit: false };
              map().set(String(line), { ...previous, hit: true });
            } else if (title.startsWith("Breakpoint") && match) {
              map().set(String(line), { breakpointId: Number(match[1]), condition: "", hit: false });
              continueIfClear();
            } else if (title.startsWith("Deleted") && line !== undefined) {
              const current = map().get(String(line));
              if (!current) continue;
              map().delete(String(line));
              if (current.hit) map().set(String(line), { ...current, breakpointId: undefined });
              continueIfClear();
            } else if (title.startsWith("*** Blank or comment")) continueIfClear();
          }
        }),
      ];
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      if (stops.length) maps.unobserveDeep(notify);
      stops.forEach(stop => stop());
      stops = [];
      listeners.clear();
    },
  };
  return source;
}

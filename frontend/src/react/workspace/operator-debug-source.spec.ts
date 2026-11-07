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
import { describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { createOperatorDebugSource, resetOperatorDebugHits } from "./operator-debug-source";
import type { WorkflowSession } from "./workflow-session";
import type { WorkflowRuntime } from "./workflow-runtime";
import type { BreakpointInfo } from "../../core/workspace/types/workflow-common.interface";
import type { TexeraWebsocketEvent } from "../../core/workspace/types/workflow-websocket.interface";

function setup() {
  const doc = new Y.Doc(),
    debugState = doc.getMap<Y.Map<BreakpointInfo>>("debug");
  let state = "Running",
    canControl = true;
  const send = vi.fn();
  let onEvent: ((event: TexeraWebsocketEvent) => void) | undefined;
  const stop = vi.fn();
  const runtime = {
    canControl: () => canControl,
    getSnapshot: () => ({ operators: { op: { operatorState: state } }, workers: { op: ["w1", "w2"] } }),
    subscribe: () => stop,
    socket: {
      send,
      subscribeEvents: (fn: typeof onEvent) => {
        onEvent = fn;
        return stop;
      },
    },
  } as unknown as WorkflowRuntime;
  const session = { shared: { debugState } } as WorkflowSession;
  const source = createOperatorDebugSource(session, runtime, "op");
  const event = (title: string) =>
    onEvent?.({
      type: "ConsoleUpdateEvent",
      operatorId: "op",
      messages: [{ source: "(Pdb)", msgType: { name: "DEBUGGER" }, title, message: "" }],
    });
  return {
    doc,
    debugState,
    source,
    send,
    event,
    stop,
    state: (next: string) => {
      state = next;
    },
    control: (allowed: boolean) => {
      canControl = allowed;
    },
  };
}
describe("operator Monaco debug adapter", () => {
  it("blocks debug commands for a read-only computing unit while allowing a controlling owner", () => {
    const t = setup();
    t.control(false);
    expect(t.source.getSnapshot().enabled).toBe(false);
    expect(() => t.source.toggle(1)).toThrow(/permission|control/i);
    expect(t.send).not.toHaveBeenCalled();
    t.control(true);
    expect(t.source.getSnapshot().enabled).toBe(true);
    t.source.toggle(1);
    expect(t.send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op", workerId: "w1", cmd: "break 1" });
    t.source.dispose();
    t.doc.destroy();
  });
  it("clears hit highlights and temporary markers after console next/continue without losing real conditions", () => {
    const t = setup();
    const map = new Y.Map<BreakpointInfo>();
    t.debugState.set("op", map);
    map.set("10", { breakpointId: 1, hit: true, condition: "x > 0" });
    map.set("11", { breakpointId: undefined, hit: true, condition: "" });
    resetOperatorDebugHits({ shared: { debugState: t.debugState } } as WorkflowSession, "op");
    expect(map.get("10")).toEqual({ breakpointId: 1, hit: false, condition: "x > 0" });
    expect(map.has("11")).toBe(false);
    expect(() =>
      resetOperatorDebugHits({ shared: { debugState: t.debugState } } as WorkflowSession, "missing")
    ).not.toThrow();
    t.source.dispose();
    t.doc.destroy();
  });
  it("preserves Pdb confirmation, conditions, temporary hit markers and worker routing", () => {
    const t = setup();
    t.source.start();
    t.source.toggle(10);
    expect(t.send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op", workerId: "w1", cmd: "break 10" });
    expect(t.source.getSnapshot().breakpoints).toEqual([]);
    t.event("Breakpoint 1 at /file.py:10");
    expect(t.source.getSnapshot().breakpoints).toEqual([{ line: 10, condition: "", hit: false }]);
    t.source.setCondition(10, " x > 0 ");
    expect(t.send).toHaveBeenCalledWith("DebugCommandRequest", {
      operatorId: "op",
      workerId: "w2",
      cmd: "condition 1 x > 0",
    });
    t.event("> /file.py(10)process()");
    t.source.toggle(10);
    t.event("Deleted breakpoint 1 at /file.py:10");
    expect(t.source.getSnapshot().breakpoints).toEqual([{ line: 10, condition: "x > 0", hit: true }]);
    t.source.step("w1");
    expect(t.source.getSnapshot().breakpoints).toEqual([]);
    expect(t.send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op", workerId: "w1", cmd: "next" });
    t.source.dispose();
    t.doc.destroy();
  });
  it("ignores malformed messages, rejects inactive/invalid commands and detaches subscriptions", () => {
    const t = setup();
    t.source.start();
    t.event("Breakpoint nonsense");
    t.event("> unknown");
    expect(t.source.getSnapshot().breakpoints).toEqual([]);
    expect(() => t.source.toggle(0)).toThrow("positive");
    t.state("Completed");
    expect(t.source.getSnapshot().enabled).toBe(false);
    expect(() => t.source.toggle(1)).toThrow("running or paused");
    expect(t.send).not.toHaveBeenCalled();
    t.source.dispose();
    expect(t.stop).toHaveBeenCalledTimes(2);
    t.doc.destroy();
  });
});

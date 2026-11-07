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

import { afterEach, describe, expect, it, vi } from "vitest";
import { WorkflowSocket } from "./workflow-socket";

class FakeSocket {
  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((message: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  send = vi.fn();
  close = vi.fn();
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  message(value: unknown) {
    this.onmessage?.({ data: JSON.stringify(value) });
  }
}
const sockets: WorkflowSocket[] = [];
function build() {
  const opened: { url: string; ws: FakeSocket }[] = [];
  const socket = new WorkflowSocket({
    wid: 42,
    uid: 7,
    cuid: 9,
    baseUrl: "https://texera.test/",
    getToken: () => "a+b&c",
    createSocket: url => {
      const ws = new FakeSocket();
      opened.push({ url, ws });
      return ws;
    },
  });
  sockets.push(socket);
  socket.connect();
  return { socket, opened };
}
afterEach(() => {
  sockets.splice(0).forEach(socket => socket.close());
  vi.useRealTimers();
});

describe("React workflow WebSocket", () => {
  it("preserves endpoint/session parameters and sends the existing typed protocol", () => {
    const { socket, opened } = build();
    const url = new URL(opened[0].url);
    expect(url.protocol).toBe("wss:");
    expect(url.pathname).toBe("/wsapi/workflow-websocket");
    expect(Object.fromEntries(url.searchParams)).toEqual({ wid: "42", uid: "7", cuid: "9", "access-token": "a+b&c" });
    expect(() => socket.send("WorkflowPauseRequest", {})).toThrow(/connected/i);
    opened[0].ws.open();
    opened[0].ws.message({ type: "ClusterStatusUpdateEvent", numWorkers: 3 });
    socket.send("WorkflowPauseRequest", {});
    expect(opened[0].ws.send).toHaveBeenLastCalledWith(JSON.stringify({ type: "WorkflowPauseRequest" }));
    expect(socket.getSnapshot()).toMatchObject({ status: "connected", numWorkers: 3 });
  });
  it("does not redial a cleanly refused session, but reconnects a live session with the latest token", async () => {
    vi.useFakeTimers();
    const { socket, opened } = build();
    opened[0].ws.onclose?.();
    await vi.advanceTimersByTimeAsync(5000);
    expect(opened).toHaveLength(1);
    expect(socket.getSnapshot().status).toBe("refused");
    socket.connect();
    opened[1].ws.open();
    opened[1].ws.message({ type: "HeartBeatResponse" });
    opened[1].ws.onclose?.();
    await vi.advanceTimersByTimeAsync(2999);
    expect(opened).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(opened).toHaveLength(3);
  });
  it("sends heartbeats and cancels timers, listeners and pending requests on close", async () => {
    vi.useFakeTimers();
    const { socket, opened } = build();
    opened[0].ws.open();
    opened[0].ws.send.mockClear();
    await vi.advanceTimersByTimeAsync(10000);
    expect(opened[0].ws.send).toHaveBeenCalledExactlyOnceWith(JSON.stringify({ type: "HeartBeatRequest" }));
    const pending = socket.request(
      "ResultPaginationRequest",
      { requestID: "r", operatorID: "op", pageIndex: 0, pageSize: 20 },
      "PaginatedResultEvent",
      "r"
    );
    const rejected = expect(pending).rejects.toThrow(/closed/i);
    socket.close();
    await rejected;
    const count = opened[0].ws.send.mock.calls.length;
    await vi.advanceTimersByTimeAsync(30000);
    expect(opened[0].ws.send).toHaveBeenCalledTimes(count);
    expect(opened[0].ws.onmessage).toBeNull();
  });
  it("correlates results and rejects a specific timeout without leaking listeners", async () => {
    vi.useFakeTimers();
    const { socket, opened } = build();
    opened[0].ws.open();
    const result = socket.request(
      "ResultPaginationRequest",
      { requestID: "r", operatorID: "op", pageIndex: 0, pageSize: 20 },
      "PaginatedResultEvent",
      "r",
      100
    );
    opened[0].ws.message({ type: "PaginatedResultEvent", requestID: "other", table: [] });
    opened[0].ws.message({ type: "PaginatedResultEvent", requestID: "r", table: [{ x: "日本語" }] });
    await expect(result).resolves.toMatchObject({ table: [{ x: "日本語" }] });
    const expired = socket.request(
      "ResultPaginationRequest",
      { requestID: "late", operatorID: "op", pageIndex: 0, pageSize: 20 },
      "PaginatedResultEvent",
      "late",
      100
    );
    const rejection = expect(expired).rejects.toThrow(/timed out/i);
    await vi.advanceTimersByTimeAsync(100);
    await rejection;
  });
  it("ignores malformed messages, exposes the error and still handles the next valid message", () => {
    const { socket, opened } = build();
    const events = vi.fn();
    socket.subscribeEvents(events);
    opened[0].ws.open();
    opened[0].ws.onmessage?.({ data: "not json" });
    expect(socket.getSnapshot().error).toContain("Malformed");
    expect(events).not.toHaveBeenCalled();
    opened[0].ws.message({ type: "WorkflowStateEvent", state: "Running" });
    expect(events).toHaveBeenCalledTimes(1);
  });
});

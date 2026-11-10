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
import { afterEach, expect, it, vi } from "vitest";
import { ExecutionControl } from "./execution-control";
import { WorkflowSocket } from "../workspace/workflow-socket";
afterEach(() => vi.useRealTimers());
function setup() {
  const wires: {
    readyState: number;
    onopen: (() => void) | null;
    onmessage: ((event: { data: string }) => void) | null;
    onerror: (() => void) | null;
    onclose: (() => void) | null;
    send: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
  }[] = [];
  const urls: string[] = [];
  const control = new ExecutionControl({
    uid: 7,
    getToken: () => "token",
    timeoutMs: 100,
    createSocket: options =>
      new WorkflowSocket({
        ...options,
        createSocket: url => {
          urls.push(url);
          const wire = {
            readyState: 1,
            onopen: null,
            onmessage: null,
            onerror: null,
            onclose: null,
            send: vi.fn(),
            close: vi.fn(),
          };
          wires.push(wire);
          return wire;
        },
      }),
  });
  return { control, wires, urls };
}
it.each(["WorkflowPauseRequest", "WorkflowResumeRequest", "WorkflowKillRequest"] as const)(
  "waits for the authenticated workflow connection before sending %s once and closing",
  async request => {
    const { control, wires, urls } = setup();
    const send = control.send(42, request);
    expect(wires[0].send).not.toHaveBeenCalled();
    wires[0].onmessage?.({ data: JSON.stringify({ type: "ClusterStatusUpdateEvent", numWorkers: 1 }) });
    await send;
    expect(wires[0].send).toHaveBeenCalledWith(JSON.stringify({ type: request }));
    expect(wires[0].close).toHaveBeenCalledOnce();
    expect(new URL(urls[0]).searchParams.get("wid")).toBe("42");
    expect(new URL(urls[0]).searchParams.get("access-token")).toBe("token");
    control.dispose();
  }
);
it("rejects timed-out, refused, invalid and disposed controls and cleans every connection", async () => {
  vi.useFakeTimers();
  const { control, wires } = setup();
  const timed = control.send(42, "WorkflowKillRequest").catch(error => error);
  await vi.advanceTimersByTimeAsync(100);
  expect((await timed).message).toMatch(/timed out/i);
  expect(wires[0].close).toHaveBeenCalledOnce();
  const refused = control.send(43, "WorkflowPauseRequest").catch(error => error);
  wires[1].onclose?.();
  expect((await refused).message).toMatch(/refused/i);
  const pending = control.send(44, "WorkflowKillRequest").catch(error => error);
  control.dispose();
  expect((await pending).message).toMatch(/closed/i);
  await expect(control.send(42, "WorkflowKillRequest")).rejects.toThrow(/closed/i);
  const fresh = setup();
  await expect(fresh.control.send(NaN, "WorkflowKillRequest")).rejects.toThrow(/identifier/i);
  fresh.control.dispose();
});

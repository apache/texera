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
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { ResultsPanel } from "./results-panel";
import { ApiClient } from "../core/api-client";
import type { WorkflowRuntime, RuntimeSnapshot } from "../workspace/workflow-runtime";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { GuiConfig } from "../../core/common/type/gui-config";
import { ExecutionState } from "../../core/workspace/types/execute-workflow.interface";
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
function build() {
  let snapshot = {
    state: "Completed",
    duration: 1,
    errors: [],
    console: {},
    operators: {},
    results: { a: { mode: { type: "PaginationMode" }, totalNumTuples: 12, dirtyPageIndices: [] } },
    tableStats: {},
    connection: { status: "connected" },
  };
  const listeners = new Set<() => void>();
  const request = vi.fn(
    async (
      _type: string,
      payload: { portIndex?: number; operatorID: string; pageIndex: number },
      _responseType?: string,
      _requestId?: string,
      _timeoutMs?: number,
      _signal?: AbortSignal
    ) => ({
      type: "PaginatedResultEvent",
      ...payload,
      portIndex: payload.portIndex ?? 0,
      totalTuples: 12,
      table: [{ first: "hello", second: null }],
      schema: [
        { attributeName: "first", attributeType: "string" },
        { attributeName: "second", attributeType: "string" },
      ],
    })
  );
  const runtime = {
    subscribe: (f: () => void) => {
      listeners.add(f);
      return () => listeners.delete(f);
    },
    getSnapshot: () => snapshot,
    socket: { request, send: vi.fn() },
    skip: vi.fn(),
    retry: vi.fn(),
    canControl: vi.fn(() => true),
  } as unknown as WorkflowRuntime;
  const workflow = {
    wid: 7,
    name: "Test",
    content: {
      operators: [
        {
          operatorID: "a",
          customDisplayName: "First",
          operatorType: "Test",
          outputPorts: [
            { portID: "0", displayName: "Data" },
            { portID: "1", displayName: "Other" },
          ],
        },
        { operatorID: "b", customDisplayName: "Second", operatorType: "Test", outputPorts: [{ portID: "0" }] },
      ],
      links: [],
    },
  };
  const sessionSnapshot = { workflow };
  const session = { getSnapshot: () => sessionSnapshot, subscribe: () => () => {} } as unknown as WorkflowSession;
  const config = {
    limitColumns: 1,
    exportExecutionResultEnabled: false,
    workflowExecutionsTrackingEnabled: false,
  } as GuiConfig;
  return {
    runtime,
    session,
    config,
    request,
    update: (patch: Partial<RuntimeSnapshot>) =>
      act(() => {
        snapshot = { ...snapshot, ...patch };
        listeners.forEach(f => f());
      }),
  };
}
describe("React results", () => {
  it("withholds debugger controls for READ computing units and checks permission again before delivery", async () => {
    const props = build();
    Object.assign(props.session, { shared: { debugState: new Map() } });
    props.update({ state: ExecutionState.Paused, workers: { a: ["w-1"] } });
    vi.mocked(props.runtime.canControl).mockReturnValue(false);
    const view = render(<ResultsPanel {...props} />);
    fireEvent.click(screen.getByRole("tab", { name: /Console/ }));
    expect(screen.queryByRole("button", { name: "Step", exact: true })).toBeNull();
    expect(props.runtime.socket.send).not.toHaveBeenCalled();
    view.unmount();
    vi.mocked(props.runtime.canControl).mockReturnValue(true);
    render(<ResultsPanel {...props} />);
    fireEvent.click(screen.getByRole("tab", { name: /Console/ }));
    const step = screen.getByRole("button", { name: "Step", exact: true });
    vi.mocked(props.runtime.canControl).mockReturnValue(false);
    fireEvent.click(step);
    await screen.findByText("Write access to the computing unit is required to debug.");
    expect(props.runtime.socket.send).not.toHaveBeenCalled();
  });
  it("clears debugger hit markers after successful next and retains them if command delivery fails", async () => {
    const props = build();
    const debug = new Map([
      ["1", { breakpointId: 1, condition: "x", hit: true }],
      ["2", { breakpointId: undefined, condition: "", hit: true }],
    ]);
    Object.assign(props.session, { shared: { debugState: new Map([["a", debug]]) } });
    props.update({ state: ExecutionState.Paused, workers: { a: ["w-1"] } });
    render(<ResultsPanel {...props} />);
    fireEvent.click(screen.getByRole("tab", { name: /Console/ }));
    fireEvent.click(screen.getByRole("button", { name: "Step", exact: true }));
    expect(debug.get("1")?.hit).toBe(false);
    expect(debug.has("2")).toBe(false);
    debug.set("1", { breakpointId: 1, condition: "x", hit: true });
    vi.mocked(props.runtime.socket.send).mockImplementationOnce(() => {
      throw new Error("Disconnected");
    });
    fireEvent.click(screen.getByRole("button", { name: "Continue", exact: true }));
    expect(debug.get("1")?.hit).toBe(true);
    await screen.findByText("Disconnected");
  });
  it("retains visible page across workspace view remounts without a row inspector moving it", async () => {
    const props = build();
    const first = render(<ResultsPanel {...props} />);
    await screen.findByText("hello");
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByText("Page 2 of 3 · 12 rows");
    first.unmount();
    render(<ResultsPanel {...props} />);
    await waitFor(() => expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("pageIndex", 2));
    fireEvent.click(await screen.findByRole("button", { name: "View row 6" }));
    await screen.findByRole("dialog", { name: "Row details" });
    fireEvent.click(screen.getByRole("button", { name: "Previous row" }));
    await waitFor(() => expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("pageIndex", 1));
    fireEvent.click(screen.getByRole("button", { name: "Close", exact: true }));
    expect(screen.getByText("Page 2 of 3 · 12 rows")).toBeTruthy();
  });
  it("keeps an in-flight visible page alive through unrelated dirty-page updates", async () => {
    const props = build();
    let resolve!: (value: unknown) => void;
    props.request.mockImplementationOnce(
      () =>
        new Promise(r => {
          resolve = r;
        })
    );
    render(<ResultsPanel {...props} />);
    props.update({ results: { a: { mode: { type: "PaginationMode" }, totalNumTuples: 12, dirtyPageIndices: [2] } } });
    await act(async () =>
      resolve({
        operatorID: "a",
        portIndex: 0,
        pageIndex: 1,
        totalTuples: 12,
        table: [{ kept: "Still current" }],
        schema: [],
      })
    );
    await screen.findByText("Still current");
    expect(props.request).toHaveBeenCalledTimes(1);
  });
  it("cancels superseded row requests and leaves the visible table untouched", async () => {
    const props = build();
    render(<ResultsPanel {...props} />);
    await screen.findByText("hello");
    const response = await props.request.mock.results[0].value;
    type Page = Awaited<ReturnType<typeof props.request>>;
    let resolvePrevious!: (page: Page) => void;
    props.request.mockImplementationOnce(
      () =>
        new Promise(resolve => {
          resolvePrevious = resolve;
        })
    );
    fireEvent.click(screen.getByRole("button", { name: "View row 1" }));
    const dialog = within(screen.getByRole("dialog", { name: "Row details" }));
    const previous = props.request.mock.calls.at(-1)?.[5];
    expect(previous?.aborted).toBe(false);

    props.request.mockResolvedValueOnce({
      ...response,
      table: [
        { first: "other", second: null },
        { first: "current row", second: null },
      ],
    });
    fireEvent.click(dialog.getByRole("button", { name: "Next row" }));
    const current = props.request.mock.calls.at(-1)?.[5];
    expect(previous?.aborted).toBe(true);
    expect(current?.aborted).toBe(false);
    await dialog.findByText("current row");

    await act(async () => resolvePrevious({ ...response, table: [{ first: "stale row", second: null }] }));
    expect(dialog.queryByText("stale row")).toBeNull();
    expect(dialog.queryByRole("alert")).toBeNull();
    fireEvent.click(dialog.getByRole("button", { name: "Close" }));
    expect(current?.aborted).toBe(true);
    expect(screen.getByText("hello")).toBeTruthy();
  });
  it("aborts row media on navigation or close and revokes each displayed blob URL", async () => {
    const props = build();
    const mediaUrls = ["first", "second", "third"].map(name => `https://host/${name}.mp4`);
    props.request.mockImplementation(async (type, payload) => ({
      type,
      ...payload,
      portIndex: 0,
      totalTuples: mediaUrls.length,
      schema: [{ attributeName: "first", attributeType: "string" }],
      table: mediaUrls.map(first => ({ first, second: null })),
    }));
    const requests: { signal?: AbortSignal | null; resolve: (response: Response) => void }[] = [];
    const fetcher = vi.fn<typeof fetch>(
      (_input, init) => new Promise(resolve => requests.push({ signal: init?.signal, resolve }))
    );
    const createObjectURL = vi.fn(() => "blob:current");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal(
      "URL",
      class extends URL {
        static createObjectURL = createObjectURL;
        static revokeObjectURL = revokeObjectURL;
      }
    );
    const view = render(
      <ResultsPanel
        {...props}
        api={new ApiClient({ fetch: fetcher, getToken: () => null })}
      />
    );
    fireEvent.click(await screen.findByRole("button", { name: "View row 1" }));
    const dialog = within(screen.getByRole("dialog", { name: "Row details" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0].signal?.aborted).toBe(false);
    fireEvent.click(dialog.getByRole("button", { name: "Next row" }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[0].signal?.aborted).toBe(true);
    expect(requests[1].signal?.aborted).toBe(false);

    const videoResponse = () => new Response("video", { headers: { "Content-Type": "video/mp4" } });
    await act(async () => {
      requests[0].resolve(videoResponse());
      requests[1].resolve(videoResponse());
    });
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(view.container.querySelector("video")?.getAttribute("src")).toBe("blob:current");

    fireEvent.click(dialog.getByRole("button", { name: "Next row" }));
    await waitFor(() => expect(requests).toHaveLength(3));
    expect(revokeObjectURL).toHaveBeenCalledExactlyOnceWith("blob:current");
    fireEvent.click(dialog.getByRole("button", { name: "Close" }));
    expect(requests[2].signal?.aborted).toBe(true);
    await act(async () => requests[2].resolve(videoResponse()));
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(view.container.querySelector("video")).toBeNull();
  });
  it("exports to writable dataset selections using deployment settings and exact server payload", async () => {
    const props = build();
    props.config.exportExecutionResultEnabled = true;
    const fetcher = vi.fn(
      async (path: string) =>
        new Response(
          JSON.stringify(
            path.endsWith("dataset/list")
              ? [
                  { accessPrivilege: "READ", dataset: { did: 1, name: "Read only" } },
                  { accessPrivilege: "WRITE", dataset: { did: 2, name: "Writable" } },
                ]
              : path.includes("downloadability")
                ? {}
                : { status: "success" }
          )
        )
    );
    render(
      <ResultsPanel
        {...props}
        api={new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null })}
        exportContext={{ computingUnitId: 9, computingUnitType: "kubernetes" }}
      />
    );
    await screen.findByText("hello");
    fireEvent.click(screen.getByRole("button", { name: "Export results" }));
    fireEvent.change(screen.getByLabelText("Destination"), { target: { value: "dataset" } });
    await screen.findByLabelText("Writable");
    expect(screen.queryByLabelText("Read only")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Create new dataset" }));
    await screen.findByLabelText("Resource name");
    fireEvent.click(screen.getByRole("button", { name: "Cancel dataset creation" }));
    expect(fetcher.mock.calls.some(([path]) => path.includes("dataset/create"))).toBe(false);
    fireEvent.click(screen.getByLabelText("Writable"));
    fireEvent.click(screen.getByRole("button", { name: "Export", exact: true }));
    await screen.findByText("Result exported successfully.");
    const call = fetcher.mock.calls.find(([path]) => path.includes("result/export/dataset"));
    expect(call?.[0]).toBe("/api/executions/result/export/dataset?cuid=9");
    expect(JSON.parse(String((call as unknown as [string, RequestInit])?.[1].body))).toEqual(
      expect.objectContaining({ datasetIds: [2], computingUnitId: 9, operators: [{ id: "a", outputType: "csv" }] })
    );
  });
  it("requests selected output port/page/column slices and preserves null in row rendering", async () => {
    const props = build();
    render(<ResultsPanel {...props} />);
    await screen.findByText("hello");
    expect(props.request.mock.calls[0][1]).toEqual(
      expect.objectContaining({ pageIndex: 1, pageSize: 5, columnLimit: 1 })
    );
    expect(screen.getByText("null")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Next columns" }));
    await waitFor(() => expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("columnOffset", 1));
    fireEvent.change(screen.getByLabelText("Output port"), { target: { value: "1" } });
    await waitFor(() => expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("portIndex", 1));
    expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("columnOffset", 0);
    fireEvent.change(screen.getByLabelText("Search columns"), { target: { value: "ü" } });
    await waitFor(() => expect(props.request.mock.calls.at(-1)?.[1]).toHaveProperty("columnSearch", "ü"));
  });
  it("ignores late response after a port switch and reports request failures without stale rows", async () => {
    const props = build();
    let resolve!: (value: unknown) => void;
    props.request.mockImplementationOnce(
      () =>
        new Promise(r => {
          resolve = r;
        })
    );
    render(<ResultsPanel {...props} />);
    fireEvent.change(screen.getByLabelText("Output port"), { target: { value: "1" } });
    await screen.findByText("hello");
    await act(async () =>
      resolve({ operatorID: "a", portIndex: 0, pageIndex: 1, table: [{ wrong: "stale" }], schema: [], totalTuples: 9 })
    );
    expect(screen.queryByText("stale")).toBeNull();
    props.request.mockRejectedValueOnce(new Error("Disconnected"));
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    await screen.findByText("Disconnected");
    expect(screen.queryByText("hello")).toBeNull();
  });
  it("renders empty-result schema and refetches a dirty visible first-port page", async () => {
    const props = build();
    props.request.mockResolvedValue({
      operatorID: "a",
      portIndex: 0,
      pageIndex: 1,
      totalTuples: 0,
      table: [],
      schema: [{ attributeName: "empty", attributeType: "string" }],
    } as never);
    render(<ResultsPanel {...props} />);
    await screen.findByRole("columnheader", { name: "empty" });
    props.update({ results: { a: { mode: { type: "PaginationMode" }, totalNumTuples: 0, dirtyPageIndices: [1] } } });
    await waitFor(() => expect(props.request).toHaveBeenCalledTimes(2));
  });
  it("uses latest materialized chart snapshot in an isolated scripted iframe and clears empty charts", () => {
    const props = build();
    props.update({
      results: { a: { mode: { type: "SetDeltaMode" }, table: [] } },
      materializedResults: { a: [{ "html-content": "<script>chart()</script><div>graph</div>" }] },
    });
    const { rerender } = render(<ResultsPanel {...props} />);
    const iframe = screen.getByTitle("Visualization for First");
    expect(iframe.getAttribute("srcdoc")).toContain("<script>chart()</script>");
    expect(iframe.getAttribute("sandbox")).toContain("allow-scripts");
    expect(iframe.getAttribute("sandbox")).not.toContain("allow-same-origin");
    props.update({ materializedResults: { a: [] } });
    rerender(<ResultsPanel {...props} />);
    expect(screen.queryByTitle("Visualization for First")).toBeNull();
  });
  it("retains console source, unknown message types, filtering, and safe fatal error details", async () => {
    const props = build();
    props.update({
      console: {
        a: [
          {
            workerId: "w-2",
            timestamp: { seconds: 1, nanos: 0 },
            msgType: { name: "ERROR" },
            source: "Python",
            title: "Oops",
            message: "<script>evil</script>",
          },
          {
            workerId: "",
            timestamp: { seconds: 2, nanos: 0 },
            msgType: { name: "CUSTOM" },
            source: "Engine",
            title: "Notice",
            message: "",
          },
        ],
      },
      errors: [
        {
          operatorId: "b",
          workerId: "w",
          type: { name: "Compilation" },
          message: "Bad schema",
          details: "<img onerror=evil>",
          timestamp: { seconds: 1, nanos: 0 },
        },
      ],
    });
    const onSelectOperator = vi.fn();
    render(
      <ResultsPanel
        {...props}
        onSelectOperator={onSelectOperator}
      />
    );
    fireEvent.click(screen.getByRole("tab", { name: /Console/ }));
    expect(screen.getByText("Notice")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Show Error messages"));
    expect(screen.queryByText("Oops")).toBeNull();
    expect(screen.getByText("Hidden by filter: 1")).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: /Errors/ }));
    expect(screen.getByText(/Bad schema/)).toBeTruthy();
    expect(document.querySelector("img[onerror]")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Focus operator b" }));
    expect(onSelectOperator).toHaveBeenCalledWith("b");
  });
  it("loads remote row media through the authenticated proxy and keeps text on proxy denial", async () => {
    const props = build();
    props.request.mockResolvedValue({
      operatorID: "a",
      portIndex: 0,
      pageIndex: 1,
      totalTuples: 1,
      table: [{ media: "https://host/movie.mp4" }],
      schema: [{ attributeName: "media", attributeType: "string" }],
    } as never);
    const fetcher = vi.fn(async () => new Response("Denied", { status: 403 }));
    const api = new ApiClient({ fetch: fetcher, getToken: () => "token" });
    render(
      <ResultsPanel
        {...props}
        api={api}
      />
    );
    await screen.findByText("Play Video");
    fireEvent.click(screen.getByRole("button", { name: "View row 1" }));
    await screen.findByRole("dialog", { name: "Row details" });
    await waitFor(() => expect(fetcher).toHaveBeenCalled());
    expect(fetcher.mock.calls[0][0]).toBe("/api/huggingface/media-proxy?url=https%3A%2F%2Fhost%2Fmovie.mp4");
    await waitFor(() => expect(screen.getByText("https://host/movie.mp4")).toBeTruthy());
    expect(document.querySelector("video")).toBeNull();
  });
});

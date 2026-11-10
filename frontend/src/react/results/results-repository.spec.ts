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

import { describe, expect, it, vi } from "vitest";
import { ResultsRepository, ALL_COLUMNS, resultColumns, valueText, visualizationDocument } from "./results-repository";
import { ApiClient } from "../core/api-client";
import type { WorkflowSocket } from "../workspace/workflow-socket";

function build() {
  const request = vi.fn(async (_type, payload) => ({
    type: "PaginatedResultEvent",
    ...payload,
    portIndex: payload.portIndex ?? 0,
    table: [],
    schema: [{ attributeName: "empty", attributeType: "string" }],
    totalTuples: 0,
  }));
  const fetcher = vi.fn(
    async () => new Response(JSON.stringify({}), { headers: { "Content-Type": "application/json" } })
  );
  const api = new ApiClient({ fetch: fetcher, getToken: () => null });
  return { request, fetcher, repository: new ResultsRepository({ request } as unknown as WorkflowSocket, api) };
}
describe("result protocol and export contracts", () => {
  it("supports HTTP deployments without the secure-context randomUUID convenience API", async () => {
    const original = globalThis.crypto;
    vi.stubGlobal("crypto", { getRandomValues: original.getRandomValues.bind(original) });
    try {
      const { repository, request } = build();
      await repository.page("op", { pageIndex: 1, pageSize: 5 });
      expect(request.mock.calls[0][1].requestID).toMatch(/^[0-9a-f-]{36}$/);
    } finally {
      vi.unstubAllGlobals();
    }
  });
  it("rejects malformed row/schema payloads before rendering and snapshots local download form fields", async () => {
    const { repository, request } = build();
    request.mockResolvedValueOnce({
      operatorID: "op",
      portIndex: 0,
      pageIndex: 1,
      table: [null],
      schema: [],
      totalTuples: 1,
    } as never);
    await expect(repository.page("op", { pageIndex: 1, pageSize: 5 })).rejects.toThrow("Malformed");
    request.mockResolvedValueOnce({
      operatorID: "op",
      portIndex: 0,
      pageIndex: 1,
      table: [],
      schema: [{}],
      totalTuples: 0,
    } as never);
    await expect(repository.page("op", { pageIndex: 1, pageSize: 5 })).rejects.toThrow("Malformed");
    const submit = vi.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {});
    vi.useFakeTimers();
    try {
      const body = {
        exportType: "arrow",
        workflowId: 7,
        workflowName: "Üser",
        operators: [{ id: "a", outputType: "arrow" }],
        datasetIds: [],
        rowIndex: -1,
        columnIndex: -1,
        filename: "r",
        computingUnitId: 9,
      };
      repository.exportToLocal(body, "kubernetes", () => "test-token");
      const form = document.querySelector("form")!;
      expect(form.getAttribute("action")).toBe("/api/executions/result/export/local?cuid=9");
      expect((form.querySelector('[name="token"]') as HTMLInputElement).value).toBe("test-token");
      expect(JSON.parse((form.querySelector('[name="request"]') as HTMLInputElement).value)).toEqual(body);
      expect(submit).toHaveBeenCalledOnce();
      vi.advanceTimersByTime(10000);
      expect(document.querySelector("form")).toBeNull();
      expect(document.querySelector("iframe")).toBeNull();
    } finally {
      vi.useRealTimers();
      submit.mockRestore();
    }
  });
  it("preserves Arrow and Parquet formats and rejects unsupported JSON export", async () => {
    const { repository } = build();
    const input = {
      workflowId: 7,
      workflowName: "Run",
      operatorIds: ["a"],
      format: "arrow",
      filename: "r",
      computingUnitId: 9,
    };
    expect((await repository.prepareExport(input)).request.operators).toEqual([{ id: "a", outputType: "arrow" }]);
    expect((await repository.prepareExport({ ...input, format: "parquet" })).request.exportType).toBe("parquet");
    await expect(repository.prepareExport({ ...input, format: "json" })).rejects.toThrow("format");
  });
  it("uses bounded visible column slices, first port omission, and non-first-port independent replies", async () => {
    const { repository, request } = build();
    await repository.page("op", { pageIndex: 2, pageSize: 5, columnOffset: 25, columnLimit: 25, columnSearch: "ü" });
    expect(request.mock.calls[0][0]).toBe("ResultPaginationRequest");
    expect(request.mock.calls[0][1]).toEqual(
      expect.objectContaining({
        operatorID: "op",
        pageIndex: 2,
        pageSize: 5,
        columnOffset: 25,
        columnLimit: 25,
        columnSearch: "ü",
      })
    );
    expect(request.mock.calls[0][1]).not.toHaveProperty("portIndex");
    await repository.page("op", { portIndex: 2, pageIndex: 1, pageSize: 5, columnLimit: 25 });
    expect(request.mock.calls[1][1]).toHaveProperty("portIndex", 2);
  });
  it("requests every row-detail column with the server Int maximum and validates malformed navigation", async () => {
    const { repository, request } = build();
    await repository.row("op", 11, 5, 1);
    expect(request.mock.calls[0][1]).toEqual(
      expect.objectContaining({ pageIndex: 3, pageSize: 5, columnLimit: 2147483647, portIndex: 1 })
    );
    expect(ALL_COLUMNS).toBe(2147483647);
    await expect(repository.page("op", { pageIndex: 0, pageSize: 5 })).rejects.toThrow("page");
    await expect(repository.page("", { pageIndex: 1, pageSize: 5 })).rejects.toThrow("operator");
    await expect(repository.row("op", -1, 5)).rejects.toThrow("row");
    expect(request).toHaveBeenCalledTimes(1);
  });
  it("rejects replies for another operator/port instead of contaminating the view", async () => {
    const { repository, request } = build();
    request.mockResolvedValueOnce({
      operatorID: "other",
      portIndex: 0,
      pageIndex: 1,
      table: [],
      schema: [],
      totalTuples: 0,
    } as never);
    await expect(repository.page("op", { pageIndex: 1, pageSize: 5 })).rejects.toThrow("different");
  });
  it("retains empty-result schema, hides row ID, and safely displays null and nested data", () => {
    expect(resultColumns([], [{ attributeName: "ü", attributeType: "string" }])).toEqual(["ü"]);
    expect(resultColumns([{ _id: 2, a: null, b: { c: true } }], [])).toEqual(["a", "b"]);
    expect(valueText(null)).toBe("null");
    expect(valueText(undefined)).toBe("");
    expect(valueText({ a: "ü" })).toBe('{"a":"ü"}');
    expect(
      visualizationDocument([{ "html-content": "old" }, { "html-content": "<script>chart()</script>" }])
    ).toContain("<script>chart()</script>");
    expect(visualizationDocument([])).toBe("");
  });
  it("filters restricted export scope using the actual dependency API and snapshots scope before awaiting", async () => {
    const { repository, fetcher } = build();
    fetcher.mockResolvedValueOnce(new Response(JSON.stringify({ blocked: ["Dataset (owner@test.org)"] })));
    const ids = ["blocked", "allowed"];
    const pending = repository.prepareExport({
      workflowId: 7,
      workflowName: "Run",
      operatorIds: ids,
      format: "csv",
      filename: "r",
      computingUnitId: 9,
      outputTypes: { allowed: "html" },
    });
    ids.push("later");
    expect(await pending).toEqual(
      expect.objectContaining({
        request: expect.objectContaining({
          workflowId: 7,
          operators: [{ id: "allowed", outputType: "html" }],
          datasetIds: [],
          rowIndex: -1,
          columnIndex: -1,
          computingUnitId: 9,
        }),
        blocked: ["Dataset (owner@test.org)"],
      })
    );
    expect(fetcher.mock.calls[0][0]).toBe("/api/executions/7/result/downloadability");
  });
  it("rejects fully restricted/invalid export and propagates permission errors", async () => {
    const { repository, fetcher } = build();
    const input = {
      workflowId: 7,
      workflowName: "Run",
      operatorIds: ["blocked"],
      format: "csv",
      filename: "r",
      computingUnitId: 9,
    };
    fetcher.mockResolvedValueOnce(new Response(JSON.stringify({ blocked: ["Dataset"] })));
    await expect(repository.prepareExport(input)).rejects.toThrow("not downloadable");
    await expect(repository.prepareExport({ ...input, computingUnitId: -1 })).rejects.toThrow("computing");
    fetcher.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));
    await expect(repository.prepareExport(input)).rejects.toThrow("Forbidden");
  });
  it("posts the exact dataset export shape and rejects server-declared failure", async () => {
    const { repository, fetcher } = build();
    fetcher.mockResolvedValueOnce(new Response(JSON.stringify({ status: "success" })));
    const request = {
      exportType: "csv",
      workflowId: 7,
      workflowName: "Run",
      operators: [{ id: "a", outputType: "csv" }],
      datasetIds: [2],
      rowIndex: -1,
      columnIndex: -1,
      filename: "r",
      computingUnitId: 9,
    };
    await repository.exportToDataset(request, "kubernetes");
    expect(fetcher.mock.calls[0][0]).toBe("/api/executions/result/export/dataset?cuid=9");
    expect(JSON.parse(String(fetcher.mock.calls[0][1].body))).toEqual(request);
    fetcher.mockResolvedValueOnce(new Response(JSON.stringify({ status: "error", message: "Denied" })));
    await expect(repository.exportToDataset(request)).rejects.toThrow("Denied");
  });
});

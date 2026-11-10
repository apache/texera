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
import { ApiClient } from "../core/api-client";
import { NotebookClient, notebookHighlight } from "./notebook-client";
function build(enabled = true) {
  const fetcher = vi.fn(async (path: string) => {
    const value = path.endsWith("fetch-notebook-and-mapping")
      ? {
          exists: true,
          mapping: { cell_to_operator: { cell: ["a", "b"] }, operator_to_cell: { a: ["cell"] } },
          notebook: { cells: [] },
        }
      : path.includes("get-jupyter-iframe-url")
        ? { success: true, url: "https://jupyter.test/notebooks/notebook_7.ipynb" }
        : path.endsWith("get-jupyter-url")
          ? { success: true, url: "https://jupyter.test/" }
          : { success: true };
    return Response.json(value);
  });
  return {
    fetcher,
    client: new NotebookClient(new ApiClient({ fetch: fetcher as typeof fetch, getToken: () => null }), enabled),
  };
}
describe("notebook panel contract", () => {
  it("fetches stored mapping, uploads per-workflow notebook and resolves iframe+origin", async () => {
    const { client, fetcher } = build();
    await client.load(7);
    expect(client.getSnapshot()).toEqual(
      expect.objectContaining({
        exists: true,
        visible: true,
        url: "https://jupyter.test/notebooks/notebook_7.ipynb",
        origin: "https://jupyter.test",
      })
    );
    expect(fetcher.mock.calls.map(([path]) => path)).toEqual([
      "/api/notebook-migration/fetch-notebook-and-mapping",
      "/api/notebook-migration/set-notebook",
      "/api/notebook-migration/get-jupyter-iframe-url?notebookName=notebook_7.ipynb",
      "/api/notebook-migration/get-jupyter-url",
    ]);
    expect(JSON.parse(String((fetcher.mock.calls[1] as unknown as [string, RequestInit])[1].body))).toEqual({
      notebookName: "notebook_7.ipynb",
      notebookData: { cells: [] },
    });
    client.minimize();
    expect(client.getSnapshot().visible).toBe(false);
    client.open();
    expect(client.getSnapshot().visible).toBe(true);
  });
  it("clears previous state on workflow switch without deleting server notebooks; disabled/unsaved performs no request", async () => {
    const { client, fetcher } = build();
    await client.load(7);
    await client.load(undefined);
    expect(client.getSnapshot()).toEqual(expect.objectContaining({ exists: false, visible: false }));
    expect(client.getSnapshot().url).toBeUndefined();
    expect(fetcher.mock.calls.some(([path]) => path.includes("delete"))).toBe(false);
    const disabled = build(false);
    await disabled.client.load(7);
    expect(disabled.fetcher).not.toHaveBeenCalled();
  });
  it("rejects malformed mapping/foreign iframe origin and leaves notebook present after failed delete", async () => {
    const { client, fetcher } = build();
    await client.load(7);
    fetcher.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));
    await expect(client.delete()).rejects.toThrow("Forbidden");
    expect(client.getSnapshot().exists).toBe(true);
    fetcher.mockResolvedValueOnce(Response.json({ exists: true, mapping: { bad: 1 }, notebook: { cells: [] } }));
    await expect(client.load(8)).rejects.toThrow("mapping");
    const other = build();
    other.fetcher.mockImplementation(async (path: string) =>
      Response.json(
        path.endsWith("fetch-notebook-and-mapping")
          ? { exists: true, mapping: { cell_to_operator: {}, operator_to_cell: {} }, notebook: { cells: [] } }
          : path.includes("get-jupyter-iframe")
            ? { success: true, url: "https://foreign.test/" }
            : path.endsWith("get-jupyter-url")
              ? { success: true, url: "https://jupyter.test" }
              : { success: true }
      )
    );
    await expect(other.client.load(7)).rejects.toThrow("origin");
  });
  it("removes database mapping before best-effort pod cleanup using captured workflow ID", async () => {
    const { client, fetcher } = build();
    await client.load(7);
    await client.delete();
    const calls = fetcher.mock.calls.filter(([path]) => path.includes("delete"));
    expect(calls.map(([path]) => path)).toEqual([
      "/api/notebook-migration/delete-notebook-and-mapping",
      "/api/notebook-migration/delete-notebook",
    ]);
    expect(JSON.parse(String((calls[0] as unknown as [string, RequestInit])[1].body))).toEqual({ wid: 7 });
    expect(JSON.parse(String((calls[1] as unknown as [string, RequestInit])[1].body))).toEqual({
      notebookName: "notebook_7.ipynb",
    });
    expect(client.getSnapshot().exists).toBe(false);
  });
  it("only highlights currently mapped existing operators and internal links", () => {
    const mapping = { cell_to_operator: { cell: ["a", "b", "missing"] }, operator_to_cell: {} };
    const workflow = {
      operators: [{ operatorID: "a" }, { operatorID: "b" }],
      links: [
        { linkID: "inside", source: { operatorID: "a" }, target: { operatorID: "b" } },
        { linkID: "outside", source: { operatorID: "a" }, target: { operatorID: "c" } },
      ],
    };
    expect(notebookHighlight(mapping, "cell", workflow as unknown as Parameters<typeof notebookHighlight>[2])).toEqual({
      operators: ["a", "b"],
      links: ["inside"],
    });
    expect(
      notebookHighlight(mapping, "unknown", workflow as unknown as Parameters<typeof notebookHighlight>[2])
    ).toEqual({ operators: [], links: [] });
  });
});

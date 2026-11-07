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
import JSZip from "jszip";
import { ApiClient } from "../core/api-client";
import { WorkflowRepository, emptyWorkflow, parseImportedWorkflow } from "./workflow-repository";

const content = {
  operators: [],
  links: [],
  commentBoxes: [],
  operatorPositions: {},
  settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
};
const record = {
  wid: 4,
  name: "日本語",
  content: JSON.stringify(content),
  description: "notes",
  isPublic: true,
  defaultView: "FORM",
};
const entry = {
  workflow: record,
  isOwner: true,
  ownerId: 7,
  ownerName: "owner",
  accessLevel: "WRITE",
  coverImage: null,
};
function setup(response: (path: string, init?: RequestInit) => Response = () => Response.json(entry)) {
  const fetch = vi.fn(async (path: string, init?: RequestInit) => response(path, init));
  return { repository: new WorkflowRepository(new ApiClient({ fetch })), fetch };
}

describe("WorkflowRepository", () => {
  it("normalizes stored JSON content and publication fields without mutating server payloads", async () => {
    const { repository } = setup(() => Response.json(record));
    const workflow = await repository.get(4);
    expect(workflow).toMatchObject({ content, isPublished: 1, defaultView: "FORM" });
    expect(record.content).toBeTypeOf("string");
    await expect(repository.get(0)).rejects.toThrow("workflow ID");
  });

  it("uses paged dashboard search, server-side sort, repeated filters and local date boundaries", async () => {
    const { repository, fetch } = setup(() =>
      Response.json({
        results: [{ resourceType: "workflow", workflow: entry }, { resourceType: "unknown" }],
        more: true,
      })
    );
    const result = await repository.search({
      keywords: [" café ", "λ"],
      owners: ["a+b@example.org"],
      ids: ["4"],
      operators: ["CSVScan"],
      createDateStart: "2026-11-01",
      orderBy: "NameAsc",
      start: 20,
      count: 10,
    });
    const query = new URL(fetch.mock.calls[0][0], "https://texera.example").searchParams;
    expect(query.getAll("query")).toEqual(["café", "λ"]);
    expect(query.get("owner")).toBe("a+b@example.org");
    expect(query.get("resourceType")).toBe("workflow");
    expect(query.get("orderBy")).toBe("NameAsc");
    expect(query.get("start")).toBe("20");
    expect(query.get("createDateStart")).toBe("2026-11-01");
    expect(result).toMatchObject({ more: true, nextStart: 22, entries: [{ workflow: { wid: 4 } }] });
  });

  it("creates real empty workflows with configured settings, preserves form default and snapshots saves", async () => {
    const { repository, fetch } = setup((_path, init) => {
      const body = JSON.parse(String(init?.body));
      return Response.json({ ...entry, workflow: { ...record, content: body.content ?? record.content } });
    });
    const fresh = emptyWorkflow({ defaultDataTransferBatchSize: 800, defaultExecutionMode: "MATERIALIZED" });
    const created = await repository.create(fresh, " flow ", "FORM");
    expect(created.workflow.content.settings).toEqual({ dataTransferBatchSize: 800, executionMode: "MATERIALIZED" });
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({
      name: " flow ",
      content: JSON.stringify(fresh),
      defaultView: "FORM",
    });
    await expect(repository.create(fresh, " ")).rejects.toThrow("name");
  });

  it("preserves batch mutation contracts and rejects empty or malformed ID collections", async () => {
    const { repository, fetch } = setup(path => Response.json(path.endsWith("duplicate") ? [entry] : {}));
    await repository.duplicate([4, 4]);
    await repository.delete([4]);
    await repository.rename(4, "new");
    await repository.updateDescription(4, "");
    await repository.setPublished(4, true);
    await repository.setPublished(4, false);
    await repository.setDefaultView(4, "FORM");
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      "/api/workflow/duplicate",
      "/api/workflow/delete",
      "/api/workflow/update/name",
      "/api/workflow/update/description",
      "/api/workflow/public/4",
      "/api/workflow/private/4",
      "/api/workflow/set-default-view/4",
    ]);
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({ wids: [4] });
    expect(JSON.parse(String(fetch.mock.calls[6][1]?.body))).toEqual({ view: "FORM" });
    await expect(repository.delete([])).rejects.toThrow("Select");
    await expect(repository.duplicate([NaN])).rejects.toThrow("workflow ID");
  });

  it("validates imported JSON, preserves form configuration and never stores defaultView inside content", async () => {
    const { repository, fetch } = setup();
    const document = { ...content, formBinding: { fields: [] }, defaultView: "FORM" };
    await repository.importJson(JSON.stringify(document), "analysis.json");
    const body = JSON.parse(String(fetch.mock.calls[0][1]?.body));
    expect(body.name).toBe("analysis");
    expect(body.defaultView).toBe("FORM");
    expect(JSON.parse(body.content)).toEqual({ ...content, formBinding: { fields: [] } });
    for (const invalid of ["null", "[]", "{}", "{broken", JSON.stringify({ ...content, links: null })]) {
      expect(() => parseImportedWorkflow(invalid)).toThrow();
    }
    expect(parseImportedWorkflow(JSON.stringify(content)).defaultView).toBeUndefined();
  });

  it("imports ZIP entries and exports collision-safe ZIP filenames with round-trippable JSON", async () => {
    const { repository, fetch } = setup(path => Response.json(path === "/api/workflow/create" ? entry : record));
    const zip = new JSZip()
      .file("first.json", JSON.stringify(content))
      .file("folder/second.json", JSON.stringify({ ...content, defaultView: "FORM" }));
    const uploaded = await repository.importZip(await zip.generateAsync({ type: "uint8array" }));
    expect(uploaded).toHaveLength(2);
    expect(fetch.mock.calls.filter(([path]) => path === "/api/workflow/create")).toHaveLength(2);
    const bytes = await repository.exportZip([
      { wid: 4, name: "same" },
      { wid: 5, name: "same" },
    ]);
    const exported = await JSZip.loadAsync(bytes);
    expect(Object.keys(exported.files)).toEqual(["same.json", "same-1.json"]);
    expect(JSON.parse(await exported.files["same.json"].async("string"))).toEqual({ ...content, defaultView: "FORM" });
    await expect(repository.importZip(new Uint8Array([1, 2, 3]))).rejects.toThrow();
  });

  it("keeps sharing endpoint segments encoded and rejects malformed email/privileges", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("/owner/")
        ? new Response("owner@example.org")
        : path.includes("/list/")
          ? Response.json([])
          : Response.json({})
    );
    await repository.getAccess(4);
    await repository.grantAccess(4, "a+b@example.org", "READ");
    await repository.revokeAccess(4, "a+b@example.org");
    expect(fetch.mock.calls.map(([path]) => path)).toContain("/api/access/workflow/grant/4/a%2Bb%40example.org/READ");
    expect(fetch.mock.calls.map(([path]) => path)).toContain("/api/access/workflow/revoke/4/a%2Bb%40example.org");
    await expect(repository.grantAccess(4, "bad", "WRITE")).rejects.toThrow("Email");
  });

  it("preserves version-history and version-clone request contracts", async () => {
    const { repository, fetch } = setup(path =>
      Response.json(path.includes("/clone/") ? 8 : path === "/api/version/4" ? [] : record)
    );
    await repository.getVersions(4);
    await repository.getVersion(4, 9);
    await expect(repository.cloneVersion(9, 2)).resolves.toBe(8);
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      "/api/version/4",
      "/api/version/4/9",
      "/api/version/clone/9",
    ]);
    expect(JSON.parse(String(fetch.mock.calls[2][1]?.body))).toEqual({ displayedVersionId: 2 });
  });

  it("orders persist requests, snapshots content and continues after a failed save", async () => {
    let finish!: () => void;
    const first = new Promise<void>(resolve => {
      finish = resolve;
    });
    let calls = 0;
    const fetch = vi.fn(async (_path: string, init?: RequestInit) => {
      calls++;
      if (calls === 1) {
        await first;
        return new Response("failed", { status: 500 });
      }
      return Response.json({ ...record, ...JSON.parse(String(init?.body)) });
    });
    const repository = new WorkflowRepository(new ApiClient({ fetch }));
    const before = repository.persist({ ...record, content } as never).catch(error => error);
    const after = repository.persist({ ...record, name: "second", content } as never);
    await Promise.resolve();
    expect(fetch).toHaveBeenCalledTimes(1);
    finish();
    expect(await before).toMatchObject({ status: 500 });
    expect(await after).toMatchObject({ name: "second", content });
    await repository.whenSavesDrained();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("enriches actual workflow activity and respects failed like toggles", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("resultsOwnersInfo")
        ? Response.json({ 7: { userName: "Ada", avatar: "avatar" } })
        : path.includes("/counts")
          ? Response.json([{ entityId: 4, entityType: "workflow", counts: { view: 3, clone: 2, like: 9 } }])
          : path.includes("/isLiked")
            ? Response.json([{ entityId: 4, entityType: "workflow", isLiked: true }])
            : path.includes("/user-access")
              ? Response.json([{ entityId: 4, entityType: "workflow", userIds: [7, 8] }])
              : path.includes("/size")
                ? Response.json({ 4: 321 })
                : Response.json(false)
    );
    const activity = await repository.getActivity([{ ...entry, workflow: { ...record, content } } as never]);
    expect(activity[4]).toMatchObject({
      ownerName: "Ada",
      size: 321,
      views: 3,
      clones: 2,
      likes: 9,
      isLiked: true,
      accessibleUserIds: [7, 8],
    });
    await expect(repository.toggleLike(4, true)).resolves.toEqual({ liked: true, likeCount: 9 });
    expect(fetch.mock.calls.some(([path]) => path === "/api/hub/unlike")).toBe(true);
    await expect(repository.getActivity([])).resolves.toEqual({});
  });

  it("updates only workflow cover endpoints and treats notebook cleanup as best effort", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("notebook") ? new Response("offline", { status: 503 }) : Response.json({})
    );
    await repository.setCover(4, "data:image/png;base64,bytes");
    await repository.deleteCover(4);
    await expect(repository.cleanupNotebook(4)).resolves.toBeUndefined();
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      "/api/workflow/4/cover",
      "/api/workflow/4/cover",
      "/api/notebook-migration/delete-notebook",
    ]);
  });
});

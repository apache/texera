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
import { HubRepository } from "./hub-repository";
const workflow = {
  wid: 4,
  name: "Example",
  description: "Analysis",
  content: {
    operators: [],
    links: [],
    commentBoxes: [],
    operatorPositions: {},
    settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" },
  },
  defaultView: "FORM",
};
const item = {
  resourceType: "workflow",
  workflow: { workflow, ownerId: 7, ownerName: "Ada", accessLevel: "READ", isOwner: false },
};
function setup(response: (path: string, init?: RequestInit) => Response = () => Response.json({})) {
  const fetch = vi.fn(async (path: string, init?: RequestInit) => response(path, init));
  return { repository: new HubRepository(new ApiClient({ fetch })), fetch };
}
describe("HubRepository", () => {
  it("browses public workflows, datasets and models with type-specific defaults and raw pagination offsets", async () => {
    const { repository, fetch } = setup(() =>
      Response.json({ results: [item, { resourceType: "unknown" }], more: true })
    );
    const page = await repository.search("workflow", { keywords: ["日本語"], start: 20 });
    expect(page).toMatchObject({ more: true, nextStart: 22, items: [{ kind: "workflow", id: 4, name: "Example" }] });
    const query = new URL(fetch.mock.calls[0][0], "https://texera.example").searchParams;
    expect(query.get("query")).toBe("日本語");
    expect(query.get("resourceType")).toBe("workflow");
    expect(query.get("includePublic")).toBe("true");
    expect(query.get("orderBy")).toBe("EditTimeDesc");
    await repository.search("model");
    expect(fetch.mock.calls[1][0]).toContain("orderBy=CreateTimeDesc");
  });
  it("normalizes versioned payloads and handles missing repository rows without inventing cards", async () => {
    const { repository } = setup(() =>
      Response.json({
        results: [
          {
            resourceType: "dataset",
            dataset: { dataset: { did: 2, name: "Data", ownerUid: 8 }, ownerEmail: "data@example.org", size: 45 },
          },
          {
            resourceType: "model",
            model: { model: { mid: 3, name: "Model", ownerUid: 8 }, ownerEmail: "data@example.org", size: 90 },
          },
          { resourceType: "model", model: null },
        ],
        more: false,
        hasMismatch: true,
      })
    );
    const page = await repository.search(undefined);
    expect(page.items.map(entry => [entry.kind, entry.id, entry.name, entry.size])).toEqual([
      ["dataset", 2, "Data", 45],
      ["model", 3, "Model", 90],
    ]);
    expect(page.hasMismatch).toBe(true);
    expect(page.nextStart).toBe(3);
  });
  it("uses existing counts, public owners and top-list APIs", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("/getTops")
        ? Response.json({ like: [item], clone: [] })
        : path.includes("/owners")
          ? Response.json(["Ada"])
          : Response.json(8)
    );
    await expect(repository.count("workflow")).resolves.toBe(8);
    await expect(repository.owners("dataset")).resolves.toEqual(["Ada"]);
    const tops = await repository.tops("workflow", ["like", "clone"]);
    expect(tops.like[0]).toMatchObject({ id: 4 });
    const query = new URL(fetch.mock.calls[2][0], "https://texera.example").searchParams;
    expect(query.getAll("actionTypes")).toEqual(["like", "clone"]);
    expect(query.get("uid")).toBe("-1");
  });
  it("loads public workflow content and owner, and records anonymous views with user ID zero", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("owner_name")
        ? new Response("Ada")
        : path.includes("/hub/view")
          ? Response.json(12)
          : Response.json(workflow)
    );
    await expect(repository.workflowDetail(4)).resolves.toMatchObject({
      workflow: { name: "Example" },
      ownerName: "Ada",
    });
    await expect(repository.recordView("workflow", 4)).resolves.toBe(12);
    const view = fetch.mock.calls.find(([path]) => path === "/api/hub/view");
    expect(JSON.parse(String(view?.[1]?.body))).toEqual({ entityId: 4, entityType: "workflow", userId: 0 });
    await expect(repository.workflowDetail(NaN)).rejects.toThrow("ID");
  });
  it("clones real workflows and applies server-confirmed like state/count", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("/clone/")
        ? Response.json(9)
        : path.includes("/counts")
          ? Response.json([{ entityId: 4, entityType: "workflow", counts: { like: 12 } }])
          : Response.json(true)
    );
    await expect(repository.cloneWorkflow(4)).resolves.toBe(9);
    await expect(repository.toggleLike("workflow", 4, false)).resolves.toEqual({ liked: true, likeCount: 12 });
    expect(fetch.mock.calls.map(([path]) => path)).toContain("/api/workflow/clone/4");
    expect(JSON.parse(String(fetch.mock.calls.find(([path]) => path === "/api/hub/like")?.[1]?.body))).toEqual({
      entityId: 4,
      entityType: "workflow",
    });
  });

  it("keeps equal numeric IDs of different resource kinds distinct and skips anonymous liked checks", async () => {
    const { repository, fetch } = setup(path =>
      path.includes("/counts")
        ? Response.json([
            { entityType: "workflow", entityId: 4, counts: { like: 3 } },
            { entityType: "dataset", entityId: 4, counts: { like: 9 } },
          ])
        : path.includes("/user-access")
          ? Response.json([])
          : Response.json({})
    );
    const activity = await repository.activity([
      { kind: "workflow", id: 4, name: "Flow", description: "" },
      { kind: "dataset", id: 4, name: "Data", description: "", size: 200 },
    ]);
    expect(activity["workflow:4"].likes).toBe(3);
    expect(activity["dataset:4"]).toMatchObject({ likes: 9, size: 200 });
    expect(fetch.mock.calls.some(([path]) => path.includes("/isLiked"))).toBe(false);
  });

  it("rejects malformed search responses and invalid pagination before making unbounded requests", async () => {
    const { repository, fetch } = setup(() => Response.json({ results: null }));
    await expect(repository.search("workflow")).rejects.toThrow("Invalid public-search");
    await expect(repository.search("workflow", { count: 0 })).rejects.toThrow("Invalid search page");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("uses authenticated search without public rows for private resource lists", async () => {
    const { repository, fetch } = setup(() => Response.json({ results: [], more: false }));
    await repository.search("dataset", { scope: "private" });
    expect(fetch.mock.calls[0][0]).toMatch(/^\/api\/dashboard\/search\?/);
    expect(new URL(fetch.mock.calls[0][0], "https://texera.example").searchParams.get("includePublic")).toBe("false");
  });

  it("returns rejected promises for invalid public action IDs without starting network requests", async () => {
    const { repository, fetch } = setup();
    await expect(repository.recordView("dataset", NaN)).rejects.toThrow("ID");
    await expect(repository.cloneWorkflow(0)).rejects.toThrow("ID");
    expect(fetch).not.toHaveBeenCalled();
  });
});

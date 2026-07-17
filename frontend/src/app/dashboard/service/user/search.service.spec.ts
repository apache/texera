/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom, of } from "rxjs";

import { SearchService } from "./search.service";
import { AppSettings } from "../../../common/app-setting";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { ActionType, EntityType, HubService } from "../../../hub/service/hub.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { SearchFilterParameters } from "../../type/search-filter-parameters";
import { SortMethod } from "../../type/sort-method";
import { SearchResult, SearchResultItem } from "../../type/search-result";
import { DashboardWorkflow } from "../../type/dashboard-workflow.interface";
import { DashboardProject } from "../../type/dashboard-project.interface";
import { DashboardDataset } from "../../type/dashboard-dataset.interface";

const API = "api";

function makeEmptyFilter(): SearchFilterParameters {
  return {
    createDateStart: null,
    createDateEnd: null,
    modifiedDateStart: null,
    modifiedDateEnd: null,
    owners: [],
    ids: [],
    operators: [],
    projectIds: [],
  };
}

function makeWorkflowItem(wid: number, ownerId: number): SearchResultItem {
  const workflow: DashboardWorkflow = {
    isOwner: true,
    ownerName: undefined,
    workflow: {
      name: `wf-${wid}`,
      description: undefined,
      wid,
      creationTime: 0,
      lastModifiedTime: 0,
      isPublished: 0,
      readonly: false,
      content: {
        operators: [],
        operatorPositions: {},
        links: [],
        commentBoxes: [],
        settings: { dataTransferBatchSize: 400, executionMode: "PIPELINED" as any },
      },
    },
    projectIDs: [],
    accessLevel: "WRITE",
    ownerId,
    coverImage: null,
  };
  return { resourceType: "workflow", workflow };
}

function makeProjectItem(pid: number, ownerId: number): SearchResultItem {
  const project: DashboardProject = {
    pid,
    name: `proj-${pid}`,
    description: "",
    ownerId,
    creationTime: 0,
    color: null,
    accessLevel: "WRITE",
  };
  return { resourceType: "project", project };
}

function makeDatasetItem(did: number, ownerUid: number): SearchResultItem {
  const dataset: DashboardDataset = {
    isOwner: true,
    ownerEmail: "o@example.com",
    accessPrivilege: "WRITE",
    size: 17,
    dataset: {
      did,
      ownerUid,
      name: `ds-${did}`,
      isPublic: false,
      isDownloadable: false,
      storagePath: undefined,
      description: "",
      creationTime: 0,
      coverImage: undefined,
    },
  };
  return { resourceType: "dataset", dataset };
}

describe("SearchService", () => {
  let service: SearchService;
  let http: HttpTestingController;
  let hubSpy: {
    getCounts: ReturnType<typeof vi.fn>;
    isLiked: ReturnType<typeof vi.fn>;
    getUserAccess: ReturnType<typeof vi.fn>;
  };
  let persistSpy: { getSizes: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    hubSpy = {
      getCounts: vi.fn().mockReturnValue(of([])),
      isLiked: vi.fn().mockReturnValue(of([])),
      getUserAccess: vi.fn().mockReturnValue(of([])),
    };
    persistSpy = { getSizes: vi.fn().mockReturnValue(of({})) };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        SearchService,
        { provide: HubService, useValue: hubSpy },
        { provide: WorkflowPersistService, useValue: persistSpy },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(SearchService);
    http = TestBed.inject(HttpTestingController);
    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);
  });

  afterEach(() => {
    http.verify();
  });

  // ─── search ───────────────────────────────────────────────────────────────

  describe("search", () => {
    it("hits dashboard/search with includePublic=true when logged in and asking for public", async () => {
      const result: SearchResult = { results: [], more: false };
      const pending = firstValueFrom(
        service.search(["k"], makeEmptyFilter(), 0, 10, "workflow", SortMethod.NameAsc, true, true)
      );
      const req = http.expectOne(r => r.url.startsWith(`${API}/dashboard/search`));
      expect(req.request.method).toBe("GET");
      expect(req.request.url).toContain("includePublic=true");
      req.flush(result);
      expect(await pending).toEqual(result);
    });

    it("hits dashboard/search with includePublic=false when logged in and asking for private only", () => {
      service.search(["k"], makeEmptyFilter(), 0, 10, "workflow", SortMethod.NameAsc, true, false).subscribe();
      const req = http.expectOne(r => r.url.startsWith(`${API}/dashboard/search`));
      expect(req.request.url).toContain("includePublic=false");
      req.flush({ results: [], more: false });
    });

    it("hits dashboard/publicSearch and forces includePublic=true when anonymous", () => {
      service.search(["k"], makeEmptyFilter(), 0, 10, "workflow", SortMethod.NameAsc, false, false).subscribe();
      const req = http.expectOne(r => r.url.startsWith(`${API}/dashboard/publicSearch`));
      expect(req.request.url).toContain("includePublic=true");
      req.flush({ results: [], more: false });
    });
  });

  // ─── getUserInfo ──────────────────────────────────────────────────────────

  it("getUserInfo encodes each user id as a repeated `userIds` query param", async () => {
    const pending = firstValueFrom(service.getUserInfo([1, 2]));
    const req = http.expectOne(r => r.url.startsWith(`${API}/dashboard/resultsOwnersInfo`));
    expect(req.request.urlWithParams).toContain("userIds=1");
    expect(req.request.urlWithParams).toContain("userIds=2");
    req.flush({ 1: { userName: "alice" } });
    expect(await pending).toEqual({ 1: { userName: "alice" } });
  });

  // ─── executeSearch ────────────────────────────────────────────────────────

  describe("executeSearch", () => {
    it("filters null/mismatched datasets and surfaces hasMismatch", async () => {
      const dsItem = makeDatasetItem(10, 5);
      const flagged: any = { resourceType: "dataset", dataset: null };
      const result: SearchResult = { results: [dsItem, flagged, null as any], more: true, hasMismatch: true };
      vi.spyOn(service, "search").mockReturnValue(of(result));
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({} as any));

      const batch = await firstValueFrom(
        service.executeSearch([], makeEmptyFilter(), 0, 10, "dataset", SortMethod.NameAsc, true, false)
      );

      expect(batch.entries).toHaveLength(1);
      expect(batch.entries[0].id).toBe(10);
      expect(batch.more).toBe(true);
      expect(batch.hasMismatch).toBe(true);
    });

    it("leaves hasMismatch undefined and skips filtering for non-dataset searches", async () => {
      const wf = makeWorkflowItem(11, 7);
      vi.spyOn(service, "search").mockReturnValue(of({ results: [wf], more: false, hasMismatch: true }));
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({} as any));

      const batch = await firstValueFrom(
        service.executeSearch([], makeEmptyFilter(), 0, 10, "workflow", SortMethod.NameAsc, true, false)
      );

      expect(batch.hasMismatch).toBeUndefined();
      expect(batch.entries).toHaveLength(1);
    });
  });

  // ─── extendSearchResultsWithHubActivityInfo ───────────────────────────────

  describe("extendSearchResultsWithHubActivityInfo", () => {
    it("skips hub fetches and persist size lookup when there are no items", async () => {
      const entries = await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([], true));
      expect(entries).toEqual([]);
      expect(hubSpy.getCounts).not.toHaveBeenCalled();
      expect(hubSpy.isLiked).not.toHaveBeenCalled();
      expect(hubSpy.getUserAccess).not.toHaveBeenCalled();
      expect(persistSpy.getSizes).not.toHaveBeenCalled();
    });

    it("hydrates counts, like flags, access ids, and sizes for a workflow item", async () => {
      const wf = makeWorkflowItem(11, 7);
      hubSpy.getCounts.mockReturnValue(
        of([{ entityId: 11, entityType: EntityType.Workflow, counts: { [ActionType.View]: 3, [ActionType.Like]: 1 } }])
      );
      hubSpy.isLiked.mockReturnValue(of([{ entityId: 11, entityType: EntityType.Workflow, isLiked: true }]));
      hubSpy.getUserAccess.mockReturnValue(of([{ entityId: 11, entityType: EntityType.Workflow, userIds: [99] }]));
      persistSpy.getSizes.mockReturnValue(of({ 11: 4096 }));

      const userInfoSpy = vi.spyOn(service, "getUserInfo").mockReturnValue(of({ 7: { userName: "alice" } }));

      const [entry] = await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([wf], true));

      expect(userInfoSpy).toHaveBeenCalledWith([7]);
      expect(entry.viewCount).toBe(3);
      expect(entry.likeCount).toBe(1);
      expect(entry.isLiked).toBe(true);
      expect(entry.accessibleUserIds).toEqual([99]);
      expect(entry.size).toBe(4096);
      expect(entry.ownerName).toBe("alice");
    });

    it("skips like lookup when the user is not logged in", async () => {
      const wf = makeWorkflowItem(11, 7);
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({} as any));

      const [entry] = await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([wf], false));

      expect(hubSpy.isLiked).not.toHaveBeenCalled();
      expect(entry.isLiked).toBe(false);
    });

    it("honors a narrowed activities list (counts only)", async () => {
      const wf = makeWorkflowItem(11, 7);
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({} as any));

      await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([wf], true, ["counts"]));

      expect(hubSpy.getCounts).toHaveBeenCalled();
      expect(hubSpy.isLiked).not.toHaveBeenCalled();
      expect(hubSpy.getUserAccess).not.toHaveBeenCalled();
      expect(persistSpy.getSizes).not.toHaveBeenCalled();
    });

    it("uses Project entity routing for project items", async () => {
      const proj = makeProjectItem(20, 8);
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({ 8: { userName: "bob" } }));
      hubSpy.getCounts.mockReturnValue(
        of([{ entityId: 20, entityType: EntityType.Project, counts: { [ActionType.Clone]: 2 } }])
      );

      const [entry] = await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([proj], true));

      const [types, ids] = hubSpy.getCounts.mock.calls[0];
      expect(types).toEqual([EntityType.Project]);
      expect(ids).toEqual([20]);
      expect(entry.cloneCount).toBe(2);
      expect(entry.ownerName).toBe("bob");
    });

    it("uses Dataset entity routing and pulls ownerUid for dataset items", async () => {
      const ds = makeDatasetItem(30, 9);
      const userInfoSpy = vi.spyOn(service, "getUserInfo").mockReturnValue(of({ 9: { userName: "carol" } }));
      hubSpy.getUserAccess.mockReturnValue(of([{ entityId: 30, entityType: EntityType.Dataset, userIds: [42, 43] }]));

      const [entry] = await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([ds], true));

      expect(userInfoSpy).toHaveBeenCalledWith([9]);
      expect(hubSpy.getUserAccess.mock.calls[0][0]).toEqual([EntityType.Dataset]);
      expect(entry.accessibleUserIds).toEqual([42, 43]);
      expect(entry.ownerName).toBe("carol");
    });

    it("does not request sizes when there are no workflow items", async () => {
      const proj = makeProjectItem(20, 8);
      vi.spyOn(service, "getUserInfo").mockReturnValue(of({} as any));

      await firstValueFrom(service.extendSearchResultsWithHubActivityInfo([proj], true));

      expect(persistSpy.getSizes).not.toHaveBeenCalled();
    });
  });
});

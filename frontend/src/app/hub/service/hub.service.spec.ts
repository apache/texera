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
import {
  AccessResponse,
  ActionType,
  CountResponse,
  EntityType,
  HubService,
  LikedStatus,
  WORKFLOW_BASE_URL,
} from "./hub.service";

describe("HubService", () => {
  let service: HubService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [HubService],
    });
    service = TestBed.inject(HubService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("getCount GETs /count with the entityType param and emits the count", () => {
    let result: number | undefined;
    service.getCount(EntityType.Workflow).subscribe(n => (result = n));

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/count`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.get("entityType")).toBe("workflow");

    req.flush(5);
    expect(result).toBe(5);
  });

  it("cloneWorkflow POSTs to /workflow/clone/:wid with a null body and emits the new wid", () => {
    let result: number | undefined;
    service.cloneWorkflow(42).subscribe(n => (result = n));

    const req = httpMock.expectOne(`${WORKFLOW_BASE_URL}/clone/42`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toBeNull();

    req.flush(99);
    expect(result).toBe(99);
  });

  it("isLiked GETs /isLiked appending every id and type, and emits the statuses", () => {
    const statuses: LikedStatus[] = [
      { entityId: 1, entityType: EntityType.Workflow, isLiked: true },
      { entityId: 2, entityType: EntityType.Dataset, isLiked: false },
    ];
    let result: LikedStatus[] | undefined;
    service.isLiked([1, 2], [EntityType.Workflow, EntityType.Dataset]).subscribe(s => (result = s));

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/isLiked`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.getAll("entityId")).toEqual(["1", "2"]);
    expect(req.request.params.getAll("entityType")).toEqual(["workflow", "dataset"]);

    req.flush(statuses);
    expect(result).toEqual(statuses);
  });

  it("postLike POSTs /like with a json body and emits the boolean result", () => {
    let result: boolean | undefined;
    service.postLike(1, EntityType.Workflow).subscribe(b => (result = b));

    const req = httpMock.expectOne(`${service.BASE_URL}/like`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ entityId: 1, entityType: EntityType.Workflow });
    expect(req.request.headers.get("Content-Type")).toBe("application/json");

    req.flush(true);
    expect(result).toBe(true);
  });

  it("postUnlike POSTs /unlike with a json body and emits the boolean result", () => {
    let result: boolean | undefined;
    service.postUnlike(1, EntityType.Workflow).subscribe(b => (result = b));

    const req = httpMock.expectOne(`${service.BASE_URL}/unlike`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ entityId: 1, entityType: EntityType.Workflow });

    req.flush(false);
    expect(result).toBe(false);
  });

  it("toggleLike likes (not currently liked) then re-fetches the like count", () => {
    let result: { liked: boolean; likeCount: number } | undefined;
    service.toggleLike(1, EntityType.Workflow, false).subscribe(r => (result = r));

    // First request: the like POST carries the target entity.
    const likeReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/like`);
    expect(likeReq.request.method).toBe("POST");
    expect(likeReq.request.body).toEqual({ entityId: 1, entityType: EntityType.Workflow });
    likeReq.flush(true);

    // Second request: the counts GET fired by switchMap re-queries the same entity for the like count.
    const countsReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/counts`);
    expect(countsReq.request.method).toBe("GET");
    expect(countsReq.request.params.getAll("entityType")).toEqual(["workflow"]);
    expect(countsReq.request.params.getAll("entityId")).toEqual(["1"]);
    expect(countsReq.request.params.getAll("actionType")).toEqual(["like"]);
    countsReq.flush([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 7 } }] as CountResponse[]);

    expect(result).toEqual({ liked: true, likeCount: 7 });
  });

  it("toggleLike unlikes (currently liked) then re-fetches the like count", () => {
    let result: { liked: boolean; likeCount: number } | undefined;
    service.toggleLike(1, EntityType.Workflow, true).subscribe(r => (result = r));

    const unlikeReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/unlike`);
    expect(unlikeReq.request.method).toBe("POST");
    expect(unlikeReq.request.body).toEqual({ entityId: 1, entityType: EntityType.Workflow });
    unlikeReq.flush(true);

    const countsReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/counts`);
    expect(countsReq.request.method).toBe("GET");
    expect(countsReq.request.params.getAll("entityType")).toEqual(["workflow"]);
    expect(countsReq.request.params.getAll("entityId")).toEqual(["1"]);
    expect(countsReq.request.params.getAll("actionType")).toEqual(["like"]);
    countsReq.flush([{ entityId: 1, entityType: EntityType.Workflow, counts: { like: 3 } }] as CountResponse[]);

    expect(result).toEqual({ liked: false, likeCount: 3 });
  });

  it("toggleLike keeps the current state when the action fails and defaults the count to 0", () => {
    let result: { liked: boolean; likeCount: number } | undefined;
    service.toggleLike(1, EntityType.Workflow, false).subscribe(r => (result = r));

    const likeReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/like`);
    expect(likeReq.request.method).toBe("POST");
    expect(likeReq.request.body).toEqual({ entityId: 1, entityType: EntityType.Workflow });
    likeReq.flush(false);

    const countsReq = httpMock.expectOne(r => r.url === `${service.BASE_URL}/counts`);
    expect(countsReq.request.method).toBe("GET");
    expect(countsReq.request.params.getAll("entityId")).toEqual(["1"]);
    countsReq.flush([] as CountResponse[]);

    // action returned false -> liked stays at currentlyLiked (false); empty counts -> 0.
    expect(result).toEqual({ liked: false, likeCount: 0 });
  });

  it("postView POSTs /view with a json body and emits the view count", () => {
    let result: number | undefined;
    service.postView(1, 7, EntityType.Workflow).subscribe(n => (result = n));

    const req = httpMock.expectOne(`${service.BASE_URL}/view`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ entityId: 1, userId: 7, entityType: EntityType.Workflow });
    expect(req.request.headers.get("Content-Type")).toBe("application/json");

    req.flush(12);
    expect(result).toBe(12);
  });

  it("getTops GETs /getTops with entityType, uid, limit and appended actionTypes", () => {
    let result: Record<ActionType, unknown[]> | undefined;
    service.getTops(EntityType.Workflow, [ActionType.Like, ActionType.Clone], 5, 3).subscribe(r => (result = r));

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/getTops`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.get("entityType")).toBe("workflow");
    expect(req.request.params.get("uid")).toBe("5");
    expect(req.request.params.get("limit")).toBe("3");
    expect(req.request.params.getAll("actionTypes")).toEqual(["like", "clone"]);

    const payload = { like: [], clone: [] } as unknown as Record<ActionType, unknown[]>;
    req.flush(payload);
    expect(result).toEqual(payload);
  });

  it("getTops defaults uid to -1 and omits limit when it is not provided", () => {
    service.getTops(EntityType.Dataset, [ActionType.Like]).subscribe();

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/getTops`);
    expect(req.request.params.get("uid")).toBe("-1");
    expect(req.request.params.has("limit")).toBe(false);

    req.flush({ like: [] });
  });

  it("getCounts GETs /counts appending types, ids and actionTypes", () => {
    const counts: CountResponse[] = [{ entityId: 1, entityType: EntityType.Workflow, counts: { view: 3, like: 4 } }];
    let result: CountResponse[] | undefined;
    service
      .getCounts([EntityType.Workflow, EntityType.Dataset], [1, 2], [ActionType.View, ActionType.Like])
      .subscribe(r => (result = r));

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/counts`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.getAll("entityType")).toEqual(["workflow", "dataset"]);
    expect(req.request.params.getAll("entityId")).toEqual(["1", "2"]);
    expect(req.request.params.getAll("actionType")).toEqual(["view", "like"]);

    req.flush(counts);
    expect(result).toEqual(counts);
  });

  it("getCounts omits the actionType param when no actionTypes are given", () => {
    service.getCounts([EntityType.Workflow], [1]).subscribe();

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/counts`);
    expect(req.request.params.has("actionType")).toBe(false);

    req.flush([]);
  });

  it("getUserAccess GETs /user-access appending types and ids, and emits the access list", () => {
    const access: AccessResponse[] = [{ entityType: EntityType.Workflow, entityId: 1, userIds: [10, 20] }];
    let result: AccessResponse[] | undefined;
    service.getUserAccess([EntityType.Workflow], [1]).subscribe(r => (result = r));

    const req = httpMock.expectOne(r => r.url === `${service.BASE_URL}/user-access`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.getAll("entityType")).toEqual(["workflow"]);
    expect(req.request.params.getAll("entityId")).toEqual(["1"]);

    req.flush(access);
    expect(result).toEqual(access);
  });
});

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

import { HttpClient, HttpHeaders, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../common/app-setting";
import { SearchResultItem } from "../../dashboard/type/search-result";

export const WORKFLOW_BASE_URL = `${AppSettings.getApiEndpoint()}/workflow`;

export enum EntityType {
  Workflow = "workflow",
  Dataset = "dataset",
  Project = "project",
  File = "file",
}

export enum ActionType {
  View = "view",
  Like = "like",
  Clone = "clone",
  Unlike = "unlike",
}

export type LikedStatus = {
  entityId: number;
  entityType: EntityType;
  isLiked: boolean;
};

export interface CountResponse {
  entityId: number;
  entityType: EntityType;
  counts: Partial<Record<ActionType, number>>;
}

export interface AccessResponse {
  entityType: EntityType;
  entityId: number;
  userIds: number[];
}

@Injectable({
  providedIn: "root",
})
export class HubService {
  readonly BASE_URL: string = `${AppSettings.getApiEndpoint()}/hub`;

  constructor(private http: HttpClient) {}

  public getCount(entityType: EntityType): Observable<number> {
    return this.http.get<number>(`${this.BASE_URL}/count`, {
      params: { entityType: entityType },
    });
  }

  public cloneWorkflow(wid: number): Observable<number> {
    return this.http.post<number>(`${WORKFLOW_BASE_URL}/clone/${wid}`, null);
  }

  public isLiked(entityIds: number[], entityTypes: EntityType[]): Observable<LikedStatus[]> {
    let params = new HttpParams();
    entityIds.forEach(id => {
      params = params.append("entityId", id.toString());
    });
    entityTypes.forEach(type => {
      params = params.append("entityType", type);
    });

    return this.http.get<LikedStatus[]>(`${this.BASE_URL}/isLiked`, { params });
  }

  public postLike(entityId: number, entityType: EntityType): Observable<boolean> {
    const body = { entityId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/like`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public postUnlike(entityId: number, entityType: EntityType): Observable<boolean> {
    const body = { entityId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/unlike`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public postView(entityId: number, userId: number, entityType: EntityType): Observable<number> {
    const body = { entityId, userId, entityType };
    return this.http.post<number>(`${this.BASE_URL}/view`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  /**
   * Fetches the top entities for the given action types in one request.
   *
   * @param entityType   The type of entity to query (e.g. EntityType.Workflow or EntityType.Dataset).
   * @param actionTypes  An array of action types to retrieve (only ActionType.Like and ActionType.Clone).
   * @param currentUid   Optional user ID context (will be sent as -1 if undefined).
   * @param limit        Optional maximum number of top items per action; must be >0 (default: 8).
   * @returns            An Observable resolving to a map where each key is one of ActionType.Like | ActionType.Clone
   *                     and the value is the corresponding list of SearchResultItem.
   */
  public getTops(
    entityType: EntityType,
    actionTypes: ActionType[],
    currentUid?: number,
    limit?: number
  ): Observable<Record<ActionType, SearchResultItem[]>> {
    let params = new HttpParams()
      .set("entityType", entityType)
      .set("uid", (currentUid !== undefined ? currentUid : -1).toString());

    if (limit != null && limit > 0) {
      params = params.set("limit", limit.toString());
    }

    actionTypes.forEach(act => {
      params = params.append("actionTypes", act);
    });

    return this.http.get<Record<ActionType, SearchResultItem[]>>(`${this.BASE_URL}/getTops`, { params });
  }

  /**
   * Fetches count metrics for multiple entities in a single request.
   *
   * @param entityTypes  Array of entity types (e.g., [EntityType.Workflow, EntityType.Dataset]).
   * @param entityIds    Corresponding array of entity IDs (e.g., [123, 456]). Must match length of entityTypes.
   * @param actionTypes  Optional array of action types to retrieve counts for (members of ActionType enum).
   *                     Supported values: ActionType.View, ActionType.Like, ActionType.Clone.
   *                     If omitted or empty, all three will be returned.
   * @returns            An Observable that emits an array of CountResponse objects, each containing:
   *                       - entityId: the ID of the entity
   *                       - entityType: the type of the entity
   *                       - counts: a map from ActionType to number
   */
  public getCounts(
    entityTypes: EntityType[],
    entityIds: number[],
    actionTypes: ActionType[] = []
  ): Observable<CountResponse[]> {
    let params = new HttpParams();
    entityTypes.forEach(type => {
      params = params.append("entityType", type);
    });
    entityIds.forEach(id => {
      params = params.append("entityId", id.toString());
    });
    actionTypes.forEach(a => {
      params = params.append("actionType", a);
    });

    return this.http.get<CountResponse[]>(`${this.BASE_URL}/counts`, { params });
  }

  public getUserAccess(entityTypes: EntityType[], entityIds: number[]): Observable<AccessResponse[]> {
    let params = new HttpParams();
    entityTypes.forEach(t => (params = params.append("entityType", t)));
    entityIds.forEach(i => (params = params.append("entityId", i.toString())));

    return this.http.get<AccessResponse[]>(`${this.BASE_URL}/user-access`, { params });
  }
}

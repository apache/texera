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
export interface CountRequest {
  entityId: number;
  entityType: string;
}
export interface CountResponse {
  entityId: number;
  entityType: string;
  counts: { [action: string]: number };
}

@Injectable({
  providedIn: "root",
})
export class HubService {
  readonly BASE_URL: string = `${AppSettings.getApiEndpoint()}/hub`;

  constructor(private http: HttpClient) {}

  public getCount(entityType: string): Observable<number> {
    return this.http.get<number>(`${this.BASE_URL}/count`, {
      params: { entityType: entityType },
    });
  }

  public cloneWorkflow(wid: number): Observable<number> {
    return this.http.post<number>(`${WORKFLOW_BASE_URL}/clone/${wid}`, null);
  }

  public isLiked(entityId: number, userId: number, entityType: string): Observable<boolean> {
    return this.http.get<boolean>(`${this.BASE_URL}/isLiked`, {
      params: { workflowId: entityId.toString(), userId: userId.toString(), entityType },
    });
  }

  public postLike(entityId: number, userId: number, entityType: string): Observable<boolean> {
    const body = { entityId, userId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/like`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public postUnlike(entityId: number, userId: number, entityType: string): Observable<boolean> {
    const body = { entityId, userId, entityType };
    return this.http.post<boolean>(`${this.BASE_URL}/unlike`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  public postView(entityId: number, userId: number, entityType: string): Observable<number> {
    const body = { entityId, userId, entityType };
    return this.http.post<number>(`${this.BASE_URL}/view`, body, {
      headers: new HttpHeaders({ "Content-Type": "application/json" }),
    });
  }

  /**
   * Fetches the top entities for the given action types in one request.
   *
   * @param entityType   The type of entity to query (e.g. 'workflow' or 'dataset').
   * @param actionTypes  An array of action types to retrieve (e.g. ['like', 'clone']).
   * @param currentUid   User ID context (will be sent as -1 if undefined).
   * @returns             An Observable resolving to a map where each key is an actionType
   *                      and the value is the corresponding list of SearchResultItem.
   */
  public getTops(
    entityType: string,
    actionTypes: string[],
    currentUid?: number
  ): Observable<{ [actionType: string]: SearchResultItem[] }> {
    let params = new HttpParams()
      .set("entityType", entityType)
      .set("uid", (currentUid !== undefined ? currentUid : -1).toString());

    actionTypes.forEach(act => {
      params = params.append("actionTypes", act);
    });

    return this.http.get<{ [actionType: string]: SearchResultItem[] }>(`${this.BASE_URL}/getTops`, { params });
  }

  /**
   * Fetches multiple counts (view, like, clone) for a given entity in one request.
   *
   * @param entityId    The numeric ID of the entity to query.
   * @param entityType  The type of entity (e.g. "workflow" or "dataset").
   * @param actionTypes An array of action types to fetch (each of "view", "like", or "clone").
   * @returns            An Observable resolving to a map from actionType to its count.
   */
  public getCounts(
    entityId: number,
    entityType: string,
    actionTypes: string[]
  ): Observable<{ [action: string]: number }> {
    let params = new HttpParams().set("entityId", entityId.toString()).set("entityType", entityType);

    actionTypes.forEach(t => {
      params = params.append("actionType", t);
    });

    return this.http.get<{ [action: string]: number }>(`${this.BASE_URL}/counts`, { params });
  }

  public getBatchCounts(requests: CountRequest[]): Observable<CountResponse[]> {
    return this.http.post<CountResponse[]>(`${this.BASE_URL}/batch`, requests);
  }
}

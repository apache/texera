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

import { Observable } from "rxjs";
import { DashboardEntry } from "./dashboard-entry";
import { EntityType } from "../../hub/service/hub.service";

/** What publishing an entry of a kind grants everyone else. */
export interface ResourceAffordances {
  /** Whether a published entry may also be cloned, not just read. */
  readonly clonable: boolean;
}

/**
 * What one dashboard resource kind can do. Optional members are the capability check: a component
 * asks `if (descriptor.rename)` instead of testing the entry type, so adding a resource is a new
 * descriptor rather than another arm in every branch chain.
 */
export interface ResourceDescriptor {
  readonly type: EntityType;
  /** ng-zorro icon shown on this kind's cards and rows. */
  readonly iconType: string;
  /** Route to the owner-facing page; absent when the kind has no page of its own. */
  readonly privateRoute?: string;
  /** Route to the public hub page; absent when the kind is never browsed in the hub. */
  readonly hubRoute?: string;
  /** Whether an entry of this kind carries a size worth showing. */
  readonly hasSize?: boolean;
  isOwner(entry: DashboardEntry): boolean;
  /** Applied when the user clears the name field; absent when the kind cannot be renamed. */
  readonly defaultName?: string;
  /** Returns an error message, or null when the name is acceptable. */
  validateName?(name: string): string | null;
  rename?(id: number, name: string): Observable<unknown>;
  updateDescription?(id: number, description: string): Observable<unknown>;
  /** Downloads the whole resource; absent when the kind has nothing to download. */
  download?(id: number, name: string): Observable<unknown>;
  /** Fetches one file of a version for preview, by its logical path. */
  retrieveSingleFile?(filePath: string, isLogin: boolean): Observable<Blob>;
  /** Owners of this kind, for the filter dropdown and the share modal. */
  retrieveOwners?(): Observable<string[]>;
  /** What other users get once an entry is published; absent when the kind cannot be published. */
  readonly affordances?: ResourceAffordances;
  /** Whether the entry is currently published. */
  isPublic?(id: number): Observable<boolean>;
  setPublished?(id: number, next: boolean): Observable<unknown>;
  /** A ready-to-render URL for the entry's cover image, or null when it has none. */
  coverUrl?(id: number): Observable<string | null>;
  /** Points the cover at a committed file of the entry, as "<version>/<path>". */
  setCover?(id: number, path: string): Observable<unknown>;
  /** Entry ids of this kind; absent when the backend exposes no such endpoint. */
  retrieveIds?(): Observable<number[]>;
}

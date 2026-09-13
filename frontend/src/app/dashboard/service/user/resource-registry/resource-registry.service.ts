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

import { Injectable } from "@angular/core";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { ResourceDescriptor } from "../../../type/resource-descriptor";
import { EntityType } from "../../../../hub/service/hub.service";
import { forkJoin, Observable, of } from "rxjs";
import { catchError, map } from "rxjs/operators";
import { OwnerScope } from "../../../type/owner-scope";
import { WorkflowResourceDescriptor } from "./workflow-resource.descriptor";
import { DatasetResourceDescriptor } from "./dataset-resource.descriptor";
import { FileResourceDescriptor } from "./file-resource.descriptor";
import { ModelResourceDescriptor } from "./model-resource.descriptor";

/**
 * The one place that knows every dashboard resource kind. Descriptors are injectable rather than a
 * const map because each needs an HttpClient-backed service, which `inject()` cannot supply at
 * module scope.
 */
@Injectable({
  providedIn: "root",
})
export class ResourceRegistryService {
  private readonly descriptors: ReadonlyMap<EntityType, ResourceDescriptor>;

  constructor(
    workflow: WorkflowResourceDescriptor,
    dataset: DatasetResourceDescriptor,
    file: FileResourceDescriptor,
    model: ModelResourceDescriptor
  ) {
    // Computing units are deliberately absent: nothing in the dashboard renders them as entries.
    this.descriptors = new Map<EntityType, ResourceDescriptor>(
      [workflow, dataset, file, model].map(descriptor => [descriptor.type, descriptor])
    );
  }

  /** The descriptor for a kind, or undefined when the kind has none (computing units). */
  public find(type: EntityType): ResourceDescriptor | undefined {
    return this.descriptors.get(type);
  }

  public get(type: EntityType): ResourceDescriptor {
    const descriptor = this.descriptors.get(type);
    if (!descriptor) {
      throw new Error("Unexpected type in DashboardEntry.");
    }
    return descriptor;
  }

  /**
   * Names for the Owner facet of a page listing `type` under `scope`; `null` unions every kind.
   * A kind whose request fails contributes nothing rather than blanking the facet.
   */
  public ownersFor(type: EntityType | null, scope: OwnerScope): Observable<string[]> {
    const kinds = type === null ? [EntityType.Workflow, EntityType.Dataset, EntityType.Model] : [type];
    const requests = kinds.flatMap(kind => this.ownerRequests(kind, scope));
    if (requests.length === 0) {
      return of([]);
    }
    // Unsorted, so a single kind keeps its endpoint's order.
    return forkJoin(requests).pipe(map(lists => [...new Set(lists.flat())]));
  }

  /** The one or two lists a scope is made of, skipping any the descriptor cannot answer. */
  private ownerRequests(type: EntityType, scope: OwnerScope): Observable<string[]>[] {
    const descriptor = this.find(type);
    if (!descriptor) {
      return [];
    }
    const wanted: (Observable<string[]> | undefined)[] = [];
    if (scope !== "public") {
      wanted.push(descriptor.retrieveOwners?.());
    }
    if (scope !== "accessible") {
      wanted.push(descriptor.retrievePublicOwners?.());
    }
    return wanted
      .filter((request): request is Observable<string[]> => request !== undefined)
      .map(request => request.pipe(catchError(() => of([] as string[]))));
  }

  /**
   * Where an entry's card links to: the owner-facing page when the viewer can reach it, the hub page
   * otherwise. An entry with no route, or one not yet persisted, links nowhere.
   */
  public entryLink(entry: DashboardEntry, currentUid: number | undefined): string[] {
    const descriptor = this.get(entry.type);
    if (descriptor.privateRoute === undefined || typeof entry.id !== "number") {
      return [];
    }
    if (descriptor.hubRoute === undefined) {
      return [descriptor.privateRoute, String(entry.id)];
    }
    const reachableByViewer = currentUid !== undefined && entry.accessibleUserIds.includes(currentUid);
    return [reachableByViewer ? descriptor.privateRoute : descriptor.hubRoute, String(entry.id)];
  }
}

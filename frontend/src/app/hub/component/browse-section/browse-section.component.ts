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

import { ChangeDetectorRef, Component, Input, OnChanges, OnInit, SimpleChanges } from "@angular/core";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import { ResourceRegistryService } from "../../../dashboard/service/user/resource-registry/resource-registry.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_WORKSPACE,
} from "../../../app-routing.constant";
import { NgIf, NgFor, NgStyle, DatePipe } from "@angular/common";
import { NzCardComponent } from "ng-zorro-antd/card";
import { RouterLink } from "@angular/router";
import { UserAvatarComponent } from "../../../dashboard/component/user/user-avatar/user-avatar.component";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzAvatarComponent } from "ng-zorro-antd/avatar";

@UntilDestroy()
@Component({
  selector: "texera-browse-section",
  templateUrl: "./browse-section.component.html",
  styleUrls: ["./browse-section.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NzCardComponent,
    RouterLink,
    UserAvatarComponent,
    ɵNzTransitionPatchDirective,
    NzAvatarComponent,
    NgStyle,
    DatePipe,
  ],
})
export class BrowseSectionComponent implements OnInit, OnChanges {
  @Input() entities: DashboardEntry[] = [];
  @Input() sectionTitle: string = "";
  @Input() currentUid: number | undefined;

  defaultBackground: string = "../../../../../assets/card_background.jpg";
  protected readonly HUB_WORKFLOW_RESULT_DETAIL = HUB_WORKFLOW_RESULT_DETAIL;
  protected readonly USER_WORKSPACE = USER_WORKSPACE;
  protected readonly HUB_DATASET_RESULT_DETAIL = HUB_DATASET_RESULT_DETAIL;
  protected readonly USER_DATASET = USER_DATASET;
  entityRoutes: { [key: number]: string[] } = {};

  /** Keyed by type and id: ids are only unique within a kind, and sections may mix kinds. */
  private coverImageUrls = new Map<string, string>();

  constructor(
    private resourceRegistry: ResourceRegistryService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.entities.forEach(entity => {
      this.initializeEntry(entity);
    });
    this.loadCoverImages();
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.entities.forEach(entity => {
      this.initializeEntry(entity);
    });
    this.loadCoverImages();
  }

  private initializeEntry(entity: DashboardEntry): void {
    if (typeof entity.id !== "number") {
      return;
    }

    const entityId = entity.id;
    const owners = entity.accessibleUserIds;

    if (entity.type === "workflow") {
      if (this.currentUid !== undefined && owners.includes(this.currentUid)) {
        this.entityRoutes[entityId] = [this.USER_WORKSPACE, String(entityId)];
      } else {
        this.entityRoutes[entityId] = [this.HUB_WORKFLOW_RESULT_DETAIL, String(entityId)];
      }
    } else if (entity.type === "dataset") {
      if (this.currentUid !== undefined && owners.includes(this.currentUid)) {
        this.entityRoutes[entityId] = [this.USER_DATASET, String(entityId)];
      } else {
        this.entityRoutes[entityId] = [this.HUB_DATASET_RESULT_DETAIL, String(entityId)];
      }
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }

  private coverCacheKey(entity: DashboardEntry): string {
    return `${entity.type}:${entity.id}`;
  }

  /** Asks each kind's descriptor for its cover, so the hub renders the same picture as the cards. */
  private loadCoverImages(): void {
    if (!this.entities) return;

    this.entities
      .filter(
        (entity): entity is DashboardEntry & { id: number } =>
          entity.coverImageUrl !== undefined &&
          entity.id !== undefined &&
          !this.coverImageUrls.has(this.coverCacheKey(entity))
      )
      .forEach(entity => {
        // `find`, not `get`: a section may hold a kind the registry does not carry.
        const coverUrl = this.resourceRegistry.find(entity.type)?.coverUrl;
        if (!coverUrl) {
          return;
        }
        const key = this.coverCacheKey(entity);
        coverUrl(entity.id)
          .pipe(untilDestroyed(this))
          .subscribe(url => {
            if (url) {
              this.coverImageUrls.set(key, url);
              this.cdr.markForCheck();
            }
          });
      });
  }

  getCoverImage(entity: DashboardEntry): string {
    return this.coverImageUrls.get(this.coverCacheKey(entity)) || this.defaultBackground;
  }
}

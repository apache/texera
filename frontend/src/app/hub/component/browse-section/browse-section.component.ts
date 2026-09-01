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

  /** Both maps are keyed by type and id: ids are only unique within a kind, and sections may mix kinds. */
  private coverImageUrls = new Map<string, string>();
  private entityRoutes = new Map<string, string[]>();

  constructor(
    private resourceRegistry: ResourceRegistryService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadCoverImages();
  }

  ngOnChanges(changes: SimpleChanges): void {
    // The routes depend on the viewer as well as on the entities, and both arrive as inputs.
    this.entityRoutes.clear();
    this.loadCoverImages();
  }


  routeFor(entity: DashboardEntry): string[] {
    const key = this.cacheKey(entity);
    let route = this.entityRoutes.get(key);
    if (route === undefined) {
      route = this.resourceRegistry.find(entity.type) ? this.resourceRegistry.entryLink(entity, this.currentUid) : [];
      this.entityRoutes.set(key, route);
    }
    return route;
  }

  private cacheKey(entity: DashboardEntry): string {
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
          !this.coverImageUrls.has(this.cacheKey(entity))
      )
      .forEach(entity => {
        const coverUrl = this.resourceRegistry.find(entity.type)?.coverUrl;
        if (!coverUrl) {
          return;
        }
        const key = this.cacheKey(entity);
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
    return this.coverImageUrls.get(this.cacheKey(entity)) || this.defaultBackground;
  }
}

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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnChanges, SimpleChanges } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { RouterLink } from "@angular/router";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserAvatarComponent } from "../user-avatar/user-avatar.component";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { HubService } from "../../../../hub/service/hub.service";
import { formatSize } from "../../../../common/util/size-formatter.util";
import { formatCount, formatRelativeTime } from "../../../../common/util/format.util";
import { isDefined } from "../../../../common/util/predicate";
import { HUB_DATASET_RESULT_DETAIL, USER_DATASET } from "../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-dataset-card-item",
  templateUrl: "./dataset-card-item.component.html",
  styleUrls: ["./dataset-card-item.component.scss"],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, NzCardComponent, NzIconDirective, UserAvatarComponent],
})
export class DatasetCardItemComponent implements OnChanges {
  @Input() currentUid: number | undefined;
  @Input() entry!: DashboardEntry;

  entryLink: string[] = [];
  coverImageSrc: string = "";
  readonly defaultCover = "assets/card_background.jpg";
  likeCount = 0;
  viewCount = 0;
  isLiked = false;

  constructor(
    private datasetService: DatasetService,
    private hubService: HubService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"] || changes["currentUid"]) {
      this.initializeEntry();
    }
    if (changes["entry"]) {
      this.likeCount = this.entry.likeCount ?? 0;
      this.viewCount = this.entry.viewCount ?? 0;
      this.isLiked = this.entry.isLiked ?? false;
    }
  }

  private initializeEntry(): void {
    if (!this.entry || this.entry.type !== "dataset" || typeof this.entry.id !== "number") {
      return;
    }
    const did = this.entry.id;
    const owners = this.entry.accessibleUserIds;
    if (this.currentUid !== undefined && owners.includes(this.currentUid)) {
      this.entryLink = [USER_DATASET, String(did)];
    } else {
      this.entryLink = [HUB_DATASET_RESULT_DETAIL, String(did)];
    }

    this.coverImageSrc = this.defaultCover;
    if (this.entry.coverImageUrl) {
      this.datasetService
        .getDatasetCoverUrl(did)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: ({ url }) => {
            this.coverImageSrc = url ?? this.defaultCover;
            this.cdr.markForCheck();
          },
          error: () => {
            this.coverImageSrc = this.defaultCover;
            this.cdr.markForCheck();
          },
        });
    }
  }

  onCoverError(event: Event): void {
    const image = event.target as HTMLImageElement;
    image.onerror = null;
    image.src = this.defaultCover;
  }

  toggleLike(): void {
    if (!isDefined(this.currentUid) || !isDefined(this.entry.id)) return;
    // optimistic flip; server response reconciles or reverts
    const previousLiked = this.isLiked;
    this.isLiked = !previousLiked;
    this.likeCount += previousLiked ? -1 : 1;
    this.cdr.markForCheck();

    this.hubService
      .toggleLike(this.entry.id, this.entry.type, previousLiked)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ liked, likeCount }) => {
          this.isLiked = liked;
          this.likeCount = likeCount;
          this.cdr.markForCheck();
        },
        error: () => {
          this.isLiked = previousLiked;
          this.likeCount += previousLiked ? 1 : -1;
          this.cdr.markForCheck();
        },
      });
  }

  formatSize = formatSize;
  formatCount = formatCount;
  formatRelativeTime = formatRelativeTime;
}

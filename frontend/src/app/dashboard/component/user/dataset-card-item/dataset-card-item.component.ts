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

import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgIf } from "@angular/common";
import { RouterLink } from "@angular/router";
import { firstValueFrom } from "rxjs";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { UserAvatarComponent } from "../user-avatar/user-avatar.component";
import { ShareAccessComponent } from "../share-access/share-access.component";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { DownloadService } from "../../../service/user/download/download.service";
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
  imports: [
    NgIf,
    RouterLink,
    NzCardComponent,
    NzIconDirective,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    UserAvatarComponent,
  ],
})
export class DatasetCardItemComponent implements OnChanges {
  @Input() currentUid: number | undefined;
  @Input() entry!: DashboardEntry;
  /** When true, render the management actions menu (share / download / delete). */
  @Input() showActions = false;

  @Output() deleted = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();

  entryLink: string[] = [];
  coverImageSrc: string = "";
  readonly defaultCover = "assets/card_background.jpg";
  likeCount = 0;
  viewCount = 0;
  isLiked = false;
  disableDelete = false;

  constructor(
    private datasetService: DatasetService,
    private downloadService: DownloadService,
    private hubService: HubService,
    private modalService: NzModalService,
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
    this.disableDelete = !this.entry.dataset.isOwner;

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

  async onClickOpenShareAccess(): Promise<void> {
    if (this.entry.type !== "dataset") return;
    const modal = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.entry.accessLevel === "WRITE",
        type: "dataset",
        id: this.entry.id,
        allOwners: await firstValueFrom(this.datasetService.retrieveOwners()),
      },
      nzFooter: null,
      nzTitle: "Share this dataset with others",
      nzCentered: true,
      nzWidth: "700px",
    });
    modal.componentInstance?.refresh.pipe(untilDestroyed(this)).subscribe(() => this.refresh.emit());
  }

  onClickDownload(): void {
    if (!isDefined(this.entry.id)) return;
    this.downloadService.downloadDataset(this.entry.id, this.entry.name).pipe(untilDestroyed(this)).subscribe();
  }

  onClickDelete(): void {
    if (this.disableDelete) return;
    this.modalService.confirm({
      nzTitle: "Confirm to delete this dataset.",
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () => this.deleted.emit(),
    });
  }

  formatSize = formatSize;
  formatCount = formatCount;
  formatRelativeTime = formatRelativeTime;
}

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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { FormsModule } from "@angular/forms";
import { NgIf, NgFor } from "@angular/common";
import { HttpErrorResponse } from "@angular/common/http";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzModalModule } from "ng-zorro-antd/modal";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { timer } from "rxjs";
import { filter, switchMap } from "rxjs/operators";
import { CuImage, CuImageService, CuImageStatus, isInProgress } from "../../../service/admin/cu-image/cu-image.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { extractErrorMessage } from "../../../../common/util/error";

/** Fast enough to feel live, slow enough not to hammer the API. */
const CHECK_POLL_INTERVAL_MS = 3000;

/** Enough of a digest to tell two apart; the tooltip carries the whole one. */
const DIGEST_CHARACTERS_SHOWN = 12;

@UntilDestroy()
@Component({
  selector: "texera-admin-cu-image",
  templateUrl: "./admin-cu-image.component.html",
  styleUrls: ["./admin-cu-image.component.scss"],
  imports: [
    FormsModule,
    NgIf,
    NgFor,
    NzCardComponent,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzInputDirective,
    NzTableModule,
    NzTagComponent,
    NzModalModule,
    NzTooltipDirective,
    NzPopconfirmDirective,
    NzAlertComponent,
  ],
})
export class AdminCuImageComponent implements OnInit {
  images: CuImage[] = [];
  loading = false;

  /** The API answers 503 when the deployment has curated images switched off. */
  featureDisabled = false;

  newName = "";
  newSourceRef = "";
  submitting = false;

  logVisible = false;
  logIid?: number;
  logName = "";
  logText = "";

  constructor(
    private cuImageService: CuImageService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.load();

    // A check finishes on the cluster without telling anyone, so poll while one is
    // running. The filter stops it once nothing is, on a page that is usually idle.
    timer(CHECK_POLL_INTERVAL_MS, CHECK_POLL_INTERVAL_MS)
      .pipe(
        filter(() => this.anyInProgress(this.images)),
        switchMap(() => this.cuImageService.list()),
        untilDestroyed(this)
      )
      .subscribe({
        next: images => {
          this.images = this.newestFirst(images);
          // Keep an open log in step with the check it is showing.
          if (this.logVisible && this.logIid !== undefined) {
            this.loadLog(this.logIid, false);
          }
        },
        error: () => {
          // Not worth a toast; the next tick tries again.
        },
      });
  }

  private anyInProgress(images: CuImage[]): boolean {
    return images.some(isInProgress);
  }

  /**
   * Newest first, so a just-registered image is at the top. Sorted here rather than in the
   * API because the same endpoint feeds the unit dropdown, where by-name is the useful order.
   */
  private newestFirst(images: CuImage[]): CuImage[] {
    return [...images].sort((a, b) => b.creationTime - a.creationTime);
  }

  load(): void {
    this.loading = true;
    this.cuImageService
      .list()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: images => {
          this.images = this.newestFirst(images);
          this.featureDisabled = false;
          this.loading = false;
        },
        error: (err: unknown) => {
          this.loading = false;
          if (err instanceof HttpErrorResponse && err.status === 503) {
            this.featureDisabled = true;
            return;
          }
          this.notificationService.error(`Could not load images: ${extractErrorMessage(err)}`);
        },
      });
  }

  add(): void {
    const name = this.newName.trim();
    const sourceRef = this.newSourceRef.trim();
    if (name === "" || sourceRef === "") {
      this.notificationService.error("Both a name and an image reference are required");
      return;
    }
    this.submitting = true;
    this.cuImageService
      .create(name, sourceRef)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.submitting = false;
          this.newName = "";
          this.newSourceRef = "";
          this.load();
        },
        error: (err: unknown) => {
          this.submitting = false;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  /** Re-checks the same reference: picks up a moved tag, retries a failed check. */
  refresh(image: CuImage): void {
    this.cuImageService
      .refresh(image.iid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.load(),
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
  }

  remove(image: CuImage): void {
    this.cuImageService
      .delete(image.iid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.load(),
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
  }

  showLog(image: CuImage): void {
    this.logIid = image.iid;
    this.logName = image.name;
    this.logText = "";
    this.logVisible = true;
    this.loadLog(image.iid, true);
  }

  closeLog(): void {
    this.logVisible = false;
    this.logIid = undefined;
  }

  /** `report` is false when polling, so a blip does not raise a toast. */
  private loadLog(iid: number, report: boolean): void {
    this.cuImageService
      .log(iid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: log => {
          if (this.logVisible && this.logIid === iid) {
            this.logText = log.log;
          }
        },
        error: (err: unknown) => {
          if (report) {
            this.notificationService.error(`Could not load the log: ${extractErrorMessage(err)}`);
          }
        },
      });
  }

  statusColor(status: CuImageStatus): string {
    switch (status) {
      case "READY":
        return "green";
      case "FAILED":
        return "red";
      default:
        return "blue";
    }
  }

  /** Whether a check is still running. */
  inProgress(image: CuImage): boolean {
    return isInProgress(image);
  }

  /** A 64-character digest would wrap over several lines and crowd the row. */
  shortRef(imageTag: string): string {
    const at = imageTag.indexOf("@sha256:");
    if (at < 0) {
      return imageTag;
    }
    const shown = DIGEST_CHARACTERS_SHOWN + "@sha256:".length;
    return imageTag.length <= at + shown ? imageTag : `${imageTag.slice(0, at + shown)}…`;
  }
}

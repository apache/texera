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
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SidebarTabs } from "../../../../common/type/gui-config";
import { forkJoin } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./admin-settings.component.html",
  styleUrls: ["./admin-settings.component.scss"],
})
export class AdminSettingsComponent implements OnInit {
  logoData: string | null = null;
  miniLogoData: string | null = null;
  faviconData: string | null = null;
  sidebarTabs: SidebarTabs = {
    hub_enabled: false,
    home_enabled: false,
    workflow_enabled: false,
    dataset_enabled: false,
    your_work_enabled: false,
    projects_enabled: false,
    workflows_enabled: false,
    datasets_enabled: false,
    quota_enabled: false,
    forum_enabled: false,
    about_enabled: false,
  };

  maxConcurrentFiles: number = 3;
  maxFileSizeMiB: number = 20;
  maxConcurrentChunks: number = 10;
  chunkSizeMiB: number = 50;

  // S3 Multipart Upload Constraints
  readonly MIN_PART_SIZE_MiB = 5; // 5 MiB minimum for parts (except last part)
  readonly MAX_PART_SIZE_MiB = 5120; // 5 GiB maximum per part (5 * 1024 MiB)
  readonly MAX_FILE_SIZE_MiB = 5242880; // 5 TiB maximum object size (5 * 1024 * 1024 MiB)
  readonly MAX_TOTAL_PARTS = 10000; // S3 maximum parts per upload

  private readonly RELOAD_DELAY = 1000;

  constructor(
    private adminSettingsService: AdminSettingsService,
    private message: NzMessageService
  ) {}
  ngOnInit(): void {
    this.loadBranding();
    this.loadTabs();
    this.loadDatasetSettings();
  }

  private loadBranding(): void {
    this.adminSettingsService
      .getSetting("logo")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.logoData = value || null));

    this.adminSettingsService
      .getSetting("mini_logo")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.miniLogoData = value || null));

    this.adminSettingsService
      .getSetting("favicon")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.faviconData = value || null));
  }

  private loadTabs(): void {
    (Object.keys(this.sidebarTabs) as (keyof SidebarTabs)[]).forEach(tab => {
      this.adminSettingsService
        .getSetting(tab)
        .pipe(untilDestroyed(this))
        .subscribe(value => (this.sidebarTabs[tab] = value === "true"));
    });
  }

  private loadDatasetSettings(): void {
    this.adminSettingsService
      .getSetting("max_number_of_concurrent_uploading_file")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxConcurrentFiles = parseInt(value)));
    this.adminSettingsService
      .getSetting("single_file_upload_max_size_mib")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxFileSizeMiB = parseInt(value)));
    this.adminSettingsService
      .getSetting("max_number_of_concurrent_uploading_file_chunks")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxConcurrentChunks = parseInt(value)));
    this.adminSettingsService
      .getSetting("multipart_upload_chunk_size_mib")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.chunkSizeMiB = parseInt(value)));
  }

  onFileChange(type: "logo" | "mini_logo" | "favicon", event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (file && file.type.startsWith("image/")) {
      const reader = new FileReader();
      reader.onload = e => {
        const result = typeof e.target?.result === "string" ? e.target.result : null;
        if (type === "logo") {
          this.logoData = result;
        } else if (type === "mini_logo") {
          this.miniLogoData = result;
        } else {
          this.faviconData = result;
        }
      };
      reader.readAsDataURL(file);
    } else {
      this.message.error("Please upload a valid image file.");
    }
  }

  saveLogos(): void {
    const saveRequests = [];
    if (this.logoData) {
      saveRequests.push(this.adminSettingsService.updateSetting("logo", this.logoData));
    }
    if (this.miniLogoData) {
      saveRequests.push(this.adminSettingsService.updateSetting("mini_logo", this.miniLogoData));
    }
    if (this.faviconData) {
      saveRequests.push(this.adminSettingsService.updateSetting("favicon", this.faviconData));
    }

    if (saveRequests.length > 0) {
      forkJoin(saveRequests)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.message.success("Branding saved successfully.");
            setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
          },
          error: () => this.message.error("Failed to save branding."),
        });
    }
  }

  resetBranding(): void {
    ["logo", "mini_logo", "favicon"].forEach(setting =>
      this.adminSettingsService.resetSetting(setting).pipe(untilDestroyed(this)).subscribe({})
    );

    this.message.info("Resetting branding...");
    setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
  }

  saveTabs(): void {
    const saveRequests = (Object.keys(this.sidebarTabs) as (keyof SidebarTabs)[]).map(tab =>
      this.adminSettingsService.updateSetting(tab, this.sidebarTabs[tab].toString())
    );

    forkJoin(saveRequests)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.message.success("Tabs saved successfully.");
          setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
        },
        error: () => this.message.error("Failed to save tabs."),
      });
  }

  resetTabs(): void {
    Object.keys(this.sidebarTabs).forEach(tab => {
      this.adminSettingsService.resetSetting(tab).pipe(untilDestroyed(this)).subscribe({});
    });

    this.message.info("Resetting tabs...");
    setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
  }

  // Computed properties
  get partsAtMax(): number {
    if (!this.maxFileSizeMiB || !this.chunkSizeMiB) return 0;
    return Math.ceil(this.maxFileSizeMiB / this.chunkSizeMiB);
  }

  get requiredMinPartSizeMiB(): number {
    if (!this.maxFileSizeMiB) return this.MIN_PART_SIZE_MiB;
    const byPartsLimit = Math.ceil(this.maxFileSizeMiB / this.MAX_TOTAL_PARTS);
    return Math.max(this.MIN_PART_SIZE_MiB, byPartsLimit);
  }

  saveDatasetSettings(): void {
    if (
      this.maxFileSizeMiB < 1 ||
      this.maxConcurrentFiles < 1 ||
      this.maxConcurrentChunks < 1 ||
      this.chunkSizeMiB < 1
    ) {
      this.message.error("Please enter valid integer values.");
      return;
    }

    if (this.partsAtMax > this.MAX_TOTAL_PARTS) {
      this.message.error(
        `This setting would create ${this.partsAtMax.toLocaleString()} parts (exceeds 10,000 limit). ` +
          `Increase "Part Size" to at least ${this.requiredMinPartSizeMiB} MiB or reduce "File Size".`
      );
      return;
    }

    const saveRequests = [
      this.adminSettingsService.updateSetting(
        "max_number_of_concurrent_uploading_file",
        this.maxConcurrentFiles.toString()
      ),
      this.adminSettingsService.updateSetting("single_file_upload_max_size_mib", this.maxFileSizeMiB.toString()),
      this.adminSettingsService.updateSetting(
        "max_number_of_concurrent_uploading_file_chunks",
        this.maxConcurrentChunks.toString()
      ),
      this.adminSettingsService.updateSetting("multipart_upload_chunk_size_mib", this.chunkSizeMiB.toString()),
    ];

    forkJoin(saveRequests)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.message.success("Dataset upload settings saved successfully."),
        error: () => this.message.error("Failed to save dataset settings."),
      });
  }

  resetDatasetSettings(): void {
    [
      "max_number_of_concurrent_uploading_file",
      "single_file_upload_max_size_mib",
      "max_number_of_concurrent_uploading_file_chunks",
      "multipart_upload_chunk_size_mib",
    ].forEach(setting => this.adminSettingsService.resetSetting(setting).pipe(untilDestroyed(this)).subscribe({}));

    this.message.info("Resetting dataset settings...");
    setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
  }
}

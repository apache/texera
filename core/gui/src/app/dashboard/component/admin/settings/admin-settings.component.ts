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

  maxFileSizeMB: number = 20;
  maxConcurrentChunks: number = 10;
  chunkSizeMB: number = 50;

  // S3 Multipart Upload Constraints
  readonly MIN_PART_SIZE_MB = 5; // 5 MiB minimum for parts (except last part)
  readonly MAX_PART_SIZE_MB = 5120; // 5 GiB maximum per part (5 * 1024 MiB)
  readonly MAX_FILE_SIZE_MB = 5242880; // 5 TiB maximum object size (5 * 1024 * 1024 MiB)
  readonly MAX_TOTAL_PARTS = 10000; // S3 maximum parts per upload

  constructor(
    private adminSettingsService: AdminSettingsService,
    private message: NzMessageService
  ) {}
  ngOnInit(): void {
    this.loadTabs();
    this.loadDatasetSetting();
  }

  private loadTabs(): void {
    (Object.keys(this.sidebarTabs) as (keyof SidebarTabs)[]).forEach(tab => {
      this.adminSettingsService
        .getSetting(tab)
        .pipe(untilDestroyed(this))
        .subscribe(value => (this.sidebarTabs[tab] = value === "true"));
    });
  }

  private loadDatasetSetting(): void {
    this.adminSettingsService
      .getSetting("single_file_upload_max_size_mb")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxFileSizeMB = parseInt(value)));
    this.adminSettingsService
      .getSetting("max_number_of_concurrent_uploading_file_chunks")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.maxConcurrentChunks = parseInt(value)));
    this.adminSettingsService
      .getSetting("multipart_upload_chunk_size_mb")
      .pipe(untilDestroyed(this))
      .subscribe(value => (this.chunkSizeMB = parseInt(value)));
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
    if (this.logoData) {
      this.adminSettingsService
        .updateSetting("logo", this.logoData)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => this.message.success("Logo saved successfully."),
          error: () => this.message.error("Failed to save logo."),
        });
    }

    if (this.miniLogoData) {
      this.adminSettingsService
        .updateSetting("mini_logo", this.miniLogoData)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => this.message.success("Mini logo saved successfully."),
          error: () => this.message.error("Failed to save mini logo."),
        });
    }

    if (this.faviconData) {
      this.adminSettingsService
        .updateSetting("favicon", this.faviconData)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => this.message.success("Favicon saved successfully."),
          error: () => this.message.error("Failed to save favicon."),
        });
    }

    if (this.logoData || this.miniLogoData || this.faviconData) {
      setTimeout(() => window.location.reload(), 500);
    }
  }

  resetBranding(): void {
    this.adminSettingsService
      .resetSetting("logo")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.logoData = null;
          this.message.success("Logo reset to default.");
        },
        error: () => this.message.error("Failed to reset logo."),
      });

    this.adminSettingsService
      .resetSetting("mini_logo")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.miniLogoData = null;
          this.message.success("Mini logo reset to default.");
        },
        error: () => this.message.error("Failed to reset mini logo."),
      });

    this.adminSettingsService
      .resetSetting("favicon")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.faviconData = null;
          this.message.success("Favicon reset to default.");
        },
        error: () => this.message.error("Failed to reset favicon."),
      });

    setTimeout(() => window.location.reload(), 500);
  }

  saveTabs(tab: keyof SidebarTabs): void {
    const displayTab = tab
      .replace("_enabled", "")
      .split("_")
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");

    this.adminSettingsService
      .updateSetting(tab, this.sidebarTabs[tab].toString())
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.message.success(`${displayTab} tab saved successfully.`),
        error: () => this.message.error(`Failed to save ${displayTab} tab.`),
      });
  }

  resetTabs(): void {
    Object.keys(this.sidebarTabs).forEach(tab => {
      this.adminSettingsService.resetSetting(tab).pipe(untilDestroyed(this)).subscribe({});
    });

    this.message.info("Resetting tabs...");
    setTimeout(() => window.location.reload(), 500);
  }

  saveDatasetSettings(): void {
    if (this.maxFileSizeMB < 1 || this.maxConcurrentChunks < 1 || this.chunkSizeMB < 1) {
      this.message.error("Please enter valid integer values.");
      return;
    }

    const partsAtMax = Math.ceil(this.maxFileSizeMB / this.chunkSizeMB);
    if (partsAtMax > this.MAX_TOTAL_PARTS) {
      const requiredMin = Math.max(this.MIN_PART_SIZE_MB, Math.ceil(this.maxFileSizeMB / this.MAX_TOTAL_PARTS));
      this.message.error(
        `This setting would create ${partsAtMax.toLocaleString()} parts (>10,000). ` +
          `Increase "Part Size" to at least ${requiredMin} MB or reduce "File Size".`
      );
      return;
    }

    const saveRequests = [
      this.adminSettingsService.updateSetting("single_file_upload_max_size_mb", this.maxFileSizeMB.toString()),
      this.adminSettingsService.updateSetting(
        "max_number_of_concurrent_uploading_file_chunks",
        this.maxConcurrentChunks.toString()
      ),
      this.adminSettingsService.updateSetting("multipart_upload_chunk_size_mb", this.chunkSizeMB.toString()),
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
      "single_file_upload_max_size_mb",
      "max_number_of_concurrent_uploading_file_chunks",
      "multipart_upload_chunk_size_mb",
    ].forEach(setting => this.adminSettingsService.resetSetting(setting).pipe(untilDestroyed(this)).subscribe({}));

    this.message.info("Resetting dataset settings...");
    setTimeout(() => window.location.reload(), 500);
  }
}

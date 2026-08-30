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
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SidebarTabs } from "../../../../common/type/gui-config";
import { parseIntOrDefault } from "../../../../common/util/format.util";
import {
  DATASET_FILE_RESOURCE_ENDPOINT,
  FileResourceEndpoint,
  MODEL_FILE_RESOURCE_ENDPOINT,
} from "../../../service/user/file-resource/file-resource-endpoint";
import { forkJoin } from "rxjs";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NgFor, NgIf, DecimalPipe } from "@angular/common";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSwitchComponent } from "ng-zorro-antd/switch";
import { FormsModule } from "@angular/forms";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzInputNumberComponent } from "ng-zorro-antd/input-number";

/** The four multipart-upload settings one resource family exposes on this page. */
export interface UploadSettingsForm {
  maxConcurrentFiles: number;
  maxFileSizeMiB: number;
  maxConcurrentChunks: number;
  chunkSizeMiB: number;
}

/** One upload card: a resource family, the site_settings keys it owns, and the edited values. */
export interface UploadSettingsGroup {
  /** Title case of the endpoint's own label, so a family is never named twice. */
  readonly label: string;
  readonly endpoint: FileResourceEndpoint;
  readonly form: UploadSettingsForm;
}

// Shared across families in default.conf; only the per-file ceiling differs, and the endpoint
// descriptor already carries that.
const DEFAULT_MAX_CONCURRENT_FILES = 3;
const DEFAULT_MAX_CONCURRENT_CHUNKS = 10;
const DEFAULT_CHUNK_SIZE_MIB = 50;

const uploadGroup = (endpoint: FileResourceEndpoint): UploadSettingsGroup => ({
  label: endpoint.label.charAt(0).toUpperCase() + endpoint.label.slice(1),
  endpoint,
  form: {
    maxConcurrentFiles: DEFAULT_MAX_CONCURRENT_FILES,
    maxFileSizeMiB: endpoint.defaultMaxFileSizeMiB,
    maxConcurrentChunks: DEFAULT_MAX_CONCURRENT_CHUNKS,
    chunkSizeMiB: DEFAULT_CHUNK_SIZE_MIB,
  },
});

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./admin-settings.component.html",
  styleUrls: ["./admin-settings.component.scss"],
  imports: [
    NzCardComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NgIf,
    NgFor,
    NzIconDirective,
    NzSwitchComponent,
    FormsModule,
    NzTooltipDirective,
    NzInputNumberComponent,
    DecimalPipe,
  ],
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
    compute_enabled: false,
    datasets_enabled: false,
    models_enabled: false,
    quota_enabled: false,
    forum_enabled: false,
    about_enabled: false,
  };

  // One card per resource family; adding a family here adds its card, with no new save/reset code.
  readonly uploadGroups: UploadSettingsGroup[] = [
    uploadGroup(DATASET_FILE_RESOURCE_ENDPOINT),
    uploadGroup(MODEL_FILE_RESOURCE_ENDPOINT),
  ];

  csvMaxColumns: number = 512;

  // S3 Multipart Upload Constraints
  readonly MIN_PART_SIZE_MiB = 5; // 5 MiB minimum for parts (except last part)
  readonly MAX_PART_SIZE_MiB = 5120; // 5 GiB maximum per part (5 * 1024 MiB)
  readonly MAX_FILE_SIZE_MiB = 5242880; // 5 TiB maximum object size (5 * 1024 * 1024 MiB)
  readonly MAX_TOTAL_PARTS = 10000; // S3 maximum parts per upload

  readonly MIN_CSV_MAX_COLUMNS = 1;
  readonly MAX_CSV_MAX_COLUMNS = 100000;

  private readonly RELOAD_DELAY = 1000;

  // Guards the save buttons: a failed bulk load leaves every field at its
  // initializer, so saving would persist those placeholders (e.g. disabling
  // every sidebar tab). Only allow saves once a load has actually succeeded.
  private settingsLoaded = false;

  constructor(
    private adminSettingsService: AdminSettingsService,
    private message: NzMessageService,
    private notificationService: NotificationService
  ) {}
  ngOnInit(): void {
    this.loadSettings();
  }

  // One bulk read instead of a request per key; missing or unparsable values
  // keep the field initializers above as their defaults.
  private loadSettings(): void {
    this.adminSettingsService
      .getAllSettings()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: settings => {
          this.logoData = settings["logo"] || null;
          this.miniLogoData = settings["mini_logo"] || null;
          this.faviconData = settings["favicon"] || null;
          (Object.keys(this.sidebarTabs) as (keyof SidebarTabs)[]).forEach(
            tab => (this.sidebarTabs[tab] = settings[tab] === "true")
          );
          this.uploadGroups.forEach(({ endpoint, form }) => {
            form.maxConcurrentFiles = parseIntOrDefault(
              settings[endpoint.maxConcurrentFilesSettingKey],
              form.maxConcurrentFiles
            );
            form.maxFileSizeMiB = parseIntOrDefault(settings[endpoint.maxFileSizeSettingKey], form.maxFileSizeMiB);
            form.maxConcurrentChunks = parseIntOrDefault(
              settings[endpoint.maxConcurrentChunksSettingKey],
              form.maxConcurrentChunks
            );
            form.chunkSizeMiB = parseIntOrDefault(settings[endpoint.chunkSizeSettingKey], form.chunkSizeMiB);
          });
          this.csvMaxColumns = parseIntOrDefault(settings["csv_parser_max_columns"], this.csvMaxColumns);
          this.settingsLoaded = true;
        },
        error: () => this.message.error("Failed to load settings."),
      });
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
    if (!this.settingsLoaded) {
      this.message.error("Settings have not loaded; refresh before saving.");
      return;
    }
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
  partsAtMax(form: UploadSettingsForm): number {
    if (!form.maxFileSizeMiB || !form.chunkSizeMiB) return 0;
    return Math.ceil(form.maxFileSizeMiB / form.chunkSizeMiB);
  }

  requiredMinPartSizeMiB(form: UploadSettingsForm): number {
    if (!form.maxFileSizeMiB) return this.MIN_PART_SIZE_MiB;
    const byPartsLimit = Math.ceil(form.maxFileSizeMiB / this.MAX_TOTAL_PARTS);
    return Math.max(this.MIN_PART_SIZE_MiB, byPartsLimit);
  }

  // The four key/value pairs of one group, in the order the card lists them.
  private settingEntries({ endpoint, form }: UploadSettingsGroup): Array<[string, number]> {
    return [
      [endpoint.maxConcurrentFilesSettingKey, form.maxConcurrentFiles],
      [endpoint.maxFileSizeSettingKey, form.maxFileSizeMiB],
      [endpoint.maxConcurrentChunksSettingKey, form.maxConcurrentChunks],
      [endpoint.chunkSizeSettingKey, form.chunkSizeMiB],
    ];
  }

  saveUploadSettings(group: UploadSettingsGroup): void {
    if (!this.settingsLoaded) {
      this.message.error("Settings have not loaded; refresh before saving.");
      return;
    }
    const { form } = group;
    if (
      form.maxFileSizeMiB < 1 ||
      form.maxConcurrentFiles < 1 ||
      form.maxConcurrentChunks < 1 ||
      form.chunkSizeMiB < 1
    ) {
      this.message.error("Please enter valid integer values.");
      return;
    }

    if (this.partsAtMax(form) > this.MAX_TOTAL_PARTS) {
      this.message.error(
        `This setting would create ${this.partsAtMax(form).toLocaleString()} parts (exceeds 10,000 limit). ` +
          `Increase "Part Size" to at least ${this.requiredMinPartSizeMiB(form)} MiB or reduce "File Size".`
      );
      return;
    }

    const saveRequests = this.settingEntries(group).map(([key, value]) =>
      this.adminSettingsService.updateSetting(key, value.toString())
    );

    forkJoin(saveRequests)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.message.success(`${group.label} upload settings saved successfully.`),
        error: () => this.message.error(`Failed to save ${group.endpoint.label} settings.`),
      });
  }

  resetUploadSettings(group: UploadSettingsGroup): void {
    this.settingEntries(group).forEach(([key]) =>
      this.adminSettingsService.resetSetting(key).pipe(untilDestroyed(this)).subscribe({})
    );

    this.message.info(`Resetting ${group.endpoint.label} settings...`);
    setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
  }

  saveCsvSettings(): void {
    if (!this.settingsLoaded) {
      this.message.error("Settings have not loaded; refresh before saving.");
      return;
    }
    const saveRequests = [
      this.adminSettingsService.updateSetting("csv_parser_max_columns", this.csvMaxColumns.toString()),
    ];

    forkJoin(saveRequests)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Result panel settings saved."),
        error: () => this.notificationService.error("Could not save result panel settings."),
      });
  }

  resetCsvSettings(): void {
    this.adminSettingsService
      .resetSetting("csv_parser_max_columns")
      .pipe(untilDestroyed(this))
      .subscribe({
        error: () => this.notificationService.error("Could not reset result panel settings."),
      });

    this.notificationService.info("Resetting result panel settings...");
    setTimeout(() => window.location.reload(), this.RELOAD_DELAY);
  }
}

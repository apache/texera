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

import { Component } from "@angular/core";
import { AdminSettingsService } from "../../../service/admin/settings/admin-settings.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./admin-settings.component.html",
  styleUrls: ["./admin-settings.component.scss"],
})
export class AdminSettingsComponent {
  logoData: string | null = null;
  miniLogoData: string | null = null;
  faviconData: string | null = null;

  constructor(
    private adminSettingsService: AdminSettingsService,
    private message: NzMessageService
  ) {}

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

  resetToDefault(): void {
    this.adminSettingsService
      .deleteSetting("logo")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.logoData = null;
          this.message.success("Logo reset to default.");
        },
        error: () => this.message.error("Failed to reset logo."),
      });

    this.adminSettingsService
      .deleteSetting("mini_logo")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.miniLogoData = null;
          this.message.success("Mini logo reset to default.");
        },
        error: () => this.message.error("Failed to reset mini logo."),
      });

    this.adminSettingsService
      .deleteSetting("favicon")
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
}

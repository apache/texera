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
import { AdminSettingsService, SiteSetting } from "../../../service/admin/settings/admin-settings.service";
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
  faviconData: string | null = null;

  constructor(
    private settingsSvc: AdminSettingsService,
    private message: NzMessageService
  ) {}

  onFileChange(type: "logo" | "favicon", event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (file && file.type.startsWith("image/")) {
      const reader = new FileReader();
      reader.onload = e => {
        const result = typeof e.target?.result === "string" ? e.target.result : null;
        if (type === "logo") {
          this.logoData = result;
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
    const settings: SiteSetting[] = [];
    if (this.logoData) settings.push({ key: "logo", value: this.logoData });
    if (this.faviconData) settings.push({ key: "favicon", value: this.faviconData });
    if (settings.length === 0) {
      this.message.info("No changes to save.");
      return;
    }
    this.settingsSvc
      .updateSettings(settings)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.message.success("Logo settings saved.");
          window.location.reload();
        },
        error: () => this.message.error("Failed to save logos."),
      });
  }

  resetToDefault(): void {
    this.settingsSvc
      .deleteSettings(["logo", "favicon"])
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.message.success("Logos reset to default.");
          window.location.reload();
        },
        error: () => this.message.error("Failed to reset logos."),
      });
  }
}

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
import { SiteSettingsService, SiteSetting } from "../../../service/admin/site-settings/admin-site-settings.service";
import { NzMessageService } from "ng-zorro-antd/message";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
@UntilDestroy()
@Component({
  selector: "texera-site-settings",
  templateUrl: "./site-settings.component.html",
  styleUrls: ["./site-settings.component.scss"],
})
export class SiteSettingsComponent implements OnInit {
  mainLogo: string = "assets/logos/logo.png";
  miniLogo: string = "assets/logos/favicon-32x32.png";
  mainLogoPreview: string | null = null;
  miniLogoPreview: string | null = null;

  editMiniLogo = false;
  showLogoUpload = false;

  constructor(
    private settingsSvc: SiteSettingsService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.settingsSvc
      .getAllSettings()
      .pipe(untilDestroyed(this))
      .subscribe(settings => {
        const main = settings.find(s => s.key === "main_logo")?.value;
        const mini = settings.find(s => s.key === "mini_logo")?.value;
        this.mainLogo = main || "assets/logos/logo.png";
        this.miniLogo = mini || main || "assets/logos/favicon-32x32.png";
      });
  }

  toggleLogoUpload(): void {
    this.showLogoUpload = !this.showLogoUpload;
    if (this.showLogoUpload) {
      this.mainLogoPreview = null;
      this.miniLogoPreview = null;
      this.editMiniLogo = false;
    }
  }

  onMainLogoChange(event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file || !file.type.startsWith("image/")) {
      this.message.error("Please upload a valid image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = e => (this.mainLogoPreview = typeof e.target?.result === "string" ? e.target.result : null);
    reader.readAsDataURL(file);
  }

  onMiniLogoChange(event: Event): void {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (!file || !file.type.startsWith("image/")) {
      this.message.error("Please upload an image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = e => (this.miniLogoPreview = typeof e.target?.result === "string" ? e.target.result : null);
    reader.readAsDataURL(file);
  }

  onMiniLogoCheckboxChange(): void {
    if (!this.editMiniLogo) {
      this.miniLogoPreview = null;
    }
  }

  saveAllLogos(): void {
    const mainValue = this.mainLogoPreview || "";

    const miniValue = this.editMiniLogo && this.miniLogoPreview ? this.miniLogoPreview : mainValue;

    const settings: SiteSetting[] = [
      { key: "main_logo", value: mainValue },
      { key: "mini_logo", value: miniValue },
    ];

    this.settingsSvc.updateSettings(settings).subscribe({
      next: () => {
        this.message.success("Logos updated!");
        window.location.reload();
      },
      error: () => this.message.error("Failed to save logos."),
    });
  }

  resetAllLogos(): void {
    this.mainLogoPreview = null;
    this.miniLogoPreview = null;
    this.editMiniLogo = false;

    this.settingsSvc.deleteSettings(["main_logo", "mini_logo"]).subscribe({
      next: () => {
        this.message.info("All logos reset to default.");
        window.location.reload();
      },
      error: () => this.message.error("Failed to reset logos."),
    });
  }
}

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

import { Component, ElementRef, OnInit, ViewChild } from "@angular/core";
import { NzMessageService } from "ng-zorro-antd/message";

@Component({
  selector: "site-settings",
  templateUrl: "./site-settings.component.html",
  styleUrls: ["./site-settings.component.scss"],
})
export class SiteSettingsComponent implements OnInit {
  // Main and mini logo states (for preview before saving)
  logoPreview: string | null = null;
  miniLogoPreview: string | null = null;
  showLogoUpload = false;
  editMiniLogo = false; // checkbox for uploading separate mini logo

  readonly LOCAL_LOGO_KEY = "customSiteLogo";
  readonly LOCAL_MINI_LOGO_KEY = "customMiniLogo";

  @ViewChild("logoInput") logoInputRef!: ElementRef<HTMLInputElement>;
  @ViewChild("miniLogoInput") miniLogoInputRef!: ElementRef<HTMLInputElement>;

  constructor(private message: NzMessageService) {}

  ngOnInit(): void {}

  toggleLogoUpload(): void {
    this.showLogoUpload = !this.showLogoUpload;
    if (this.showLogoUpload) {
      this.logoPreview = null;
      this.miniLogoPreview = null;
      this.editMiniLogo = false;
      setTimeout(() => {
        if (this.logoInputRef) this.logoInputRef.nativeElement.value = "";
        if (this.miniLogoInputRef) this.miniLogoInputRef.nativeElement.value = "";
      });
    }
  }

  // Main logo file input
  onLogoSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file || !file.type.startsWith("image/")) {
      this.message.error("Please upload a valid image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = e => (this.logoPreview = typeof e.target?.result === "string" ? e.target.result : null);
    reader.readAsDataURL(file);
  }

  // Mini logo file input
  onMiniLogoSelected(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file || !file.type.startsWith("image/")) {
      this.message.error("Please upload an image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = e => (this.miniLogoPreview = typeof e.target?.result === "string" ? e.target.result : null);
    reader.readAsDataURL(file);
  }

  // Checkbox toggle for mini logo
  onMiniLogoCheckboxChange(): void {
    if (!this.editMiniLogo) {
      this.miniLogoPreview = null;
      localStorage.removeItem(this.LOCAL_MINI_LOGO_KEY);
      if (this.miniLogoInputRef) this.miniLogoInputRef.nativeElement.value = "";
    }
  }

  // Save main and mini logos to localStorage
  saveAllLogos(): void {
    if (this.logoPreview) {
      localStorage.setItem(this.LOCAL_LOGO_KEY, this.logoPreview);
      if (!this.editMiniLogo) {
        localStorage.setItem(this.LOCAL_MINI_LOGO_KEY, this.logoPreview);
        this.miniLogoPreview = this.logoPreview;
      }
    } else {
      localStorage.removeItem(this.LOCAL_LOGO_KEY);
      if (!this.editMiniLogo) {
        localStorage.removeItem(this.LOCAL_MINI_LOGO_KEY);
        this.miniLogoPreview = null;
      }
    }
    if (this.editMiniLogo && this.miniLogoPreview) {
      localStorage.setItem(this.LOCAL_MINI_LOGO_KEY, this.miniLogoPreview);
    }
    this.message.success("Logos saved!");
    this.showLogoUpload = false;
  }

  // Reset both logos
  resetAllLogos(): void {
    localStorage.removeItem(this.LOCAL_LOGO_KEY);
    localStorage.removeItem(this.LOCAL_MINI_LOGO_KEY);
    this.logoPreview = null;
    this.miniLogoPreview = null;
    this.editMiniLogo = false;
    this.showLogoUpload = false;
    this.message.info("All logos reset to default.");
    setTimeout(() => {
      if (this.logoInputRef) this.logoInputRef.nativeElement.value = "";
      if (this.miniLogoInputRef) this.miniLogoInputRef.nativeElement.value = "";
    });
  }

  // Main logo for sidebar (or default)
  getSidebarLogo(): string {
    return localStorage.getItem(this.LOCAL_LOGO_KEY) || "assets/logos/logo.png";
  }

  // Mini logo for sidebar, or fallback to main logo, or default mini icon
  getMiniLogo(): string {
    const mini = localStorage.getItem(this.LOCAL_MINI_LOGO_KEY);
    if (mini) return mini;
    const main = localStorage.getItem(this.LOCAL_LOGO_KEY);
    if (main) return main;
    return "assets/logos/favicon-32x32.png";
  }
}

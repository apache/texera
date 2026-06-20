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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { HttpClient } from "@angular/common/http";
import { NzButtonModule } from "ng-zorro-antd/button";
import { firstValueFrom } from "rxjs";
import { AppSettings } from "../../../common/app-setting";

interface HuggingFaceAudioUploadResponse {
  path: string;
  fileName: string;
}

@Component({
  selector: "texera-hugging-face-audio-upload",
  templateUrl: "./hugging-face-audio-upload.component.html",
  styleUrls: ["./hugging-face-audio-upload.component.scss"],
  imports: [CommonModule, NzButtonModule],
})
export class HuggingFaceAudioUploadComponent extends FieldType<FieldTypeConfig> implements OnInit, OnDestroy {
  fileName = "";
  errorMessage = "";
  isUploading = false;
  private localPreviewUrl = "";

  ngOnInit(): void {
    if (typeof this.formControl.value === "string" && this.formControl.value.trim().length > 0) {
      this.fileName = this.getDisplayName(this.formControl.value);
    }
  }

  constructor(private http: HttpClient) {
    super();
  }

  get previewSrc(): string {
    if (this.localPreviewUrl) {
      return this.localPreviewUrl;
    }
    const value = this.formControl.value;
    if (typeof value !== "string" || value.trim().length === 0) {
      return "";
    }
    if (value.startsWith("data:audio/")) {
      return value;
    }
    return `${AppSettings.getApiEndpoint()}/huggingface/audio-preview?path=${encodeURIComponent(value)}`;
  }

  ngOnDestroy(): void {
    this.revokePreviewUrl();
  }

  async onFileSelected(event: Event): Promise<void> {
    if (this.isUploading) {
      return;
    }
    this.errorMessage = "";
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];

    if (!file) {
      return;
    }
    if (!file.type.startsWith("audio/")) {
      this.errorMessage = "Choose an audio file.";
      input.value = "";
      return;
    }
    this.revokePreviewUrl();
    const previewUrl = URL.createObjectURL(file);
    this.localPreviewUrl = previewUrl;
    this.isUploading = true;

    try {
      const response = await firstValueFrom(
        this.http.post<HuggingFaceAudioUploadResponse>(
          `${AppSettings.getApiEndpoint()}/huggingface/upload-audio?filename=${encodeURIComponent(file.name)}`,
          file,
          {
            headers: {
              "Content-Type": "application/octet-stream",
            },
          }
        )
      );
      // If the user clicked Clear while the upload was in flight,
      // localPreviewUrl will have been revoked/reset — discard the stale response.
      if (this.localPreviewUrl !== previewUrl) return;
      this.fileName = response.fileName || file.name;
      this.formControl.setValue(response.path);
      if (typeof this.key === "string" && this.model) {
        this.model[this.key] = response.path;
      }
      this.formControl.markAsDirty();
      this.formControl.markAsTouched();
      this.formControl.updateValueAndValidity();
    } catch {
      if (this.localPreviewUrl !== previewUrl) return;
      this.clearAudio(input, false);
      this.errorMessage = "Could not upload this audio file.";
    } finally {
      this.isUploading = false;
    }
  }

  clearAudio(input: HTMLInputElement, clearError: boolean = true): void {
    this.fileName = "";
    if (clearError) {
      this.errorMessage = "";
    }
    this.isUploading = false;
    this.revokePreviewUrl();
    input.value = "";
    this.formControl.setValue("");
    if (typeof this.key === "string" && this.model) {
      this.model[this.key] = "";
    }
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
    this.formControl.updateValueAndValidity();
  }

  private revokePreviewUrl(): void {
    if (this.localPreviewUrl) {
      URL.revokeObjectURL(this.localPreviewUrl);
      this.localPreviewUrl = "";
    }
  }

  private getDisplayName(value: string): string {
    const trimmedValue = value.trim();
    if (!trimmedValue) {
      return "";
    }
    if (trimmedValue.startsWith("data:audio/")) {
      return "Selected audio";
    }
    const segments = trimmedValue.split(/[\\/]/);
    return segments[segments.length - 1] || "Selected audio";
  }
}

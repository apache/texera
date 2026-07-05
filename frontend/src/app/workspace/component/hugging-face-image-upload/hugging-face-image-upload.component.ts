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
import { CommonModule } from "@angular/common";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { NzButtonModule } from "ng-zorro-antd/button";

// Keep in sync with PythonCodegenBase._compress_image_bytes(max_bytes) on the backend:
// the uploaded data URL must stay within the size the inference helpers expect.
const MAX_DATA_URL_LENGTH = 45000;
const INITIAL_MAX_DIMENSION = 512;
const MIN_MAX_DIMENSION = 160;
const INITIAL_JPEG_QUALITY = 0.75;
const MIN_JPEG_QUALITY = 0.35;

@Component({
  selector: "texera-hugging-face-image-upload",
  templateUrl: "./hugging-face-image-upload.component.html",
  styleUrls: ["./hugging-face-image-upload.component.scss"],
  imports: [CommonModule, NzButtonModule],
})
export class HuggingFaceImageUploadComponent extends FieldType<FieldTypeConfig> {
  fileName = "";
  errorMessage = "";

  get hasImage(): boolean {
    const value = this.formControl.value;
    return typeof value === "string" && value.startsWith("data:image/");
  }

  get previewSrc(): string {
    return this.hasImage ? this.formControl.value : "";
  }

  get displayFileName(): string {
    if (this.fileName) return this.fileName;
    if (this.hasImage) return "Uploaded image";
    return "";
  }

  async onFileSelected(event: Event): Promise<void> {
    this.errorMessage = "";
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];

    if (!file) {
      return;
    }
    if (!file.type.startsWith("image/")) {
      this.errorMessage = "Choose an image file.";
      input.value = "";
      return;
    }

    try {
      const dataUrl = await this.compressImage(file);
      this.fileName = file.name;
      this.formControl.setValue(dataUrl);
      if (typeof this.key === "string" && this.model) {
        this.model[this.key] = dataUrl;
      }
      this.formControl.markAsDirty();
      this.formControl.markAsTouched();
      this.formControl.updateValueAndValidity();
    } catch {
      this.errorMessage = "Could not prepare this image. Try a smaller image file.";
      input.value = "";
    }
  }

  private compressImage(file: File): Promise<string> {
    const reader = new FileReader();
    const image = new Image();

    return new Promise((resolve, reject) => {
      reader.onload = () => {
        if (typeof reader.result !== "string") {
          reject();
          return;
        }
        image.onload = () => {
          const compressed = this.renderCompressedDataUrl(image);
          if (!compressed.startsWith("data:image/") || compressed.length > MAX_DATA_URL_LENGTH) {
            reject();
            return;
          }
          resolve(compressed);
        };
        image.onerror = () => reject();
        image.src = reader.result;
      };
      reader.onerror = () => reject();
      reader.readAsDataURL(file);
    });
  }

  private renderCompressedDataUrl(image: HTMLImageElement): string {
    let maxDimension = INITIAL_MAX_DIMENSION;
    let quality = INITIAL_JPEG_QUALITY;
    let bestDataUrl = "";

    while (maxDimension >= MIN_MAX_DIMENSION) {
      const scale = Math.min(1, maxDimension / Math.max(image.width, image.height));
      const width = Math.max(1, Math.round(image.width * scale));
      const height = Math.max(1, Math.round(image.height * scale));
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d");

      if (!ctx) {
        return bestDataUrl;
      }

      ctx.drawImage(image, 0, 0, width, height);
      quality = INITIAL_JPEG_QUALITY;

      while (quality >= MIN_JPEG_QUALITY) {
        const dataUrl = canvas.toDataURL("image/jpeg", quality);
        bestDataUrl = dataUrl;
        if (dataUrl.length <= MAX_DATA_URL_LENGTH) {
          return dataUrl;
        }
        quality -= 0.1;
      }

      maxDimension = Math.floor(maxDimension * 0.75);
    }

    return bestDataUrl;
  }

  clearImage(input: HTMLInputElement): void {
    this.fileName = "";
    this.errorMessage = "";
    input.value = "";
    this.formControl.setValue("");
    if (typeof this.key === "string" && this.model) {
      this.model[this.key] = "";
    }
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
    this.formControl.updateValueAndValidity();
  }
}

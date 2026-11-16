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
import { NzButtonModule } from "ng-zorro-antd/button";
import { HuggingFaceMediaUploadBase } from "../hugging-face-media-upload.base";

// Cap the raw audio size before it is inlined as a base64 data URL into the
// workflow JSON. Mirrors the image-upload component's data-URL approach so the
// value the operator stores is directly consumable by the generated Python
// (`_read_audio_input` decodes `data:` URLs); avoids a server-side temp file
// the Python worker cannot read in distributed deployments.
const MAX_AUDIO_BYTES = 25 * 1024 * 1024;

@Component({
  selector: "texera-hugging-face-audio-upload",
  templateUrl: "./hugging-face-audio-upload.component.html",
  styleUrls: ["./hugging-face-audio-upload.component.scss"],
  imports: [CommonModule, NzButtonModule],
})
export class HuggingFaceAudioUploadComponent extends HuggingFaceMediaUploadBase {
  protected readonly dataUrlPrefix = "data:audio/";
  protected readonly mediaFallbackLabel = "Selected audio";

  async onFileSelected(event: Event): Promise<void> {
    this.errorMessage = "";
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];

    if (!file) {
      return;
    }
    if (!file.type.startsWith("audio/")) {
      this.errorMessage = "Choose an audio file.";
      this.resetSelection(input);
      return;
    }
    if (file.size > MAX_AUDIO_BYTES) {
      this.errorMessage = "Audio file is too large (max 25 MB).";
      this.resetSelection(input);
      return;
    }

    try {
      const dataUrl = await this.readAsDataUrl(file);
      if (!dataUrl.startsWith("data:audio/")) {
        throw new Error("Not an audio data URL");
      }
      this.setMediaValue(dataUrl, file.name);
    } catch {
      this.errorMessage = "Could not read this audio file.";
      this.resetSelection(input);
    }
  }

  clearAudio(input: HTMLInputElement): void {
    this.clearMedia(input);
  }

  private readAsDataUrl(file: File): Promise<string> {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        if (typeof reader.result === "string") {
          resolve(reader.result);
        } else {
          reject(new Error("Unexpected FileReader result"));
        }
      };
      reader.onerror = () => reject(reader.error ?? new Error("Audio read failed"));
      reader.readAsDataURL(file);
    });
  }
}

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

import { Injectable } from "@angular/core";

/**
 * Stores an optional custom top image per card, purely in the browser's
 * localStorage, as a (downscaled) data URL. Keyed by `${type}:${id}` so it is
 * stable across reloads but scoped to a single browser. No backend involvement.
 */

const BACKGROUNDS_STORAGE_KEY = "texera.card.backgrounds";

/** Longest edge (px) a custom card image is downscaled to before being stored. */
const MAX_IMAGE_EDGE = 640;
/** JPEG quality used when re-encoding a custom card image for storage. */
const IMAGE_QUALITY = 0.8;

@Injectable({
  providedIn: "root",
})
export class CardBackgroundService {
  private backgrounds = new Map<string, string>(
    Object.entries(this.readJson<Record<string, string>>(BACKGROUNDS_STORAGE_KEY, {}))
  );

  private key(type: string, id: number | string | undefined): string {
    return `${type}:${id}`;
  }

  /** Custom top-image data URL for a card, or undefined if it should use the default. */
  getBackground(type: string, id: number | string | undefined): string | undefined {
    return this.backgrounds.get(this.key(type, id));
  }

  clearBackground(type: string, id: number | string | undefined): void {
    if (this.backgrounds.delete(this.key(type, id))) {
      this.persistBackgrounds();
    }
  }

  /**
   * Downscales/re-encodes the chosen image and stores it as the card's top image.
   * Resolves with the stored data URL. Rejects if the file cannot be read/decoded or
   * if localStorage rejects the write (e.g. quota exceeded).
   */
  async setBackgroundFromFile(type: string, id: number | string | undefined, file: File): Promise<string> {
    const dataUrl = await this.fileToResizedDataUrl(file);
    this.backgrounds.set(this.key(type, id), dataUrl);
    this.persistBackgrounds();
    return dataUrl;
  }

  private persistBackgrounds(): void {
    this.writeJson(BACKGROUNDS_STORAGE_KEY, Object.fromEntries(this.backgrounds));
  }

  private fileToResizedDataUrl(file: File): Promise<string> {
    return new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error("Failed to read the selected image."));
      reader.onload = () => {
        const img = new Image();
        img.onerror = () => reject(new Error("The selected file is not a valid image."));
        img.onload = () => {
          const scale = Math.min(1, MAX_IMAGE_EDGE / Math.max(img.width, img.height));
          const canvas = document.createElement("canvas");
          canvas.width = Math.max(1, Math.round(img.width * scale));
          canvas.height = Math.max(1, Math.round(img.height * scale));
          const ctx = canvas.getContext("2d");
          if (!ctx) {
            reject(new Error("Unable to process the selected image."));
            return;
          }
          ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
          resolve(canvas.toDataURL("image/jpeg", IMAGE_QUALITY));
        };
        img.src = reader.result as string;
      };
      reader.readAsDataURL(file);
    });
  }

  private readJson<T>(storageKey: string, fallback: T): T {
    try {
      const raw = localStorage.getItem(storageKey);
      return raw ? (JSON.parse(raw) as T) : fallback;
    } catch {
      return fallback;
    }
  }

  private writeJson(storageKey: string, value: unknown): void {
    localStorage.setItem(storageKey, JSON.stringify(value));
  }
}

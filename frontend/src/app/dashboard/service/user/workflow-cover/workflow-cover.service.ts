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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable, firstValueFrom, of } from "rxjs";
import { catchError, map } from "rxjs/operators";
import { AppSettings } from "../../../../common/app-setting";

export const WORKFLOW_COVER_URL = "workflow";

/** Longest edge (px) a custom cover image is downscaled to before being stored. */
const MAX_IMAGE_EDGE = 640;
/** JPEG quality used when re-encoding a custom cover image for storage. */
const IMAGE_QUALITY = 0.8;

/** Stores an optional custom cover image per workflow on the backend, downscaled and re-encoded as a JPEG data URL. */
@Injectable({
  providedIn: "root",
})
export class WorkflowCoverService {
  constructor(private http: HttpClient) {}

  /** The workflow's custom cover image data URL, or undefined if it has none. */
  getCover(wid: number): Observable<string | undefined> {
    return this.http.get<{ image: string }>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_COVER_URL}/${wid}/cover`).pipe(
      map(response => response.image),
      catchError(() => of(undefined))
    );
  }

  /** Downscales/re-encodes the chosen image, stores it as the workflow's cover, and resolves with the data URL. */
  async setCoverFromFile(wid: number, file: File): Promise<string> {
    const dataUrl = await this.fileToResizedDataUrl(file);
    await firstValueFrom(
      this.http.put(`${AppSettings.getApiEndpoint()}/${WORKFLOW_COVER_URL}/${wid}/cover`, { image: dataUrl })
    );
    return dataUrl;
  }

  /** Removes the workflow's custom cover image. */
  clearCover(wid: number): Observable<void> {
    return this.http.delete<void>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_COVER_URL}/${wid}/cover`);
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
}

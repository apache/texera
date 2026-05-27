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

import { Injectable, NgZone } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, Subject, from, firstValueFrom } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";

export interface DriveTokenResponse {
  status: string;
  accessToken?: string;
}

export interface DriveFolder {
  id: string;
  name: string;
}

// gapi is loaded via the script tag in index.html
declare var gapi: any;
declare var google: any;

@Injectable({
  providedIn: "root",
})
export class DriveService {
  private readonly BASE = `${AppSettings.getApiEndpoint()}/auth/google/drive`;
  private readonly CONFIG_URL = `${AppSettings.getApiEndpoint()}/auth/google/config`;

  private connected$ = new Subject<void>();
  private pickerLoaded = false;

  constructor(
    private http: HttpClient,
    private ngZone: NgZone
  ) {}

  connect(reauth = false): void {
    this.http.get(`${this.BASE}/connect?reauth=${reauth}`, { responseType: "text" }).subscribe(url => {
      const popup = window.open(url, "gdrive-connect", "width=500,height=600");

      const onMessage = (event: MessageEvent) => {
        if (event.data === "gdrive-connected") {
          window.removeEventListener("message", onMessage);
          popup?.close();
          this.ngZone.run(() => this.connected$.next());
        }
      };

      window.addEventListener("message", onMessage);
    });
  }

  onConnected(): Observable<void> {
    return this.connected$.asObservable();
  }

  getToken(): Observable<DriveTokenResponse> {
    return this.http.get<DriveTokenResponse>(`${this.BASE}/token`);
  }

  exportToDrive(blob: Blob, fileName: string): Observable<void> {
    const result$ = new Subject<void>();

    Promise.all([this.loadPicker(), this.getAccessToken()]).then(([, accessToken]) => {
      if (!accessToken) {
        result$.error(new Error("Not connected to Google Drive"));
        return;
      }

      this.http.get<{ clientId: string; apiKey: string }>(this.CONFIG_URL).subscribe(config => {
        const folderView = new google.picker.DocsView(google.picker.ViewId.FOLDERS)
          .setIncludeFolders(true)
          .setSelectFolderEnabled(true)
          .setMimeTypes("application/vnd.google-apps.folder");

        const picker = new google.picker.PickerBuilder()
          .addView(folderView)
          .setOAuthToken(accessToken)
          .setDeveloperKey(config.apiKey)
          .setTitle("Choose a folder to export to")
          .setCallback((data: any) => {
            if (data.action === google.picker.Action.PICKED) {
              const folderId = data.docs[0].id;
              this.ngZone.run(() => {
                this.uploadToDrive(blob, fileName, folderId, accessToken).subscribe({
                  next: () => {
                    result$.next();
                    result$.complete();
                  },
                  error: (err: unknown) => result$.error(err),
                });
              });
            } else if (data.action === google.picker.Action.CANCEL) {
              this.ngZone.run(() => result$.complete());
            }
          })
          .build();

        picker.setVisible(true);
      });
    });

    return result$.asObservable();
  }

  openFolderPicker(): Observable<DriveFolder> {
    const result$ = new Subject<DriveFolder>();

    Promise.all([this.loadPicker(), this.getAccessToken()]).then(([, accessToken]) => {
      if (!accessToken) {
        result$.error(new Error("Not connected to Google Drive"));
        return;
      }

      this.http.get<{ clientId: string; apiKey: string }>(this.CONFIG_URL).subscribe(config => {
        const folderView = new google.picker.DocsView(google.picker.ViewId.FOLDERS)
          .setIncludeFolders(true)
          .setSelectFolderEnabled(true)
          .setMimeTypes("application/vnd.google-apps.folder");

        const picker = new google.picker.PickerBuilder()
          .addView(folderView)
          .setOAuthToken(accessToken)
          .setDeveloperKey(config.apiKey)
          .setTitle("Choose a folder to export to")
          .setCallback((data: any) => {
            if (data.action === google.picker.Action.PICKED) {
              const doc = data.docs[0];
              this.ngZone.run(() => {
                result$.next({ id: doc.id, name: doc.name });
                result$.complete();
              });
            } else if (data.action === google.picker.Action.CANCEL) {
              this.ngZone.run(() => result$.complete());
            }
          })
          .build();

        picker.setVisible(true);
      });
    });

    return result$.asObservable();
  }

  private uploadToDrive(blob: Blob, fileName: string, folderId: string, accessToken: string): Observable<void> {
    const boundary = "texera_gdrive_boundary";
    const metadata = JSON.stringify({ name: fileName, parents: [folderId] });
    const contentType = blob.type || "application/octet-stream";

    const body = new Blob([
      `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n${metadata}\r\n--${boundary}\r\nContent-Type: ${contentType}\r\n\r\n`,
      blob,
      `\r\n--${boundary}--`,
    ]);

    return from(
      fetch("https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": `multipart/related; boundary="${boundary}"`,
        },
        body,
      }).then(res => {
        if (!res.ok) throw new Error(`Drive upload failed: ${res.status}`);
      })
    );
  }

  private loadPicker(): Promise<void> {
    if (this.pickerLoaded) return Promise.resolve();
    return new Promise(resolve => {
      gapi.load("picker", () => {
        this.pickerLoaded = true;
        resolve();
      });
    });
  }

  private async getAccessToken(): Promise<string | null> {
    const res = await firstValueFrom(this.getToken());
    return res.status === "ok" ? res.accessToken ?? null : null;
  }
}

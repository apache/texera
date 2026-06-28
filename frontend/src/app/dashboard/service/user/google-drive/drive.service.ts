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
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";

@Injectable({
  providedIn: "root",
})
export class DriveService {
  private readonly CONNECT_URL = `${AppSettings.getApiEndpoint()}/auth/google/drive/connect`;

  constructor(
    private http: HttpClient,
    private ngZone: NgZone
  ) {}

  connect(): Observable<void> {
    return new Observable(observer => {
      let popupCleanup: (() => void) | undefined;

      const subscription = this.http.get(this.CONNECT_URL, { responseType: "text" }).subscribe({
        next: url => {
          const popup = window.open(url, "gdrive-connect", "width=500,height=600");

          if (!popup) {
            observer.error(new Error("Popup blocked. Please allow popups for this site."));
            return;
          }

          const onMessage = (event: MessageEvent) => {
            if (event.origin !== window.location.origin) return;
            if (event.source !== popup) return;
            if (event.data === "gdrive-connected") {
              window.removeEventListener("message", onMessage);
              popup.close();
              this.ngZone.run(() => {
                observer.next();
                observer.complete();
              });
            } else if (event.data === "gdrive-error") {
              window.removeEventListener("message", onMessage);
              this.ngZone.run(() => {
                observer.error(new Error("Google Drive connection failed"));
              });
            }
          };

          window.addEventListener("message", onMessage);
          popupCleanup = () => {
            window.removeEventListener("message", onMessage);
            popup.close();
          };
        },
        error: (err: unknown) => observer.error(err),
      });

      return () => {
        subscription.unsubscribe();
        popupCleanup?.();
      };
    });
  }
}

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
import { Observable, Subscription, of, timer } from "rxjs";
import { catchError, filter, map, switchMap, take } from "rxjs/operators";
import { Version } from "../../../../environments/version";
import { NotificationService } from "../notification/notification.service";

export const VERSION_MANIFEST_URL = "assets/version.json";
export const VERSION_POLL_INTERVAL_MS = 5 * 60 * 1000;

@Injectable({
  providedIn: "root",
})
export class DeploymentVersionService {
  constructor(
    private http: HttpClient,
    private notification: NotificationService
  ) {}

  // True when the deployed build's buildNumber differs from the running one.
  checkForUpdate(): Observable<boolean> {
    return this.http.get<{ buildNumber?: string }>(VERSION_MANIFEST_URL, { params: { t: Date.now().toString() } }).pipe(
      map(manifest => {
        const deployed = manifest?.buildNumber;
        return typeof deployed === "string" && deployed.length > 0 && deployed !== Version.buildNumber;
      }),
      catchError(() => of(false))
    );
  }

  // Poll until a new deployment is detected, then prompt once.
  start(intervalMs: number = VERSION_POLL_INTERVAL_MS): Subscription {
    return timer(intervalMs, intervalMs)
      .pipe(
        switchMap(() => this.checkForUpdate()),
        filter(updated => updated),
        take(1)
      )
      .subscribe(() => this.promptReload());
  }

  promptReload(): void {
    this.notification.blank(
      "New version available",
      "A new version of Texera is available. Refresh the page to update.",
      { nzDuration: 0 }
    );
  }
}

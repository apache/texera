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
import { Observable, Subject } from "rxjs";
import { TRACE_ID_RE } from "./observability.types";

/**
 * Small coordinator that lets the Logs panel ask the shell to switch
 * to the Traces tab and pre-fill a trace id. Singleton (providedIn:
 * "root"), Subject-based — no router state, no global mutation.
 *
 * The trace id is regex-validated at the boundary so a corrupted log
 * entry can never push a malformed value at downstream subscribers.
 */
@Injectable({ providedIn: "root" })
export class TracesPivotService {
  private readonly pivot$ = new Subject<string>();

  /** Stream of trace ids the user has clicked-to-open from another
   *  panel. Subscribers (the shell, the traces panel) receive only
   *  values that pass the W3C trace-id regex. */
  readonly onPivot: Observable<string> = this.pivot$.asObservable();

  /** Request a pivot. Silently no-op for invalid input so a caller
   *  cannot wedge the UI by emitting nonsense — same posture as the
   *  service's HTTP guards. */
  pivot(traceId: string): void {
    if (typeof traceId === "string" && TRACE_ID_RE.test(traceId)) {
      this.pivot$.next(traceId);
    }
  }
}

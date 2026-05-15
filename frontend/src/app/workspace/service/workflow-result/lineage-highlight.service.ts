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
import { BehaviorSubject, Observable } from "rxjs";

export interface LineageHighlightRequest {
  /** The operator whose result panel should show the highlighted row. */
  operatorID: string;
  /** 1-indexed source-row position emitted by the lineage-tracking source operator. */
  sourceRow: number;
}

/**
 * Shared bus that lets a downstream "Why?" click ask the upstream source
 * operator's result panel to scroll to and highlight the row that produced
 * the clicked tuple. The publisher (lineage modal) and subscriber
 * (`ResultTableFrameComponent`) are not aware of each other.
 */
@Injectable({
  providedIn: "root",
})
export class LineageHighlightService {
  private readonly pending$ = new BehaviorSubject<LineageHighlightRequest | null>(null);

  /** Current pending request (may be null). Subscribers receive the latest value on subscribe. */
  pendingHighlight(): Observable<LineageHighlightRequest | null> {
    return this.pending$.asObservable();
  }

  getPending(): LineageHighlightRequest | null {
    return this.pending$.getValue();
  }

  requestHighlight(operatorID: string, sourceRow: number): void {
    this.pending$.next({ operatorID, sourceRow });
  }

  /** Called by the subscriber once the highlight has been applied. */
  clear(): void {
    if (this.pending$.getValue() !== null) this.pending$.next(null);
  }
}

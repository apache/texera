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
import { Subject } from "rxjs";

/**
 * Interface for an action plan highlight event
 */
export interface ActionPlanHighlight {
  operatorIds: string[];
  summary: string;
}

/**
 * Service to manage temporary action plan highlights
 * Emits events when action plans are created, which trigger 5-second visual highlights
 */
@Injectable({
  providedIn: "root",
})
export class ActionPlanService {
  private actionPlanHighlightSubject = new Subject<ActionPlanHighlight>();

  constructor() {}

  /**
   * Get action plan highlight stream
   */
  public getActionPlanHighlightStream() {
    return this.actionPlanHighlightSubject.asObservable();
  }

  /**
   * Show a temporary highlight for an action plan (5 seconds)
   */
  public showActionPlanHighlight(operatorIds: string[], summary: string): void {
    this.actionPlanHighlightSubject.next({ operatorIds, summary });
  }
}

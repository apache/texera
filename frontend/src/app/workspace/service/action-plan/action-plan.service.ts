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
import { Subject, Observable } from "rxjs";

/**
 * Interface for an action plan highlight event
 */
export interface ActionPlanHighlight {
  operatorIds: string[];
  linkIds: string[];
  summary: string;
}

/**
 * User feedback for an action plan
 */
export interface ActionPlanFeedback {
  accepted: boolean;
  message?: string; // Optional message when rejecting
}

/**
 * Service to manage action plan highlights and user feedback
 * Handles the interactive flow: show plan -> wait for user decision -> return feedback
 */
@Injectable({
  providedIn: "root",
})
export class ActionPlanService {
  private actionPlanHighlightSubject = new Subject<ActionPlanHighlight>();
  private feedbackSubject: Subject<ActionPlanFeedback> | null = null;
  private cleanupSubject = new Subject<void>();

  constructor() {}

  /**
   * Get action plan highlight stream
   */
  public getActionPlanHighlightStream() {
    return this.actionPlanHighlightSubject.asObservable();
  }

  /**
   * Get cleanup stream - emits when user provides feedback (accept/reject)
   */
  public getCleanupStream() {
    return this.cleanupSubject.asObservable();
  }

  /**
   * Show an action plan and wait for user feedback (accept/reject)
   * Returns an Observable that emits when user makes a decision
   */
  public showActionPlanAndWaitForFeedback(
    operatorIds: string[],
    linkIds: string[],
    summary: string
  ): Observable<ActionPlanFeedback> {
    // Create new feedback subject for this plan
    this.feedbackSubject = new Subject<ActionPlanFeedback>();

    // Emit highlight event
    this.actionPlanHighlightSubject.next({ operatorIds, linkIds, summary });

    // Return observable that will emit when user accepts/rejects
    return this.feedbackSubject.asObservable();
  }

  /**
   * User accepted the action plan
   */
  public acceptPlan(): void {
    if (this.feedbackSubject) {
      this.feedbackSubject.next({ accepted: true });
      this.feedbackSubject.complete();
      this.feedbackSubject = null;

      // Trigger cleanup (remove highlight and panel)
      this.cleanupSubject.next();
    }
  }

  /**
   * User rejected the action plan with optional feedback message
   */
  public rejectPlan(message?: string): void {
    if (this.feedbackSubject) {
      this.feedbackSubject.next({ accepted: false, message });
      this.feedbackSubject.complete();
      this.feedbackSubject = null;

      // Trigger cleanup (remove highlight and panel)
      this.cleanupSubject.next();
    }
  }
}

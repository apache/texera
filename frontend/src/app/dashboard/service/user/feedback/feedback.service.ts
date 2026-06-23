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

import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { Feedback, FeedbackCount } from "../../../type/feedback.interface";

export const FEEDBACK_BASE_URL = `${AppSettings.getApiEndpoint()}/feedback`;
export const FEEDBACK_COUNTS_URL = `${FEEDBACK_BASE_URL}/counts`;
export const FEEDBACK_USER_URL = `${FEEDBACK_BASE_URL}/user`;

@Injectable({
  providedIn: "root",
})
export class FeedbackService {
  constructor(private http: HttpClient) {}

  /** Submit a new feedback message for the current user. */
  public submitFeedback(message: string): Observable<void> {
    return this.http.post<void>(`${FEEDBACK_BASE_URL}`, { message });
  }

  /** List the current user's own feedback, newest first. */
  public getMyFeedback(): Observable<ReadonlyArray<Feedback>> {
    return this.http.get<ReadonlyArray<Feedback>>(`${FEEDBACK_BASE_URL}`);
  }

  /** Admin only: feedback counts per user (only users with >= 1 feedback). */
  public getFeedbackCounts(): Observable<ReadonlyArray<FeedbackCount>> {
    return this.http.get<ReadonlyArray<FeedbackCount>>(`${FEEDBACK_COUNTS_URL}`);
  }

  /** Admin only: list the feedback submitted by a specific user, newest first. */
  public getUserFeedback(uid: number): Observable<ReadonlyArray<Feedback>> {
    const params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<Feedback>>(`${FEEDBACK_USER_URL}`, { params });
  }
}

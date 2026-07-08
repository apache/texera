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

import { Component, inject, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgFor, NgIf, DatePipe } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import {
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { NzMessageService } from "ng-zorro-antd/message";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { FeedbackService } from "../../../service/user/feedback/feedback.service";
import { Feedback } from "../../../type/feedback.interface";

/**
 * Feedback view. Used in two modes:
 *  - As a routed page (no modal data): the logged-in user submits feedback and
 *    sees a table of their own previous feedback.
 *  - As modal content with `{ uid }` injected via NZ_MODAL_DATA: an admin views
 *    a specific user's feedback read-only (no submit box).
 */
@UntilDestroy()
@Component({
  selector: "texera-feedback",
  templateUrl: "./feedback.component.html",
  styleUrls: ["./feedback.component.scss"],
  imports: [
    NgFor,
    NgIf,
    DatePipe,
    FormsModule,
    NzCardComponent,
    NzButtonComponent,
    NzWaveDirective,
    NzInputDirective,
    NzIconDirective,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzTbodyComponent,
  ],
})
export class FeedbackComponent implements OnInit {
  // When set, the component is showing another user's feedback (admin modal view).
  readonly adminUid: number | undefined = inject(NZ_MODAL_DATA, { optional: true })?.uid;
  newFeedback: string = "";
  submitting: boolean = false;
  feedbackList: ReadonlyArray<Feedback> = [];

  constructor(
    private feedbackService: FeedbackService,
    private messageService: NzMessageService
  ) {}

  get isAdminView(): boolean {
    return this.adminUid !== undefined;
  }

  ngOnInit(): void {
    this.loadFeedback();
  }

  loadFeedback(): void {
    const request$ = this.isAdminView
      ? this.feedbackService.getUserFeedback(this.adminUid as number)
      : this.feedbackService.getMyFeedback();
    request$.pipe(untilDestroyed(this)).subscribe({
      next: feedbackList => (this.feedbackList = feedbackList),
      error: (err: unknown) => this.messageService.error(this.extractError(err)),
    });
  }

  submitFeedback(): void {
    const message = this.newFeedback.trim();
    if (message.length === 0) {
      this.messageService.warning("Feedback cannot be empty.");
      return;
    }
    this.submitting = true;
    this.feedbackService
      .submitFeedback(message)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.submitting = false;
          this.newFeedback = "";
          this.messageService.success("Thank you for your feedback!");
          this.loadFeedback();
        },
        error: (err: unknown) => {
          this.submitting = false;
          this.messageService.error(this.extractError(err));
        },
      });
  }

  private extractError(err: unknown): string {
    return (err as any)?.error?.message || (err as Error)?.message || "An unexpected error occurred.";
  }
}

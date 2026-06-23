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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of } from "rxjs";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";

import { FeedbackComponent } from "./feedback.component";
import { FeedbackService } from "../../../service/user/feedback/feedback.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Feedback } from "../../../type/feedback.interface";

function makeFeedbackServiceSpy() {
  return {
    getMyFeedback: vi.fn().mockReturnValue(of([] as Feedback[])),
    getUserFeedback: vi.fn().mockReturnValue(of([] as Feedback[])),
    submitFeedback: vi.fn().mockReturnValue(of(undefined)),
    getFeedbackCounts: vi.fn().mockReturnValue(of([])),
  };
}

function makeMessageSpy() {
  return { success: vi.fn(), error: vi.fn(), warning: vi.fn() };
}

describe("FeedbackComponent", () => {
  describe("own-feedback (page) mode", () => {
    let component: FeedbackComponent;
    let fixture: ComponentFixture<FeedbackComponent>;
    let feedbackSpy: ReturnType<typeof makeFeedbackServiceSpy>;
    let messageSpy: ReturnType<typeof makeMessageSpy>;

    beforeEach(async () => {
      feedbackSpy = makeFeedbackServiceSpy();
      messageSpy = makeMessageSpy();
      await TestBed.configureTestingModule({
        imports: [FeedbackComponent, HttpClientTestingModule],
        providers: [
          { provide: FeedbackService, useValue: feedbackSpy },
          { provide: NzMessageService, useValue: messageSpy },
          ...commonTestProviders,
        ],
      }).compileComponents();
      fixture = TestBed.createComponent(FeedbackComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
    });

    it("creates and is not in admin view", () => {
      expect(component).toBeTruthy();
      expect(component.isAdminView).toBe(false);
    });

    it("loads the current user's own feedback on init", () => {
      expect(feedbackSpy.getMyFeedback).toHaveBeenCalled();
      expect(feedbackSpy.getUserFeedback).not.toHaveBeenCalled();
    });

    it("does not submit empty or whitespace-only feedback", () => {
      component.newFeedback = "   ";
      component.submitFeedback();
      expect(feedbackSpy.submitFeedback).not.toHaveBeenCalled();
      expect(messageSpy.warning).toHaveBeenCalled();
    });

    it("submits trimmed feedback, clears the box, and reloads on success", () => {
      feedbackSpy.getMyFeedback.mockClear();
      component.newFeedback = "  great tool  ";
      component.submitFeedback();
      expect(feedbackSpy.submitFeedback).toHaveBeenCalledWith("great tool");
      expect(component.newFeedback).toBe("");
      expect(messageSpy.success).toHaveBeenCalled();
      expect(feedbackSpy.getMyFeedback).toHaveBeenCalled();
    });
  });

  describe("admin (modal) mode", () => {
    let component: FeedbackComponent;
    let fixture: ComponentFixture<FeedbackComponent>;
    let feedbackSpy: ReturnType<typeof makeFeedbackServiceSpy>;

    beforeEach(async () => {
      feedbackSpy = makeFeedbackServiceSpy();
      await TestBed.configureTestingModule({
        imports: [FeedbackComponent, HttpClientTestingModule],
        providers: [
          { provide: FeedbackService, useValue: feedbackSpy },
          { provide: NzMessageService, useValue: makeMessageSpy() },
          { provide: NZ_MODAL_DATA, useValue: { uid: 42 } },
          ...commonTestProviders,
        ],
      }).compileComponents();
      fixture = TestBed.createComponent(FeedbackComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
    });

    it("is in admin view and loads the target user's feedback", () => {
      expect(component.isAdminView).toBe(true);
      expect(component.adminUid).toBe(42);
      expect(feedbackSpy.getUserFeedback).toHaveBeenCalledWith(42);
      expect(feedbackSpy.getMyFeedback).not.toHaveBeenCalled();
    });
  });
});

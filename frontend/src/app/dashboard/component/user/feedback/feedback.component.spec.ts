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
import { formatDate } from "@angular/common";
import { NEVER, of, throwError } from "rxjs";
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

/** Feedback fixture. `creationTime` is 2023-11-14T22:13:20Z; `fid`/`uid` stay small so that
 * feeding either of them to the date pipe instead would render a 1970 date, which the
 * table assertion below can tell apart from the real submission time. */
const FIXTURE_CREATION_TIME = 1700000000000;

function makeFeedback(fid: number, message: string): Feedback {
  return { fid, uid: 1, message, creationTime: FIXTURE_CREATION_TIME };
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
      // The success arm has to release the box too, not only the failure arm: `submitting`
      // drives [nzLoading] on the button and [disabled] on the textarea, so leaving it set
      // would lock the user out of sending a second piece of feedback.
      expect(component.submitting).toBe(false);
    });

    /**
     * Failure paths. Both requests report through the same `extractError` helper, whose job is
     * to prefer the server's own explanation over the transport's generic one — so the fixtures
     * below deliberately carry BOTH, otherwise the assertion passes either way.
     */
    describe("failures", () => {
      it("shows the server's own message when the feedback list fails to load", () => {
        feedbackSpy.getMyFeedback.mockReturnValue(
          throwError(() => ({
            error: { message: "feedback is unavailable" },
            message: "Http failure response for /api/feedback/me: 500 Internal Server Error",
          }))
        );

        component.loadFeedback();

        expect(messageSpy.error).toHaveBeenCalledWith("feedback is unavailable");
      });

      it("re-enables the submit box and shows the server's own message when submitting fails", () => {
        feedbackSpy.submitFeedback.mockReturnValue(
          throwError(() => ({
            error: { message: "feedback quota exceeded" },
            message: "Http failure response for /api/feedback: 429 Too Many Requests",
          }))
        );
        feedbackSpy.getMyFeedback.mockClear();
        component.newFeedback = "one more thing";

        component.submitFeedback();

        expect(messageSpy.error).toHaveBeenCalledWith("feedback quota exceeded");
        // The box must not stay locked, and a failed submit must not clear what was typed
        // nor reload the list as if it had been accepted.
        expect(component.submitting).toBe(false);
        expect(component.newFeedback).toBe("one more thing");
        expect(messageSpy.success).not.toHaveBeenCalled();
        expect(feedbackSpy.getMyFeedback).not.toHaveBeenCalled();
      });

      it("falls back to the transport message when the server sent no body", () => {
        feedbackSpy.getMyFeedback.mockReturnValue(throwError(() => new Error("connection refused")));

        component.loadFeedback();

        expect(messageSpy.error).toHaveBeenCalledWith("connection refused");
      });

      it("falls back to a generic message for an error that carries no message at all", () => {
        feedbackSpy.getMyFeedback.mockReturnValue(throwError(() => ({})));

        component.loadFeedback();

        expect(messageSpy.error).toHaveBeenCalledWith("An unexpected error occurred.");
      });
    });

    /**
     * The template owns the rest of the submit flow: the box has to write what was typed back
     * into the component, and the button has to be the thing that sends it.
     */
    describe("rendered page", () => {
      const textarea = () => fixture.nativeElement.querySelector("textarea") as HTMLTextAreaElement;
      const submitButton = () => fixture.nativeElement.querySelector(".feedback-submit-button") as HTMLButtonElement;
      const rows = () => Array.from(fixture.nativeElement.querySelectorAll("tbody tr") as NodeListOf<HTMLElement>);

      it("submits exactly what was typed into the box", () => {
        const box = textarea();
        box.value = "please add dark mode";
        box.dispatchEvent(new Event("input"));
        fixture.detectChanges();

        // The two-way binding has to have written the typed text back to the component,
        // which is also what un-disables the button.
        expect(component.newFeedback).toBe("please add dark mode");
        expect(submitButton().disabled).toBe(false);

        submitButton().click();

        expect(feedbackSpy.submitFeedback).toHaveBeenCalledWith("please add dark mode");
      });

      it("shows the button as loading while the submit is in flight, and idle before it", () => {
        // [nzLoading]="submitting" is the only signal the user gets that the submit is under
        // way; a button that still looks idle invites a second click on the same feedback.
        // nz-button reflects it as a host class, so it is read that way.
        expect(submitButton().classList.contains("ant-btn-loading")).toBe(false);

        feedbackSpy.submitFeedback.mockReturnValue(NEVER);
        component.newFeedback = "one more thing";
        component.submitFeedback();
        fixture.detectChanges();

        expect(component.submitting).toBe(true);
        expect(submitButton().classList.contains("ant-btn-loading")).toBe(true);
      });

      it("offers the submit box in own-feedback mode", () => {
        // Positive control for the admin-mode assertion that the box is absent: without
        // this pair, `*ngIf="!isAdminView"` could be widened to a constant and go unnoticed.
        expect(fixture.nativeElement.querySelector(".feedback-submit-card")).not.toBeNull();
        expect(textarea()).not.toBeNull();
      });

      it("keeps the submit button locked while the box is empty", () => {
        expect(submitButton().disabled).toBe(true);
      });

      it("keeps the submit button locked for whitespace-only text", () => {
        // The guard is `newFeedback.trim().length === 0`; without the trim the button
        // un-disables here and the click is then rejected by submitFeedback instead.
        const box = textarea();
        box.value = "   ";
        box.dispatchEvent(new Event("input"));
        fixture.detectChanges();

        // The typed text must have reached the model, or the assertion below is vacuous.
        expect(component.newFeedback).toBe("   ");
        expect(submitButton().disabled).toBe(true);
      });

      it("renders a row per feedback entry with its submitted time and its message", () => {
        feedbackSpy.getMyFeedback.mockReturnValue(of([makeFeedback(1, "please add dark mode")]));
        component.loadFeedback();
        fixture.detectChanges();

        expect(rows().length).toBe(1);
        const cells = Array.from(rows()[0].querySelectorAll("td"));
        expect(cells.length).toBe(2);
        // Compared against the same format independently applied to creationTime rather than
        // against a date-shaped regex: a regex passes for any number in that cell, and both
        // other numeric fields of the fixture (fid, uid) render as a 1970 date that matches it.
        expect(cells[0].textContent?.trim()).toBe(formatDate(FIXTURE_CREATION_TIME, "MM/dd/y, h:mm a", "en-US"));
        expect(cells[1].classList.contains("feedback-message")).toBe(true);
        expect(cells[1].textContent?.trim()).toBe("please add dark mode");
      });
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

    it("renders the target user's feedback read-only, with no submit box", () => {
      // The read-only half of the two-mode contract. A submit box shown here is wired to
      // submitFeedback(), which posts as the admin rather than as the user being inspected.
      expect(fixture.nativeElement.querySelector(".feedback-submit-card")).toBeNull();
      expect(fixture.nativeElement.querySelector("textarea")).toBeNull();
      expect(fixture.nativeElement.querySelector(".feedback-submit-button")).toBeNull();
    });
  });
});

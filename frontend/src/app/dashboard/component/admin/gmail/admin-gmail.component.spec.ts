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
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { Subject } from "rxjs";
import { AdminGmailComponent } from "./admin-gmail.component";
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("AdminGmailComponent", () => {
  let component: AdminGmailComponent;
  let fixture: ComponentFixture<AdminGmailComponent>;
  let senderEmailSubject: Subject<string>;
  let getSenderEmailSpy: ReturnType<typeof vi.fn>;
  let sendEmailSpy: ReturnType<typeof vi.fn>;

  /** Create the component (runs ngOnInit) and render the template once. */
  function createComponent(): void {
    fixture = TestBed.createComponent(AdminGmailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(async () => {
    // Controllable GmailService double: getSenderEmail returns a subject we drive
    // per test (both next and error branches); sendEmail is fire-and-forget so a
    // plain vi.fn() is enough.
    senderEmailSubject = new Subject<string>();
    getSenderEmailSpy = vi.fn(() => senderEmailSubject.asObservable());
    sendEmailSpy = vi.fn();
    const gmailServiceStub = { getSenderEmail: getSenderEmailSpy, sendEmail: sendEmailSpy };

    await TestBed.configureTestingModule({
      imports: [AdminGmailComponent, FormsModule, ReactiveFormsModule, NoopAnimationsModule, HttpClientTestingModule],
      providers: [{ provide: GmailService, useValue: gmailServiceStub }, ...commonTestProviders],
    }).compileComponents();
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
    vi.restoreAllMocks();
  });

  it("should create and render the reactive form template", () => {
    createComponent();

    expect(component).toBeTruthy();
    const el = fixture.nativeElement as HTMLElement;
    // The template binds each control; confirm the inputs rendered for .html coverage.
    expect(el.querySelector('input[formControlName="email"]')).toBeTruthy();
    expect(el.querySelector('input[formControlName="subject"]')).toBeTruthy();
    expect(el.querySelector('textarea[formControlName="content"]')).toBeTruthy();
  });

  describe("validateForm validation", () => {
    beforeEach(() => createComponent());

    it("builds the form with email/subject/content controls", () => {
      expect(component.validateForm.contains("email")).toBe(true);
      expect(component.validateForm.contains("subject")).toBe(true);
      expect(component.validateForm.contains("content")).toBe(true);
    });

    it("is invalid when empty", () => {
      expect(component.validateForm.valid).toBe(false);
    });

    it("is invalid when the email is malformed (all fields otherwise filled)", () => {
      component.validateForm.setValue({
        email: "not-an-email",
        subject: "Hello",
        content: "Body",
      });
      expect(component.validateForm.valid).toBe(false);
      expect(component.validateForm.get("email")?.hasError("email")).toBe(true);
    });

    it("is invalid when subject is missing", () => {
      component.validateForm.setValue({
        email: "to@example.com",
        subject: null,
        content: "Body",
      });
      expect(component.validateForm.valid).toBe(false);
      expect(component.validateForm.get("subject")?.hasError("required")).toBe(true);
    });

    it("is invalid when content is missing", () => {
      component.validateForm.setValue({
        email: "to@example.com",
        subject: "Hello",
        content: null,
      });
      expect(component.validateForm.valid).toBe(false);
      expect(component.validateForm.get("content")?.hasError("required")).toBe(true);
    });

    it("is valid when all three fields are filled with a well-formed email", () => {
      component.validateForm.setValue({
        email: "to@example.com",
        subject: "Hello",
        content: "Body",
      });
      expect(component.validateForm.valid).toBe(true);
    });
  });

  describe("getSenderEmail (called from ngOnInit)", () => {
    it("subscribes on init and sets email to the emitted value", () => {
      createComponent();
      expect(getSenderEmailSpy).toHaveBeenCalledTimes(1);
      expect(component.email).toBeUndefined();

      senderEmailSubject.next("sender@texera.org");

      expect(component.email).toBe("sender@texera.org");
    });

    it("resets email to undefined and logs when the sender lookup errors", () => {
      const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
      createComponent();
      // Sentinel so the reset-to-undefined is observable, not a no-op default.
      component.email = "stale@texera.org";
      const err = new Error("lookup failed");

      senderEmailSubject.error(err);

      expect(component.email).toBeUndefined();
      expect(logSpy).toHaveBeenCalledWith(err);
    });
  });

  describe("sendTestEmail", () => {
    it("calls GmailService.sendEmail with the form's subject, content and email", () => {
      createComponent();
      component.validateForm.setValue({
        email: "to@example.com",
        subject: "Test Subject",
        content: "Test Content",
      });

      component.sendTestEmail();

      expect(sendEmailSpy).toHaveBeenCalledTimes(1);
      expect(sendEmailSpy).toHaveBeenCalledWith("Test Subject", "Test Content", "to@example.com");
    });
  });
});

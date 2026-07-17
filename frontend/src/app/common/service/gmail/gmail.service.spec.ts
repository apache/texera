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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { GmailService } from "./gmail.service";
import { NotificationService } from "../notification/notification.service";

describe("GmailService", () => {
  let service: GmailService;
  let httpTestingController: HttpTestingController;
  let notificationSpy: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    notificationSpy = { success: vi.fn(), error: vi.fn() };
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [GmailService, { provide: NotificationService, useValue: notificationSpy }],
    });
    service = TestBed.inject(GmailService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTestingController.verify();
    vi.restoreAllMocks();
  });

  it("should show a success toast when the backend accepts the send request", () => {
    service.sendEmail("subj", "body", "to@example.com");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    req.flush(null);

    expect(notificationSpy.success).toHaveBeenCalledWith("Email sent successfully");
    expect(notificationSpy.error).not.toHaveBeenCalled();
  });

  it("sends the correct PUT body for sendEmail with an explicit receiver", () => {
    service.sendEmail("subj", "body", "to@example.com");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    expect(req.request.body).toEqual({ receiver: "to@example.com", subject: "subj", content: "body" });
    req.flush(null);
  });

  it("defaults the receiver to an empty string when it is omitted", () => {
    service.sendEmail("subj", "body");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    expect(req.request.body).toEqual({ receiver: "", subject: "subj", content: "body" });
    req.flush(null);
  });

  it("shows an error toast and logs to the console on a failed send", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    service.sendEmail("subj", "body", "to@example.com");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    req.flush("boom", { status: 502, statusText: "Bad Gateway" });

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to send email. Please try again or contact admin.");
    expect(notificationSpy.success).not.toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith("Send email error:", "boom");
  });

  it("issues a GET to the sender-email endpoint and emits the response without notifying", () => {
    let emitted: string | undefined;
    service.getSenderEmail().subscribe(value => (emitted = value as string));

    const req = httpTestingController.expectOne(
      r => r.url.endsWith("/gmail/sender/email") && r.method === "GET" && r.responseType === "text"
    );
    req.flush("sender@example.com");

    expect(emitted).toBe("sender@example.com");
    expect(notificationSpy.success).not.toHaveBeenCalled();
    expect(notificationSpy.error).not.toHaveBeenCalled();
  });

  it("sends the correct POST body for notifyUnauthorizedLogin", () => {
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    const req = httpTestingController.expectOne(
      r => r.url.endsWith("/gmail/notify-unauthorized") && r.method === "POST"
    );
    expect(req.request.body).toEqual({ receiver: "u@example.com", affiliation: "ACME", reason: "for research" });
    req.flush(null);
  });

  it("shows a success toast when the unauthorized-login notification is accepted", () => {
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    httpTestingController
      .expectOne(r => r.url.endsWith("/gmail/notify-unauthorized") && r.method === "POST")
      .flush(null);

    expect(notificationSpy.success).toHaveBeenCalledWith("An admin has been notified about your account request.");
  });

  it("shows an error toast and logs to the console when the notification fails", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    httpTestingController
      .expectOne(r => r.url.endsWith("/gmail/notify-unauthorized") && r.method === "POST")
      .flush("boom", { status: 500, statusText: "Internal Server Error" });

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to notify admin about your account request.");
    expect(consoleSpy).toHaveBeenCalledWith("Notify error:", "boom");
  });
});

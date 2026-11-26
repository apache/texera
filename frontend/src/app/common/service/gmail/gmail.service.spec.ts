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

import { HttpTestingController } from "@angular/common/http/testing";
import { flushHttpRequest, setupHttpServiceTest } from "../../testing/http-service-test-utils";
import { GmailService } from "./gmail.service";
import { NotificationService } from "../notification/notification.service";

describe("GmailService", () => {
  let service: GmailService;
  let httpTestingController: HttpTestingController;
  let notificationSpy: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    notificationSpy = { success: vi.fn(), error: vi.fn() };
    ({ service, httpMock: httpTestingController } = setupHttpServiceTest(GmailService, [
      { provide: NotificationService, useValue: notificationSpy },
    ]));
  });

  afterEach(() => {
    httpTestingController.verify();
    vi.restoreAllMocks();
  });

  it("should show a success toast when the backend accepts the send request", () => {
    service.sendEmail("subj", "body", "to@example.com");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/send"), "PUT", null);

    expect(notificationSpy.success).toHaveBeenCalledWith("Email sent successfully");
    expect(notificationSpy.error).not.toHaveBeenCalled();
  });

  it("sends the correct PUT body for sendEmail with an explicit receiver", () => {
    service.sendEmail("subj", "body", "to@example.com");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/send"), "PUT", null, {
      body: { receiver: "to@example.com", subject: "subj", content: "body" },
    });
  });

  it("defaults the receiver to an empty string when it is omitted", () => {
    service.sendEmail("subj", "body");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/send"), "PUT", null, {
      body: { receiver: "", subject: "subj", content: "body" },
    });
  });

  it("shows an error toast and logs to the console on a failed send", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    service.sendEmail("subj", "body", "to@example.com");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/send"), "PUT", "boom", {
      error: { status: 502, statusText: "Bad Gateway" },
    });

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to send email. Please try again or contact admin.");
    expect(notificationSpy.success).not.toHaveBeenCalled();
    expect(consoleSpy).toHaveBeenCalledWith("Send email error:", "boom");
  });

  it("issues a GET to the sender-email endpoint and emits the response without notifying", () => {
    let emitted: string | undefined;
    service.getSenderEmail().subscribe(value => (emitted = value as string));

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/sender/email"), "GET", "sender@example.com", {
      responseType: "text",
    });

    expect(emitted).toBe("sender@example.com");
    expect(notificationSpy.success).not.toHaveBeenCalled();
    expect(notificationSpy.error).not.toHaveBeenCalled();
  });

  it("sends the correct POST body for notifyUnauthorizedLogin", () => {
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/notify-unauthorized"), "POST", null, {
      body: { receiver: "u@example.com", affiliation: "ACME", reason: "for research" },
    });
  });

  it("shows a success toast when the unauthorized-login notification is accepted", () => {
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/notify-unauthorized"), "POST", null);

    expect(notificationSpy.success).toHaveBeenCalledWith("An admin has been notified about your account request.");
  });

  it("shows an error toast and logs to the console when the notification fails", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    service.notifyUnauthorizedLogin("u@example.com", "ACME", "for research");

    flushHttpRequest(httpTestingController, r => r.url.endsWith("/gmail/notify-unauthorized"), "POST", "boom", {
      error: { status: 500, statusText: "Internal Server Error" },
    });

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to notify admin about your account request.");
    expect(consoleSpy).toHaveBeenCalledWith("Notify error:", "boom");
  });
});

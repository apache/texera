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
  });

  it("should show a success toast when the backend accepts the send request", () => {
    service.sendEmail("subj", "body", "to@example.com");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    req.flush(null);

    expect(notificationSpy.success).toHaveBeenCalledWith("Email sent successfully");
    expect(notificationSpy.error).not.toHaveBeenCalled();
  });

  it("should show an error toast when the backend returns an HTTP error (e.g. SMTP failure)", () => {
    service.sendEmail("subj", "body", "to@example.com");

    const req = httpTestingController.expectOne(r => r.url.endsWith("/gmail/send") && r.method === "PUT");
    req.flush("Failed to send email: 535-5.7.8 Username and Password not accepted", {
      status: 502,
      statusText: "Bad Gateway",
    });

    expect(notificationSpy.error).toHaveBeenCalledWith("Failed to send email. Please try again or contact admin.");
    expect(notificationSpy.success).not.toHaveBeenCalled();
  });
});

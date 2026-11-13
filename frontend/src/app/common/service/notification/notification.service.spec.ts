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
import { NzMessageService } from "ng-zorro-antd/message";
import { NzNotificationService } from "ng-zorro-antd/notification";
import { NotificationService } from "./notification.service";

describe("NotificationService", () => {
  let service: NotificationService;
  let message: Record<"success" | "info" | "error" | "warning" | "loading", ReturnType<typeof vi.fn>>;
  let notification: Record<"blank" | "remove", ReturnType<typeof vi.fn>>;

  beforeEach(() => {
    message = { success: vi.fn(), info: vi.fn(), error: vi.fn(), warning: vi.fn(), loading: vi.fn() };
    notification = { blank: vi.fn(), remove: vi.fn() };

    TestBed.configureTestingModule({
      providers: [
        NotificationService,
        { provide: NzMessageService, useValue: message },
        { provide: NzNotificationService, useValue: notification },
      ],
    });
    service = TestBed.inject(NotificationService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("success / info / error / warning delegate to the message service with the content and options", () => {
    const options = { nzDuration: 5000 };
    service.success("s", options);
    service.info("i", options);
    service.error("e", options);
    service.warning("w", options);

    expect(message.success).toHaveBeenCalledWith("s", options);
    expect(message.info).toHaveBeenCalledWith("i", options);
    expect(message.error).toHaveBeenCalledWith("e", options);
    expect(message.warning).toHaveBeenCalledWith("w", options);
  });

  it("defaults to empty options when none are given", () => {
    service.error("boom");
    expect(message.error).toHaveBeenCalledWith("boom", {});
  });

  it("loading delegates to the message service and returns its ref", () => {
    const ref = { messageId: "m1" };
    message.loading.mockReturnValue(ref);

    expect(service.loading("working", { nzDuration: 0 })).toBe(ref);
    expect(message.loading).toHaveBeenCalledWith("working", { nzDuration: 0 });
  });

  it("blank delegates to the notification service and returns its ref", () => {
    const ref = { messageId: "n1" };
    notification.blank.mockReturnValue(ref);
    const options = { nzDuration: 0 };

    expect(service.blank("title", "content", options)).toBe(ref);
    expect(notification.blank).toHaveBeenCalledWith("title", "content", options);
  });

  it("remove delegates to the notification service", () => {
    service.remove();
    expect(notification.remove).toHaveBeenCalledTimes(1);
  });
});

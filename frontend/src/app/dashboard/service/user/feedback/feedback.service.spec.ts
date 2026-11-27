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
import { firstValueFrom } from "rxjs";

import { FeedbackService } from "./feedback.service";
import { AppSettings } from "../../../../common/app-setting";
import { flushHttpRequest, setupHttpServiceTest } from "../../../../common/testing/http-service-test-utils";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { Feedback, FeedbackCount } from "../../../type/feedback.interface";

const API = "api";

describe("FeedbackService", () => {
  let service: FeedbackService;
  let http: HttpTestingController;

  beforeEach(() => {
    ({ service, httpMock: http } = setupHttpServiceTest(FeedbackService, commonTestProviders));
    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);
  });

  afterEach(() => {
    http.verify();
  });

  it("submitFeedback POSTs the message to /feedback", () => {
    service.submitFeedback("hello world").subscribe();
    flushHttpRequest(http, `${API}/feedback`, "POST", null, { body: { message: "hello world" } });
  });

  it("getMyFeedback GETs /feedback and returns the list", async () => {
    const expected: ReadonlyArray<Feedback> = [{ fid: 1, uid: 7, message: "m", creationTime: 123 }];
    const pending = firstValueFrom(service.getMyFeedback());
    flushHttpRequest(http, `${API}/feedback`, "GET", expected);
    expect(await pending).toEqual(expected);
  });

  it("getFeedbackCounts GETs /feedback/counts", async () => {
    const expected: ReadonlyArray<FeedbackCount> = [{ uid: 7, count: 3 }];
    const pending = firstValueFrom(service.getFeedbackCounts());
    flushHttpRequest(http, `${API}/feedback/counts`, "GET", expected);
    expect(await pending).toEqual(expected);
  });

  it("getUserFeedback GETs /feedback/user with a user_id query param", async () => {
    const expected: ReadonlyArray<Feedback> = [{ fid: 2, uid: 42, message: "x", creationTime: 9 }];
    const pending = firstValueFrom(service.getUserFeedback(42));
    flushHttpRequest(http, r => r.url === `${API}/feedback/user`, "GET", expected, { params: { user_id: "42" } });
    expect(await pending).toEqual(expected);
  });
});

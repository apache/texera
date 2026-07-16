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
import { throwError } from "rxjs";
import { AiAnalystService } from "./ai-analyst.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { AppSettings } from "../../../common/app-setting";

describe("AiAnalystService", () => {
  let service: AiAnalystService;
  let httpMock: HttpTestingController;
  const isEnabledUrl = `${AppSettings.getApiEndpoint()}/aiassistant/isenabled`;
  const openaiUrl = `${AppSettings.getApiEndpoint()}/aiassistant/openai`;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      // WorkflowActionService is a constructor dependency but unused by the tested
      // methods, so an empty stub avoids pulling in the workflow-graph module.
      providers: [AiAnalystService, { provide: WorkflowActionService, useValue: {} }],
    });
    service = TestBed.inject(AiAnalystService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.restoreAllMocks();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("isOpenAIEnabled", () => {
    it("GETs /aiassistant/isenabled as text and emits true when the backend returns OpenAI", () => {
      let result: boolean | undefined;
      service.isOpenAIEnabled().subscribe(v => (result = v));

      const req = httpMock.expectOne(isEnabledUrl);
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");

      req.flush("OpenAI");
      expect(result).toBe(true);
    });

    it("emits false for any non-OpenAI response", () => {
      let result: boolean | undefined;
      service.isOpenAIEnabled().subscribe(v => (result = v));

      httpMock.expectOne(isEnabledUrl).flush("NoAiAssistant");
      expect(result).toBe(false);
    });

    it("emits false when the request errors", () => {
      let result: boolean | undefined;
      service.isOpenAIEnabled().subscribe(v => (result = v));

      httpMock.expectOne(isEnabledUrl).flush("boom", { status: 500, statusText: "Server Error" });
      expect(result).toBe(false);
    });

    it("returns the cached flag without issuing a request", () => {
      (service as any).isAIAssistantEnabled = true;
      let result: boolean | undefined;
      service.isOpenAIEnabled().subscribe(v => (result = v));

      expect(result).toBe(true);
      httpMock.expectNone(isEnabledUrl);
    });
  });

  describe("sendPromptToOpenAI", () => {
    it("emits an empty string without calling OpenAI when the assistant is disabled", () => {
      let result: string | undefined;
      service.sendPromptToOpenAI("hello").subscribe(v => (result = v));

      httpMock.expectOne(isEnabledUrl).flush("NoAiAssistant");
      httpMock.expectNone(openaiUrl);
      expect(result).toBe("");
    });

    it("POSTs the prompt and emits the trimmed completion when enabled", () => {
      let result: string | undefined;
      service.sendPromptToOpenAI("analyze this").subscribe(v => (result = v));

      httpMock.expectOne(isEnabledUrl).flush("OpenAI");

      const req = httpMock.expectOne(openaiUrl);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ prompt: "analyze this" });
      req.flush({ choices: [{ message: { content: "  the answer  " } }] });

      expect(result).toBe("the answer");
    });

    it("emits an empty string when the OpenAI request fails", () => {
      let result: string | undefined;
      service.sendPromptToOpenAI("x").subscribe(v => (result = v));

      httpMock.expectOne(isEnabledUrl).flush("OpenAI");
      httpMock.expectOne(openaiUrl).flush("boom", { status: 500, statusText: "Server Error" });

      expect(result).toBe("");
    });

    it("emits an empty string when the enablement check errors", () => {
      vi.spyOn(service, "isOpenAIEnabled").mockReturnValue(throwError(() => new Error("check failed")));
      let result: string | undefined;
      service.sendPromptToOpenAI("x").subscribe(v => (result = v));

      expect(result).toBe("");
    });
  });
});

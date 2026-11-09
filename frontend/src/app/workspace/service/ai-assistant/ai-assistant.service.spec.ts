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
import {
  AIAssistantService,
  AI_ASSISTANT_API_BASE_URL,
  TypeAnnotationResponse,
  UnannotatedArgument,
} from "./ai-assistant.service";

describe("AIAssistantService", () => {
  let service: AIAssistantService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    // The service logs to the console on every call; keep the test output quiet.
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AIAssistantService],
    });
    service = TestBed.inject(AIAssistantService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.restoreAllMocks();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("checkAIAssistantEnabled", () => {
    it("GETs /isenabled as text and emits OpenAI when the backend returns OpenAI", () => {
      let result: string | undefined;
      service.checkAIAssistantEnabled().subscribe(r => (result = r));

      const req = httpMock.expectOne(`${AI_ASSISTANT_API_BASE_URL}/isenabled`);
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");

      req.flush("OpenAI");
      expect(result).toBe("OpenAI");
    });

    it("emits NoAiAssistant for any non-OpenAI response", () => {
      let result: string | undefined;
      service.checkAIAssistantEnabled().subscribe(r => (result = r));

      httpMock.expectOne(`${AI_ASSISTANT_API_BASE_URL}/isenabled`).flush("Gemini");
      expect(result).toBe("NoAiAssistant");
    });

    it("falls back to NoAiAssistant when the request errors", () => {
      let result: string | undefined;
      service.checkAIAssistantEnabled().subscribe(r => (result = r));

      httpMock
        .expectOne(`${AI_ASSISTANT_API_BASE_URL}/isenabled`)
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(result).toBe("NoAiAssistant");
    });
  });

  describe("getTypeAnnotations", () => {
    it("POSTs /annotationresult with the code payload and passes the response through", () => {
      const response: TypeAnnotationResponse = { choices: [{ message: { content: "def f(x: int) -> int" } }] };
      let result: TypeAnnotationResponse | undefined;
      service.getTypeAnnotations("x + 1", 3, "def f(x):\n    return x + 1").subscribe(r => (result = r));

      const req = httpMock.expectOne(`${AI_ASSISTANT_API_BASE_URL}/annotationresult`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ code: "x + 1", lineNumber: 3, allcode: "def f(x):\n    return x + 1" });

      req.flush(response);
      expect(result).toEqual(response);
    });
  });

  describe("locateUnannotated", () => {
    it("POSTs /annotate-argument and flattens the nested response into UnannotatedArgument[]", () => {
      const backendResponse = {
        underlying: {
          result: {
            value: [
              {
                underlying: {
                  name: { value: "x" },
                  startLine: { value: 1 },
                  startColumn: { value: 4 },
                  endLine: { value: 1 },
                  endColumn: { value: 5 },
                },
              },
            ],
          },
        },
      };
      let result: UnannotatedArgument[] | undefined;
      service.locateUnannotated("def f(x):", 1).subscribe(r => (result = r));

      const req = httpMock.expectOne(`${AI_ASSISTANT_API_BASE_URL}/annotate-argument`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ selectedCode: "def f(x):", startLine: 1 });

      req.flush(backendResponse);
      expect(result).toEqual([{ name: "x", startLine: 1, startColumn: 4, endLine: 1, endColumn: 5 }]);
    });

    it("emits an empty array when the response body is null", () => {
      let result: UnannotatedArgument[] | undefined;
      service.locateUnannotated("code", 1).subscribe(r => (result = r));

      httpMock.expectOne(`${AI_ASSISTANT_API_BASE_URL}/annotate-argument`).flush(null);
      expect(result).toEqual([]);
    });

    it("errors with a wrapped message when the request fails", () => {
      let caught: unknown;
      service.locateUnannotated("code", 1).subscribe({ error: (e: unknown) => (caught = e) });

      httpMock
        .expectOne(`${AI_ASSISTANT_API_BASE_URL}/annotate-argument`)
        .flush("boom", { status: 500, statusText: "Server Error" });
      expect(caught).toBeInstanceOf(Error);
      expect((caught as Error).message).toBe("Request to backend failed");
    });
  });
});

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

import { HttpRequest } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController, TestRequest } from "@angular/common/http/testing";
import { Provider, Type } from "@angular/core";
import { TestBed } from "@angular/core/testing";

type RequestMatcher = string | ((req: HttpRequest<unknown>) => boolean);

/**
 * Configures TestBed with `HttpClientTestingModule` for an Angular service spec and
 * returns both the service instance and `HttpTestingController`.
 */
export function setupHttpServiceTest<T>(
  serviceType: Type<T>,
  providers: Provider[] = []
): { service: T; httpMock: HttpTestingController } {
  TestBed.configureTestingModule({
    imports: [HttpClientTestingModule],
    providers: [serviceType, ...providers],
  });
  return {
    service: TestBed.inject(serviceType),
    httpMock: TestBed.inject(HttpTestingController),
  };
}

/**
 * Matches one pending HTTP request, asserts its method and optional body, responseType,
 * content type, or query params. Flushes `response` (optionally with an error status).
 */
export function flushHttpRequest(
  httpMock: HttpTestingController,
  matcher: RequestMatcher,
  method: string,
  response: Parameters<TestRequest["flush"]>[0],
  options?: {
    body?: unknown;
    responseType?: string;
    contentType?: string;
    params?: Record<string, string>;
    error?: { status: number; statusText: string };
  }
): TestRequest {
  const req = httpMock.expectOne(matcher);
  expect(req.request.method).toBe(method);
  if (options?.responseType !== undefined) {
    expect(req.request.responseType).toBe(options.responseType);
  }
  if (options?.body !== undefined) {
    expect(req.request.body).toEqual(options.body);
  }
  if (options?.contentType) {
    expect(req.request.headers.get("Content-Type")).toEqual(options.contentType);
  }
  for (const [key, expected] of Object.entries(options?.params ?? {})) {
    expect(req.request.params.get(key)).toBe(expected);
  }
  req.flush(response, options?.error);
  return req;
}

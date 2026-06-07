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

import { HttpErrorResponse, HttpEvent, HttpHandler, HttpRequest, HttpResponse } from "@angular/common/http";
import { Observable, firstValueFrom, of, throwError } from "rxjs";

import { BlobErrorHttpInterceptor } from "./blob-error-http-interceptor.service";

/**
 * The interceptor is a pure function of (req, next), so the specs drive it
 * directly with a stub `HttpHandler` rather than through HttpClient. Two of
 * the branches under test — a non-`HttpErrorResponse` error and a
 * `FileReader` failure — cannot be produced through `HttpClient` at all
 * (it always wraps errors as `HttpErrorResponse`, and a readable Blob never
 * triggers `FileReader.onerror`), so direct invocation is the only way to
 * cover them.
 */
describe("BlobErrorHttpInterceptor", () => {
  let interceptor: BlobErrorHttpInterceptor;
  const req = new HttpRequest("GET", "/test");

  const handlerReturning = (obs: Observable<HttpEvent<any>>): HttpHandler => ({ handle: () => obs }) as HttpHandler;

  // Run the interceptor and resolve to the emitted value or, on error, the error.
  const run = (next: HttpHandler): Promise<any> => firstValueFrom(interceptor.intercept(req, next)).catch(e => e);

  beforeEach(() => {
    interceptor = new BlobErrorHttpInterceptor();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("passes a successful response through unchanged", async () => {
    const response = new HttpResponse({ body: "ok", status: 200 });
    expect(await run(handlerReturning(of(response)))).toBe(response);
  });

  it("re-throws an error that is not an HttpErrorResponse unchanged", async () => {
    const err = new Error("not-http");
    expect(await run(handlerReturning(throwError(() => err)))).toBe(err);
  });

  it("re-throws an HttpErrorResponse whose error is not a Blob unchanged", async () => {
    const err = new HttpErrorResponse({ error: { message: "plain" }, status: 500 });
    expect(await run(handlerReturning(throwError(() => err)))).toBe(err);
  });

  it("re-throws an HttpErrorResponse with a non-json Blob unchanged", async () => {
    const err = new HttpErrorResponse({
      error: new Blob(["whatever"], { type: "text/plain" }),
      status: 500,
    });
    expect(await run(handlerReturning(throwError(() => err)))).toBe(err);
  });

  it("parses an application/json Blob error into a structured HttpErrorResponse", async () => {
    const err = new HttpErrorResponse({
      error: new Blob([JSON.stringify({ message: "Boom" })], { type: "application/json" }),
      status: 502,
      statusText: "Bad Gateway",
      url: "http://example.com/api",
    });

    const rejected = await run(handlerReturning(throwError(() => err)));

    expect(rejected).toBeInstanceOf(HttpErrorResponse);
    expect(rejected.error).toEqual({ message: "Boom" });
    expect(rejected.status).toBe(502);
    expect(rejected.statusText).toBe("Bad Gateway");
    expect(rejected.url).toBe("http://example.com/api");
  });

  it("re-throws the original error when the Blob contains malformed JSON", async () => {
    const err = new HttpErrorResponse({
      error: new Blob(["not json {"], { type: "application/json" }),
      status: 500,
    });
    expect(await run(handlerReturning(throwError(() => err)))).toBe(err);
  });

  it("re-throws the original error when the FileReader fails", async () => {
    class FailingFileReader {
      onload: ((e: Event) => void) | null = null;
      onerror: ((e: Event) => void) | null = null;
      readAsText(): void {
        this.onerror?.(new Event("error"));
      }
    }
    vi.stubGlobal("FileReader", FailingFileReader);

    const err = new HttpErrorResponse({
      error: new Blob([JSON.stringify({ message: "Boom" })], { type: "application/json" }),
      status: 500,
    });
    expect(await run(handlerReturning(throwError(() => err)))).toBe(err);
  });
});

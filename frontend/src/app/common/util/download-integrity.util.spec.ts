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

import { HttpHeaders, HttpResponse } from "@angular/common/http";
import { firstValueFrom, of } from "rxjs";

import { TruncatedDownloadError, verifyCompleteDownload } from "./download-integrity.util";

function blobResponse(body: Blob | null, headers: Record<string, string> = {}): HttpResponse<Blob> {
  return new HttpResponse({ body, headers: new HttpHeaders(headers) });
}

function verify(response: HttpResponse<Blob>): Promise<Blob> {
  return firstValueFrom(of(response).pipe(verifyCompleteDownload()));
}

describe("verifyCompleteDownload", () => {
  it("passes a body whose size matches the declared Content-Length", async () => {
    const blob = new Blob(["12345"]);
    expect(await verify(blobResponse(blob, { "Content-Length": "5" }))).toBe(blob);
  });

  it("rejects a body shorter than the declared Content-Length", async () => {
    const pending = verify(blobResponse(new Blob(["12345"]), { "Content-Length": "2048" }));

    await expect(pending).rejects.toBeInstanceOf(TruncatedDownloadError);
    await expect(pending).rejects.toThrow("Download truncated: expected 2048 bytes but received 5.");
  });

  it("passes a chunked response, which declares no Content-Length", async () => {
    const blob = new Blob(["12345"]);
    expect(await verify(blobResponse(blob))).toBe(blob);
  });

  it("passes a body longer than Content-Length, which means decoding rather than truncation", async () => {
    const blob = new Blob(["1234567890"]);
    expect(await verify(blobResponse(blob, { "Content-Length": "4" }))).toBe(blob);
  });

  it("substitutes an empty blob for a body-less response", async () => {
    expect((await verify(blobResponse(null))).size).toBe(0);
  });
});

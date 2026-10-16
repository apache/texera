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

import { HttpResponse } from "@angular/common/http";
import { OperatorFunction } from "rxjs";
import { map } from "rxjs/operators";

export class TruncatedDownloadError extends Error {
  constructor(
    readonly expectedBytes: number,
    readonly receivedBytes: number
  ) {
    super(`Download truncated: expected ${expectedBytes} bytes but received ${receivedBytes}.`);
    this.name = "TruncatedDownloadError";
  }
}

export function verifyCompleteDownload(): OperatorFunction<HttpResponse<Blob>, Blob> {
  return map(response => {
    const blob = response.body ?? new Blob([]);
    const declaredLength = Number(response.headers.get("Content-Length"));
    if (Number.isFinite(declaredLength) && declaredLength > blob.size) {
      throw new TruncatedDownloadError(declaredLength, blob.size);
    }
    return blob;
  });
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { Observable } from "rxjs";
import { ApiClient } from "../core/api-client";

// Preserve the URL encoding of the existing multipart protocol, including its
// intentionally pre-encoded filePath for init/finish and single-encoded XHR parts.
export class HttpParams {
  private readonly values: Map<string, string>;
  constructor(values: Map<string, string> = new Map()) {
    this.values = values;
  }
  set(key: string, value: string | number | boolean): HttpParams {
    const next = new Map(this.values);
    next.set(key, String(value));
    return new HttpParams(next);
  }
  get(key: string): string | null {
    return this.values.get(key) ?? null;
  }
  toString(): string {
    const encode = (text: string) =>
      encodeURIComponent(text).replace(/%(40|3A|24|2C|3B|3D|3F|2F)/gi, code =>
        String.fromCharCode(Number.parseInt(code.slice(1), 16))
      );
    return [...this.values].map(([key, value]) => `${encode(key)}=${encode(value)}`).join("&");
  }
}
export interface UploadHttp {
  post<T = unknown>(url: string, body: unknown, options?: { params?: HttpParams }): Observable<T>;
}
export function uploadHttp(api: ApiClient, baseUrl = "/api"): UploadHttp {
  return {
    post<T>(url: string, body: unknown, options?: { params?: HttpParams }): Observable<T> {
      return new Observable<T>(observer => {
        const controller = new AbortController();
        const path = url.startsWith(baseUrl + "/") ? url.slice(baseUrl.length + 1) : url;
        const query = options?.params?.toString();
        void api.post<T>(`${path}${query ? `?${query}` : ""}`, body, { signal: controller.signal }).then(
          value => {
            observer.next(value);
            observer.complete();
          },
          error => observer.error(error)
        );
        return () => controller.abort();
      });
    },
  };
}

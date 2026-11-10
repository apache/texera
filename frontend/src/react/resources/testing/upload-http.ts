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
import { HttpParams, type UploadHttp } from "../upload-http";
interface Request {
  url: string;
  body: unknown;
  params: HttpParams;
}
interface Pending {
  request: Request;
  flush: (value: unknown) => void;
}
export class UploadHttpTestingController implements UploadHttp {
  private pending: Pending[] = [];
  post<T>(url: string, body: unknown, options?: { params?: HttpParams }): Observable<T> {
    return new Observable<T>(subscriber => {
      const record = {
        request: { url, body, params: options?.params ?? new HttpParams() },
        flush: (value: unknown) => {
          subscriber.next(value as T);
          subscriber.complete();
        },
      };
      this.pending.push(record);
      return () => {
        this.pending = this.pending.filter(item => item !== record);
      };
    });
  }
  expectOne(matcher: string | ((request: Request) => boolean)): Pending {
    const matches = this.pending.filter(record =>
      typeof matcher === "string" ? record.request.url === matcher : matcher(record.request)
    );
    if (matches.length !== 1)
      throw new Error(
        `Expected one upload request, found ${matches.length}; pending: ${this.pending.map(item => item.request.url).join(", ")}`
      );
    const match = matches[0];
    this.pending = this.pending.filter(item => item !== match);
    return match;
  }
  verify(): void {
    if (this.pending.length)
      throw new Error(`Unmatched upload requests: ${this.pending.map(item => item.request.url).join(", ")}`);
  }
}

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

import { Injectable } from "@angular/core";
import { HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { catchError } from "rxjs/operators";

@Injectable()
export class BlobErrorHttpInterceptor implements HttpInterceptor {
  public intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(req).pipe(
      catchError((err: unknown) => {
        if (err instanceof HttpErrorResponse && err.error instanceof Blob && err.error.type === "application/json") {
          // https://github.com/angular/angular/issues/19888
          // When request of type Blob, the error is also in Blob instead of object of the json data
          return new Promise<any>((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e: Event) => {
              try {
                const errmsg = JSON.parse((<any>e.target).result);
                reject(
                  new HttpErrorResponse({
                    error: errmsg,
                    headers: err.headers,
                    status: err.status,
                    statusText: err.statusText,
                    url: err.url !== null ? err.url : undefined,
                  })
                );
              } catch (_) {
                reject(err);
              }
            };
            reader.onerror = _ => {
              reject(err);
            };
            reader.readAsText(err.error);
          });
        }
        return throwError(err);
      })
    );
  }
}

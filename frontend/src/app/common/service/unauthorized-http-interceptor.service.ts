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

import { HttpErrorResponse, HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Router } from "@angular/router";
import { Observable, throwError } from "rxjs";
import { catchError } from "rxjs/operators";
import { ABOUT } from "../../app-routing.constant";
import { NotificationService } from "./notification/notification.service";
import { AuthService } from "./user/auth.service";

/**
 * Globally handles 401 responses that come back for *authenticated* requests:
 * clears the stored JWT, notifies the user, and routes to the landing page
 * with returnUrl so they can be sent back after re-login. 401s on anonymous
 * requests are passed through unchanged — see issue #5391 / #4901 / #5026.
 *
 * Token cleanup goes through AuthService.removeAccessToken (a static method)
 * rather than injecting AuthService, to avoid the AuthService → HttpClient →
 * interceptor DI cycle.
 */
@Injectable()
export class UnauthorizedHttpInterceptor implements HttpInterceptor {
  constructor(
    private router: Router,
    private notificationService: NotificationService
  ) {}

  public intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    return next.handle(req).pipe(
      catchError((err: unknown) => {
        if (err instanceof HttpErrorResponse && err.status === 401 && req.headers.has("Authorization")) {
          AuthService.removeAccessToken();
          this.notificationService.error("Your session has expired. Please log in again.");
          const currentUrl = this.router.url;
          this.router.navigate([ABOUT], {
            queryParams: { returnUrl: currentUrl === "/" ? null : currentUrl },
          });
        }
        return throwError(() => err);
      })
    );
  }
}

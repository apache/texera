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
import { Injectable, Injector } from "@angular/core";
import { Router } from "@angular/router";
import { Observable, throwError } from "rxjs";
import { catchError } from "rxjs/operators";
import { ABOUT } from "../../app-routing.constant";
import { NotificationService } from "./notification/notification.service";
import { UserService } from "./user/user.service";

// Endpoints whose own 401 means "wrong credentials", not "your session is invalid".
// JwtModule blanket-attaches Authorization to every request, so a stale token
// gets piggy-backed on login/register/refresh attempts even when the user is
// trying to *acquire* a session. Treating those 401s as session-invalidation
// would kick out an already-authenticated user who fat-fingers a re-login.
const AUTH_ENDPOINT_PATTERN = /\/auth\/(login|register|refresh|google\/login)(?:\?|$)/;

/**
 * Globally handles 401 responses that come back for *authenticated* requests:
 * routes the logout through UserService (which clears the token and broadcasts
 * `userChanged(undefined)` so header / dashboard / in-memory state stay in
 * sync), surfaces a notification, and routes to the landing page with
 * returnUrl. 401s on anonymous, auth-endpoint, or already-logged-out paths
 * pass through unchanged. See #5391 / #4901 / #5026.
 *
 * UserService is resolved lazily via Injector because it (transitively)
 * depends on HttpClient, and HttpClient depends on the interceptor chain —
 * direct injection here would form a DI cycle.
 */
@Injectable()
export class UnauthorizedHttpInterceptor implements HttpInterceptor {
  constructor(
    private injector: Injector,
    private router: Router,
    private notificationService: NotificationService
  ) {}

  public intercept(req: HttpRequest<unknown>, next: HttpHandler): Observable<HttpEvent<unknown>> {
    return next.handle(req).pipe(
      catchError((err: unknown) => {
        if (this.shouldHandleAsSessionExpired(req, err)) {
          const userService = this.injector.get(UserService);
          // Dedup: if a burst of 401s arrives, only the first one through here
          // sees isLogin()=true. logout() flips it to false (via changeUser),
          // so the rest of the burst skip the side effects. After a real
          // re-login isLogin() flips back to true and the interceptor re-arms.
          if (userService.isLogin()) {
            userService.logout();
            this.notificationService.error("Your session has expired. Please log in again.");
            const currentUrl = this.router.url;
            this.router.navigate([ABOUT], {
              queryParams: { returnUrl: currentUrl === "/" ? null : currentUrl },
            });
          }
        }
        return throwError(() => err);
      })
    );
  }

  private shouldHandleAsSessionExpired(req: HttpRequest<unknown>, err: unknown): boolean {
    return (
      err instanceof HttpErrorResponse &&
      err.status === 401 &&
      req.headers.has("Authorization") &&
      !AUTH_ENDPOINT_PATTERN.test(req.url)
    );
  }
}

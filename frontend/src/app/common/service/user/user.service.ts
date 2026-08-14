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
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../app-setting";
import { Observable, of, ReplaySubject } from "rxjs";
import { Role, User } from "../../type/user";
import { AuthService } from "./auth.service";
import { GuiConfigService } from "../gui-config.service";
import { catchError, map, shareReplay, switchMap } from "rxjs/operators";
import { validateEmailFormat } from "../../util/email";

/**
 * User Service manages User information. It relies on different
 * auth services to authenticate a valid User.
 */
@Injectable({
  providedIn: "root",
})
export class UserService {
  private currentUser?: User = undefined;
  private userChangeSubject: ReplaySubject<User | undefined> = new ReplaySubject<User | undefined>(1);
  private cache = new Map<string, { url: string; expiry: number }>();
  private readonly cacheDuration = 3600 * 1000; // cache duration: 1h

  constructor(
    private authService: AuthService,
    private config: GuiConfigService,
    private http: HttpClient
  ) {
    const user = this.authService.loginWithExistingToken();
    this.changeUser(user);

    // AuthService changes the session on its own when its email prompt resolves — an identity-only
    // login starts with no address, so it either replaces the token with one carrying the new
    // address or signs out. Re-deriving here is what turns either outcome into the `currentUser`
    // every subscriber reads.
    this.authService.sessionChanged().subscribe(() => this.changeUser(this.authService.loginWithExistingToken()));
  }
  public getCurrentUser(): User | undefined {
    return this.currentUser;
  }

  public login(username: string, password: string): Observable<void> {
    // validate the credentials with backend
    return this.authService
      .auth(username, password)
      .pipe(switchMap(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  public googleLogin(credential: string): Observable<void> {
    return this.authService
      .googleAuth(credential)
      .pipe(switchMap(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  public orcidLogin(code: string): Observable<void> {
    return this.authService.orcidAuth(code).pipe(switchMap(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  public isLogin(): boolean {
    return this.currentUser !== undefined;
  }

  public isAdmin(): boolean {
    return this.currentUser?.role === Role.ADMIN;
  }

  public userChanged(): Observable<User | undefined> {
    return this.userChangeSubject.asObservable();
  }

  public logout(): void {
    this.authService.logout();
    this.changeUser(undefined);
  }

  public register(username: string, email: string, password: string): Observable<void> {
    return this.authService
      .register(username, email, password)
      .pipe(switchMap(({ accessToken }) => this.handleAccessToken(accessToken)));
  }

  /**
   * changes the current user and triggers currentUserSubject
   * @param user
   */
  private changeUser(user: User | undefined): void {
    if (user) {
      const hue = Math.floor(Math.random() * 360); // Hue (0-360)
      const sat = Math.floor(60 + Math.random() * 20); // Saturation (60%-80%)
      const light = 50; // Lightness (50%)
      this.currentUser = { ...user, color: `hsl(${hue}, ${sat}%, ${light}%)` };
    } else {
      this.currentUser = user;
    }
    this.userChangeSubject.next(this.currentUser);
  }

  // Returns Observable<void> rather than void so callers (login / googleLogin /
  // register) can switchMap through the post-login config fetch. The /config/gui
  // and /config/user-system endpoints are @RolesAllowed, so we must wait for the
  // new JWT to be in localStorage before they will answer; loginWithExistingToken
  // also reads config.env.inviteOnly, so it must run after loadPostLogin resolves.
  private handleAccessToken(accessToken: string): Observable<void> {
    AuthService.setAccessToken(accessToken);
    return this.config.loadPostLogin().pipe(
      catchError((err: unknown) => {
        // If the authenticated config fetch fails, still complete login with
        // whatever we have. The JwtAuthFilter on every protected endpoint is
        // the authoritative gate; degraded config is preferable to a stuck
        // login flow.
        console.warn("Failed to load authenticated config after login; continuing.", err);
        return of(undefined);
      }),
      map(() => this.changeUser(this.authService.loginWithExistingToken()))
    );
  }

  /**
   * check the given parameter is legal for login/registration
   * @param username
   */
  static validateUsername(username: string): { result: boolean; message: string } {
    if (username.trim().length === 0) {
      return { result: false, message: "Username should not be empty." };
    }
    return { result: true, message: "Username frontend validation success." };
  }

  /**
   * check the given parameter is a syntactically valid email address for registration
   * @param email
   */
  static validateEmail(email: string): { result: boolean; message: string } {
    return validateEmailFormat(email);
  }

  /**
   * Fetch the avatar at `avatarUrl` and expose it as an object URL, cached for `cacheDuration`.
   *
   * `avatarUrl` is the complete URL the identity provider supplied, stored as-is on the user
   * record. It used to be only the last path segment of Google's `picture` claim, with this
   * method rebuilding `https://lh3.googleusercontent.com/a/<fragment>` around it — which made
   * the stored value unusable for any other provider. The backend allowlists the host before
   * storing it, so what arrives here has already been validated.
   */
  getAvatar(avatarUrl: string): Observable<string | undefined> {
    if (!avatarUrl) return of(undefined);

    const cached = this.cache.get(avatarUrl);
    if (cached) {
      if (Date.now() <= cached.expiry) {
        return of(cached.url);
      } else {
        URL.revokeObjectURL(cached.url);
        this.cache.delete(avatarUrl);
      }
    }

    return this.fetchBlob(avatarUrl).pipe(
      map(blob => {
        const blobUrl = URL.createObjectURL(blob);
        this.cache.set(avatarUrl, {
          url: blobUrl,
          expiry: Date.now() + this.cacheDuration,
        });
        return blobUrl;
      }),
      catchError(() => of(undefined)),
      shareReplay(1)
    );
  }

  private fetchBlob(url: string): Observable<Blob> {
    return new Observable(observer => {
      fetch(url, { referrerPolicy: "no-referrer" })
        .then(response => {
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
          return response.blob();
        })
        .then(blob => observer.next(blob))
        .catch(error => observer.error(error));
    });
  }
}

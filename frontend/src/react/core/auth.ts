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

import { ApiClient } from "./api-client";
import { GuiConfig, RuntimeConfig } from "./runtime-config";
import { validateEmailFormat } from "../../core/common/util/email";

export const TOKEN_KEY = "access_token";
export type Role = "INACTIVE" | "RESTRICTED" | "REGULAR" | "ADMIN";
export type AuthStatus = "anonymous" | "authenticated" | "email-required" | "approval-required";

export interface SessionUser {
  uid: number;
  name: string;
  email?: string;
  role: Role;
  avatar?: string;
  googleId?: string;
  comment?: string;
  joiningReason?: string;
  expiresAt: number;
}
export interface AuthSnapshot {
  status: AuthStatus;
  user?: SessionUser;
  config: Readonly<GuiConfig>;
  registrationRequired?: boolean;
  loading: boolean;
  error?: string;
}
type StorageLike = Pick<Storage, "getItem" | "setItem" | "removeItem">;
export interface AuthOptions {
  storage?: StorageLike;
  now?: () => number;
}

/** Reads display claims only; the backend validates signatures and authorizes every operation. */
export function decodeSessionToken(token: string, now = Date.now()): SessionUser | undefined {
  try {
    const parts = token.split(".");
    if (parts.length !== 3 || !parts[1]) return undefined;
    const binary = atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"));
    const claims = JSON.parse(new TextDecoder().decode(Uint8Array.from(binary, character => character.charCodeAt(0))));
    if (
      !claims ||
      !Number.isInteger(claims.userId) ||
      claims.userId < 0 ||
      typeof claims.sub !== "string" ||
      !claims.sub ||
      !["INACTIVE", "RESTRICTED", "REGULAR", "ADMIN"].includes(claims.role) ||
      typeof claims.exp !== "number" ||
      !Number.isFinite(claims.exp) ||
      claims.exp * 1000 <= now
    )
      return undefined;
    return {
      uid: claims.userId,
      name: claims.sub,
      email: typeof claims.email === "string" && claims.email ? claims.email : undefined,
      role: claims.role,
      avatar:
        typeof (claims.avatar ?? claims.googleAvatar) === "string" ? claims.avatar ?? claims.googleAvatar : undefined,
      googleId: typeof claims.googleId === "string" ? claims.googleId : undefined,
      comment: typeof claims.comment === "string" ? claims.comment : undefined,
      joiningReason: typeof claims.joiningReason === "string" ? claims.joiningReason : undefined,
      expiresAt: claims.exp * 1000,
    };
  } catch {
    return undefined;
  }
}

/** Return links stay within this application and cannot cycle back through sign-in. */
export function safeReturnUrl(value?: string | null): string {
  const fallback = "/user/workflow";
  if (!value || !value.startsWith("/") || /[\\\u0000-\u0020]/.test(value)) return fallback;
  let decoded = value;
  try {
    for (let index = 0; index < 3; index++) {
      if (!decoded.startsWith("/") || decoded.startsWith("//") || /[\\\u0000-\u0020]/.test(decoded)) return fallback;
      const next = decodeURIComponent(decoded);
      if (next === decoded) break;
      decoded = next;
    }
  } catch {
    return fallback;
  }
  const path = new URL(decoded, "https://texera.invalid").pathname.replace(/\/+$/, "");
  if (
    decoded.startsWith("//") ||
    /[\\\u0000-\u0020]/.test(decoded) ||
    path === "/login" ||
    path === "/callback" ||
    path.startsWith("/callback/")
  )
    return fallback;
  return value;
}

function validateCredentials(username: string, password: string, email?: string): void {
  if (!username?.trim()) throw new Error("Username should not be empty.");
  if (!password || password.length < 6) throw new Error("Password length should be greater than 5.");
  if (email !== undefined) validateEmail(email);
}

function validateEmail(email: string): void {
  const validation = validateEmailFormat(email);
  if (!validation.result) throw new Error(validation.message);
}

/** Framework-independent session store, consumed with React's useSyncExternalStore. */
export class AuthStore {
  private snapshot: AuthSnapshot;
  private readonly listeners = new Set<() => void>();
  private readonly storage: StorageLike;
  private readonly now: () => number;
  private expirationTimer?: ReturnType<typeof setTimeout>;
  private generation = 0;
  private readonly onStorage = (event: StorageEvent) => {
    if (event.key === TOKEN_KEY || event.key === null) void this.bootstrap();
  };

  constructor(
    private readonly api: ApiClient,
    private readonly runtimeConfig: RuntimeConfig,
    options: AuthOptions = {}
  ) {
    this.storage = options.storage ?? globalThis.localStorage;
    this.now = options.now ?? Date.now;
    this.snapshot = { status: "anonymous", config: runtimeConfig.env, loading: false };
    if (!options.storage && typeof window !== "undefined") window.addEventListener("storage", this.onStorage);
  }

  getSnapshot = (): AuthSnapshot => this.snapshot;
  getToken = (): string | null => this.storage.getItem(TOKEN_KEY);
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  async bootstrap(): Promise<void> {
    const generation = ++this.generation;
    this.update({ ...this.snapshot, loading: true, error: undefined });
    const token = this.getToken();
    const candidate = token ? decodeSessionToken(token, this.now()) : undefined;
    if (token && !candidate) this.storage.removeItem(TOKEN_KEY);
    try {
      await this.runtimeConfig.loadPreLogin();
      if (generation !== this.generation) return;
      if (token && candidate) await this.acceptToken(token, generation);
      else this.update({ status: "anonymous", config: this.runtimeConfig.env, loading: false });
    } catch (error) {
      if (generation === this.generation) {
        this.update({
          status: "anonymous",
          config: this.runtimeConfig.env,
          loading: false,
          error: this.message(error),
        });
      }
      throw error;
    }
  }

  async login(username: string, password: string): Promise<void> {
    validateCredentials(username, password);
    await this.authenticate(() =>
      this.api.post<{ accessToken: string }>("auth/login", { username: username.trim(), password })
    );
  }

  async register(username: string, email: string, password: string): Promise<{ verificationRequired: boolean }> {
    validateCredentials(username, password, email);
    const generation = this.begin();
    try {
      const response = await this.api.post<{ accessToken: string | null }>("auth/register", {
        username: username.trim(),
        email: email.trim(),
        password,
      });
      if (generation !== this.generation) return { verificationRequired: false };
      if (response.accessToken) await this.acceptToken(response.accessToken, generation);
      else this.update({ ...this.snapshot, loading: false });
      return { verificationRequired: !response.accessToken };
    } catch (error) {
      this.failed(error, generation);
      throw error;
    }
  }

  async registerVerify(username: string, email: string, password: string, code: string): Promise<void> {
    validateCredentials(username, password, email);
    if (!code?.trim()) throw new Error("Verification code is required.");
    await this.authenticate(() =>
      this.api.post<{ accessToken: string }>("auth/register/verify", {
        username: username.trim(),
        email: email.trim(),
        password,
        code: code.trim(),
      })
    );
  }

  async socialLogin(provider: "google" | "apple" | "orcid", credential: string): Promise<void> {
    if (!credential) throw new Error("Sign-in credential is missing.");
    await this.authenticate(() =>
      this.api.post<{ accessToken: string }>(`auth/${provider}/login`, credential, {
        headers: { "Content-Type": "text/plain", Accept: "application/json" },
      })
    );
  }

  async requestEmailCode(email: string): Promise<void> {
    validateEmail(email);
    await this.api.post("auth/email/code", { email: email.trim() });
  }

  async completeEmail(email: string, code?: string): Promise<void> {
    validateEmail(email);
    if (this.snapshot.status !== "email-required") throw new Error("No email address is required for this session.");
    await this.authenticate(() => this.api.put<{ accessToken: string }>("auth/email", { email: email.trim(), code }));
  }

  async submitJoiningReason(affiliation: string, reason: string): Promise<void> {
    const user = this.snapshot.user;
    if (this.snapshot.status !== "approval-required" || !user) throw new Error("No approval request is pending.");
    if (!reason?.trim()) throw new Error("Reason is required.");
    await this.api.put("user/joining-reason", {
      uid: user.uid,
      affiliation: affiliation.trim(),
      joiningReason: reason.trim(),
    });
    try {
      await this.api.post("gmail/notify-unauthorized", {
        receiver: user.email,
        affiliation: affiliation.trim(),
        reason: reason.trim(),
      });
    } finally {
      this.logout();
    }
  }

  logout = (): void => {
    ++this.generation;
    this.clearTimer();
    this.storage.removeItem(TOKEN_KEY);
    this.runtimeConfig.clearAuthenticated();
    this.update({ status: "anonymous", config: this.runtimeConfig.env, loading: false });
  };

  dispose(): void {
    ++this.generation;
    this.clearTimer();
    this.listeners.clear();
    if (typeof window !== "undefined") window.removeEventListener("storage", this.onStorage);
  }

  private async authenticate(request: () => Promise<{ accessToken: string }>): Promise<void> {
    const generation = this.begin();
    try {
      const { accessToken } = await request();
      if (generation === this.generation) await this.acceptToken(accessToken, generation);
    } catch (error) {
      this.failed(error, generation);
      throw error;
    }
  }

  private begin(): number {
    const generation = ++this.generation;
    this.update({ ...this.snapshot, loading: true, error: undefined });
    return generation;
  }

  private async acceptToken(token: string, generation: number): Promise<void> {
    const user = decodeSessionToken(token, this.now());
    if (!user) throw new Error("The server returned an invalid or expired access token.");
    this.storage.setItem(TOKEN_KEY, token);
    try {
      await this.runtimeConfig.loadPostLogin();
    } catch {
      /* Backend authorization remains authoritative if config is temporarily unavailable. */
    }
    if (generation !== this.generation) return;
    if (!decodeSessionToken(token, this.now())) {
      this.logout();
      throw new Error("Access token is expired.");
    }
    this.scheduleExpiration(user.expiresAt);
    const common = { user, config: this.runtimeConfig.env, loading: false };
    if (!user.email) {
      this.update({ ...common, status: "email-required" });
    } else if (this.runtimeConfig.env.inviteOnly !== false && user.role === "INACTIVE") {
      this.update({ ...common, status: "approval-required" });
      const required = await this.api.get<boolean>(`user/joining-reason/required?uid=${user.uid}`);
      if (generation === this.generation)
        this.update({ ...common, status: "approval-required", registrationRequired: required });
    } else {
      this.update({ ...common, status: "authenticated" });
    }
  }

  private scheduleExpiration(expiresAt: number): void {
    this.clearTimer();
    const remaining = expiresAt - this.now();
    if (remaining <= 0) {
      this.logout();
      return;
    }
    this.expirationTimer = setTimeout(() => this.scheduleExpiration(expiresAt), Math.min(remaining, 2147483647));
  }

  private clearTimer(): void {
    if (this.expirationTimer !== undefined) clearTimeout(this.expirationTimer);
    this.expirationTimer = undefined;
  }

  private failed(error: unknown, generation: number): void {
    if (generation === this.generation) this.update({ ...this.snapshot, loading: false, error: this.message(error) });
  }

  private update(snapshot: AuthSnapshot): void {
    this.snapshot = snapshot;
    for (const listener of this.listeners) listener();
  }

  private message(error: unknown): string {
    return error instanceof Error ? error.message : "Authentication failed.";
  }
}

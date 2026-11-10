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

export type ResponseType = "json" | "text" | "blob" | "void";
export interface RequestOptions extends Omit<RequestInit, "body"> {
  responseType?: ResponseType;
}
export interface ApiClientOptions {
  baseUrl?: string;
  getToken?: () => string | null;
  onUnauthorized?: () => void;
  fetch?: typeof fetch;
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/** Same-origin API transport. Server error strings remain plain text for React to escape. */
export class ApiClient {
  private readonly baseUrl: string;
  private readonly fetcher: typeof fetch;
  private readonly getToken: () => string | null;
  private readonly onUnauthorized?: () => void;

  constructor(options: ApiClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "/api").replace(/\/$/, "");
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.getToken = options.getToken ?? (() => globalThis.localStorage?.getItem("access_token") ?? null);
    this.onUnauthorized = options.onUnauthorized;
  }

  get<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, options);
  }

  post<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.send<T>(path, body, { ...options, method: "POST" });
  }

  put<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.send<T>(path, body, { ...options, method: "PUT" });
  }
  patch<T = unknown>(path: string, body?: unknown, options: RequestOptions = {}): Promise<T> {
    return this.send<T>(path, body, { ...options, method: "PATCH" });
  }

  delete<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    return this.request<T>(path, { ...options, method: "DELETE" });
  }

  request<T = unknown>(path: string, options: RequestOptions = {}): Promise<T> {
    return this.send<T>(path, undefined, options);
  }

  private async send<T>(path: string, body: unknown, options: RequestOptions): Promise<T> {
    if (/^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(path) || path.includes("\\")) {
      throw new Error("API path must be relative");
    }
    const relative = path.replace(/^\//, "");
    const segments = relative.split(/[?#]/)[0].split("/");
    if (
      segments.some(segment => {
        try {
          return [".", ".."].includes(decodeURIComponent(segment));
        } catch {
          return true;
        }
      })
    ) {
      throw new Error("API path must be relative");
    }
    const { responseType = "json", ...init } = options;
    const headers = new Headers(init.headers);
    if (!headers.has("Accept")) headers.set("Accept", "application/json");
    const anonymous = /^config\/(?:pre-login|settings\/public)(?:[/?#]|$)/.test(relative);
    const token = anonymous ? null : this.getToken();
    if (token && !headers.has("Authorization")) headers.set("Authorization", `Bearer ${token}`);

    let payload: BodyInit | undefined;
    if (body !== undefined && body !== null) {
      if (
        body instanceof FormData ||
        body instanceof Blob ||
        body instanceof URLSearchParams ||
        typeof body === "string"
      ) {
        payload = body;
      } else {
        payload = JSON.stringify(body);
        if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
      }
    }

    const response = await this.fetcher(`${this.baseUrl}/${relative}`, {
      ...init,
      headers,
      body: payload,
      credentials: init.credentials ?? "same-origin",
    });
    if (!response.ok) {
      const text = await response.text();
      let errorBody: unknown = text;
      try {
        errorBody = JSON.parse(text);
      } catch {
        /* Plain-text gateway errors are also useful. */
      }
      const serverMessage =
        typeof errorBody === "object" && errorBody !== null && "message" in errorBody
          ? (errorBody as { message: unknown }).message
          : undefined;
      const message = typeof serverMessage === "string" ? serverMessage : text || `Request failed (${response.status})`;
      if (response.status === 401 && headers.has("Authorization") && !/^auth\//.test(relative)) {
        this.onUnauthorized?.();
      }
      throw new ApiError(response.status, message, errorBody);
    }
    if (response.status === 204 || response.status === 205 || responseType === "void") return undefined as T;
    if (responseType === "blob") return (await response.blob()) as T;
    const text = await response.text();
    if (responseType === "text") return text as T;
    if (!text.trim()) return undefined as T;
    try {
      return JSON.parse(text) as T;
    } catch {
      throw new ApiError(response.status, "Invalid JSON response");
    }
  }
}

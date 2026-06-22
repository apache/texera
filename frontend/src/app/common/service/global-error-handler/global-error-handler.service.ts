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

import { ErrorHandler, Injectable } from "@angular/core";

const CHUNK_LOAD_ERROR = /chunkloaderror|loading chunk [^ ]+ failed|dynamically imported module/i;
export const RELOAD_GUARD_KEY = "texera-chunk-reload-at";
const RELOAD_GUARD_WINDOW_MS = 10_000;

// True for a failed JS chunk / dynamic-import load.
export function isChunkLoadError(error: unknown): boolean {
  if (error == null) {
    return false;
  }
  if ((error as { name?: unknown }).name === "ChunkLoadError") {
    return true;
  }
  const message = (error as { message?: unknown }).message;
  const text = typeof message === "string" ? message : typeof error === "string" ? error : "";
  return CHUNK_LOAD_ERROR.test(text);
}

@Injectable()
export class GlobalErrorHandler implements ErrorHandler {
  private readonly defaultHandler = new ErrorHandler();

  handleError(error: unknown): void {
    if (isChunkLoadError(error) && this.tryReload()) {
      return;
    }
    this.defaultHandler.handleError(error);
  }

  // Reload at most once per guard window so a missing chunk cannot loop.
  private tryReload(): boolean {
    const now = Date.now();
    const last = Number(sessionStorage.getItem(RELOAD_GUARD_KEY));
    if (Number.isFinite(last) && last > 0 && now - last < RELOAD_GUARD_WINDOW_MS) {
      return false;
    }
    sessionStorage.setItem(RELOAD_GUARD_KEY, String(now));
    this.reload();
    return true;
  }

  protected reload(): void {
    window.location.reload();
  }
}

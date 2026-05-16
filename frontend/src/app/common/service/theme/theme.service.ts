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
import { BehaviorSubject, Observable } from "rxjs";
import {
  BUILTIN_THEMES,
  DEFAULT_THEME,
  Theme,
  defaultThemeForSystem,
  findTheme,
} from "./themes";
import { UserConfigService } from "../user/config/user-config.service";
import { UserService } from "../user/user.service";

/**
 * Key under which the selected theme id is persisted in the user-config
 * dictionary (see UserConfigResource on the backend).
 */
const THEME_CONFIG_KEY = "ui.theme";

/**
 * Key for the localStorage fallback used when the user isn't logged in.
 * Same value as the user-config key for convenience.
 */
const THEME_LOCAL_KEY = "ui.theme";

/**
 * Applies the currently selected theme to the document, persists the
 * selection, and exposes a stream of the active theme so components can
 * react (e.g. to retheme third-party widgets like Monaco that don't read
 * CSS variables).
 *
 * Lifecycle:
 *   1. On construction, applies whatever the system-preference default is
 *      (light or dark) so first paint isn't a flash of unstyled content.
 *   2. Asynchronously loads the user's saved choice (from user-config when
 *      logged in, localStorage otherwise) and re-applies if it differs.
 *   3. setTheme() updates :root, persists, and emits.
 */
@Injectable({ providedIn: "root" })
export class ThemeService {
  /** Every built-in theme, in picker display order. */
  public readonly themes: ReadonlyArray<Theme> = BUILTIN_THEMES;

  private readonly current$ = new BehaviorSubject<Theme>(DEFAULT_THEME);

  constructor(
    private readonly userConfig: UserConfigService,
    private readonly userService: UserService
  ) {
    // First paint: system-preference theme so dark-mode-OS users don't get
    // a flash of light theme.
    this.applyToDom(defaultThemeForSystem());

    // Then resolve the persisted choice (if any) and apply it. Out-of-order
    // updates are fine because applyToDom just sets CSS vars.
    this.loadPersisted().then(theme => {
      if (theme) {
        this.applyToDom(theme);
        this.current$.next(theme);
      } else {
        this.current$.next(defaultThemeForSystem());
      }
    });

    // If the user logs in / out, re-resolve the persisted theme — a logged-in
    // user's saved preference should win over a localStorage fallback.
    this.userService.userChanged().subscribe(() => {
      this.loadPersisted().then(theme => {
        if (theme && theme.id !== this.current$.value.id) {
          this.applyToDom(theme);
          this.current$.next(theme);
        }
      });
    });
  }

  /** The currently active theme. Use this for one-shot reads. */
  public getCurrent(): Theme {
    return this.current$.value;
  }

  /** Stream of theme changes. Use this in components that need to react. */
  public current(): Observable<Theme> {
    return this.current$.asObservable();
  }

  /**
   * Switch to a theme by id (or full Theme object). Idempotent; no-ops if
   * the id is already active.
   */
  public setTheme(theme: Theme | string): void {
    const resolved = typeof theme === "string" ? findTheme(theme) : theme;
    if (resolved.id === this.current$.value.id) return;
    this.applyToDom(resolved);
    this.persist(resolved.id);
    this.current$.next(resolved);
  }

  /**
   * Apply a Theme's tokens to :root and set color-scheme so the browser
   * picks correct defaults for scrollbars, form controls, etc.
   */
  private applyToDom(theme: Theme): void {
    if (typeof document === "undefined") return;
    const root = document.documentElement;
    for (const [key, value] of Object.entries(theme.tokens)) {
      root.style.setProperty(`--${key}`, value);
    }
    root.style.colorScheme = theme.mode;
    // Expose the active theme id as a data attribute so any non-token
    // styling (e.g. SVG patterns) can branch on it via CSS selectors.
    root.dataset["theme"] = theme.id;
    // Monaco doesn't read CSS variables — but it does have a global
    // setTheme() that retints every live editor. If monaco is loaded
    // (Code Editor dialog is open or has been opened this session),
    // sync it to the active theme. We swallow errors because monaco
    // may not be on window yet on first paint, or may have been GC'd.
    try {
      const m = (window as unknown as { monaco?: { editor?: { setTheme?: (n: string) => void } } }).monaco;
      const themeName = theme.mode === "dark" ? "vs-dark" : "vs";
      m?.editor?.setTheme?.(themeName);
    } catch {
      /* monaco not loaded yet; will be retinted by its component on init */
    }
  }

  /**
   * Read the persisted theme id from user-config (preferred) or
   * localStorage. Returns null if no choice has been recorded.
   *
   * Both paths swallow errors and treat them as "no preference recorded" —
   * a missing key or a transient network failure should never prevent the
   * app from rendering. The first-paint system-preference theme stays.
   */
  private async loadPersisted(): Promise<Theme | null> {
    if (this.userService.isLogin()) {
      try {
        const id = await new Promise<string | null>(resolve => {
          this.userConfig.fetchKey(THEME_CONFIG_KEY).subscribe({
            next: v => resolve(v),
            error: () => resolve(null),
          });
        });
        if (id) return findTheme(id);
      } catch {
        // fetchKey throws synchronously when called while logged out — the
        // isLogin() check above prevents that, but be defensive anyway.
      }
    }
    if (typeof localStorage !== "undefined") {
      const id = localStorage.getItem(THEME_LOCAL_KEY);
      if (id) return findTheme(id);
    }
    return null;
  }

  /**
   * Save the user's choice. Mirrors to localStorage so an unauthenticated
   * page load still picks up the same theme.
   */
  private persist(themeId: string): void {
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(THEME_LOCAL_KEY, themeId);
    }
    if (this.userService.isLogin()) {
      try {
        this.userConfig.set(THEME_CONFIG_KEY, themeId).subscribe({
          error: () => {
            // localStorage is still authoritative for next load; nothing more
            // to do if the backend is unreachable.
          },
        });
      } catch {
        // userConfig.set throws synchronously when logged out (shouldn't
        // happen given the isLogin() check, but be defensive).
      }
    }
  }
}

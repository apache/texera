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
import { map } from "rxjs/operators";

export type AgentNotificationCategory = "run" | "social" | "admin";
export type AgentNotificationLevel = "info" | "success" | "warning" | "error";
export type AgentNotificationType =
  | "runSuccess"
  | "runFailure"
  | "runKilled"
  | "workflowLikes"
  | "workflowClones"
  | "datasetLikes"
  | "adminRequests";

export interface AgentNotificationSettings {
  runSuccess: boolean;
  runFailure: boolean;
  runKilled: boolean;
  workflowLikes: boolean;
  workflowClones: boolean;
  datasetLikes: boolean;
  adminRequests: boolean;
}

export const DEFAULT_NOTIFICATION_SETTINGS: AgentNotificationSettings = {
  runSuccess: true,
  runFailure: true,
  runKilled: true,
  workflowLikes: true,
  workflowClones: true,
  datasetLikes: true,
  adminRequests: true,
};

export interface AgentNotificationAction {
  /** Call-to-action text displayed on the notification. */
  label: string;
  /** Angular router commands; passed to Router.navigate(). */
  route: unknown[];
}

export interface AgentNotification {
  id: string;
  category: AgentNotificationCategory;
  level: AgentNotificationLevel;
  /** Specific notification type for filtering. */
  type?: AgentNotificationType;
  title: string;
  message: string;
  /** Optional remediation hint surfaced for run errors. */
  hint?: string;
  /** Optional clickable call-to-action. */
  action?: AgentNotificationAction;
  /** Unix epoch ms. */
  timestamp: number;
  read: boolean;
  /** Optional metadata for downstream actions; not displayed directly. */
  meta?: Record<string, unknown>;
  /** LLM-generated remediation suggestion, streamed in after the notification is pushed. */
  aiSuggestion?: string;
  /** Whether an AI suggestion request is in flight. */
  aiSuggestionLoading?: boolean;
}

const MAX_NOTIFICATIONS = 100;
const STORAGE_KEY = "texera-floating-agent-notifications";
const SETTINGS_STORAGE_KEY = "texera-floating-agent-settings";
const DISMISSED_KEYS_STORAGE_KEY = "texera-floating-agent-dismissed-keys";
const MAX_DISMISSED_KEYS = 500;

@Injectable({ providedIn: "root" })
export class FloatingAgentService {
  private readonly notificationsSubject = new BehaviorSubject<AgentNotification[]>(
    FloatingAgentService.loadFromStorage()
  );
  private readonly settingsSubject = new BehaviorSubject<AgentNotificationSettings>(
    FloatingAgentService.loadSettingsFromStorage()
  );

  public readonly notifications$: Observable<AgentNotification[]> = this.notificationsSubject.asObservable();
  public readonly settings$: Observable<AgentNotificationSettings> = this.settingsSubject.asObservable();

  public readonly unreadCount$: Observable<number> = this.notifications$.pipe(
    map(list => list.filter(n => !n.read).length)
  );

  public unreadCountByCategory$(category: AgentNotificationCategory): Observable<number> {
    return this.notifications$.pipe(map(list => list.filter(n => !n.read && n.category === category).length));
  }

  public notificationsByCategory$(category: AgentNotificationCategory): Observable<AgentNotification[]> {
    return this.notifications$.pipe(map(list => list.filter(n => n.category === category)));
  }

  /** Synchronous snapshot — use for one-off lookups, not in templates. */
  public peekByCategory(category: AgentNotificationCategory): AgentNotification[] {
    return this.notificationsSubject.value.filter(n => n.category === category);
  }

  /** Remove notifications matching a predicate. Used to clean up stale entries when
   *  the underlying state changes (e.g., admin request marked viewed elsewhere). */
  public removeWhere(predicate: (n: AgentNotification) => boolean): void {
    const filtered = this.notificationsSubject.value.filter(n => !predicate(n));
    if (filtered.length === this.notificationsSubject.value.length) return;
    this.notificationsSubject.next(filtered);
    this.persist();
  }


  public getSettings(): AgentNotificationSettings {
    return this.settingsSubject.value;
  }

  public updateSettings(settings: Partial<AgentNotificationSettings>): void {
    const next = { ...this.settingsSubject.value, ...settings };
    this.settingsSubject.next(next);
    this.persistSettings();
  }

  public isTypeEnabled(type: AgentNotificationType | undefined): boolean {
    if (!type) return true;
    return this.settingsSubject.value[type] !== false;
  }

  public push(notification: Omit<AgentNotification, "id" | "timestamp" | "read">): string | undefined {
    // Filter out muted notification types
    if (notification.type && !this.isTypeEnabled(notification.type)) {
      return undefined;
    }
    const entry: AgentNotification = {
      ...notification,
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      timestamp: Date.now(),
      read: false,
    };
    const next = [entry, ...this.notificationsSubject.value].slice(0, MAX_NOTIFICATIONS);
    this.notificationsSubject.next(next);
    this.persist();
    return entry.id;
  }

  /** Update fields on an existing notification (used to stream in AI suggestions). */
  public updateById(id: string, partial: Partial<AgentNotification>): void {
    const list = this.notificationsSubject.value;
    const idx = list.findIndex(n => n.id === id);
    if (idx < 0) return;
    const next = [...list];
    next[idx] = { ...list[idx], ...partial };
    this.notificationsSubject.next(next);
    this.persist();
  }

  /**
   * Build a semantic signature for a notification so we can recognize the
   * "same" event across reloads/HMR. Returns undefined when the notification
   * has no stable identity (in which case dismissal can't apply).
   */
  public static signatureFor(
    n: Pick<AgentNotification, "category" | "type" | "meta">
  ): string | undefined {
    const meta = n.meta as Record<string, unknown> | undefined;
    if (n.category === "run") {
      // Use the workflow id + the run's notification type (runSuccess/runFailure/runKilled).
      const wid = meta?.["wid"] as number | undefined;
      if (typeof wid === "number" && n.type) return `run:${wid}:${n.type}`;
    }
    if (n.category === "social") {
      // Include the current total count so a later increase produces a new
      // signature (and therefore a new notification even after dismissal).
      const entityType = meta?.["entityType"];
      const entityId = meta?.["entityId"];
      const action = meta?.["action"];
      const count = meta?.["count"];
      if (entityType !== undefined && entityId !== undefined && action !== undefined) {
        return `social:${entityType}:${entityId}:${action}:${count ?? "?"}`;
      }
    }
    if (n.category === "admin") {
      const uid = meta?.["uid"] as number | undefined;
      if (typeof uid === "number") return `admin:${uid}`;
    }
    return undefined;
  }

  private loadDismissedKeys(): Set<string> {
    try {
      const raw = localStorage.getItem(DISMISSED_KEYS_STORAGE_KEY);
      if (!raw) return new Set();
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return new Set();
      return new Set(parsed.filter((s): s is string => typeof s === "string"));
    } catch {
      return new Set();
    }
  }

  private saveDismissedKeys(keys: Set<string>): void {
    try {
      // Bound the size so the dismissed set can't grow unbounded.
      const arr = Array.from(keys).slice(-MAX_DISMISSED_KEYS);
      localStorage.setItem(DISMISSED_KEYS_STORAGE_KEY, JSON.stringify(arr));
    } catch {
      // Storage may be unavailable; ignore.
    }
  }

  public isDismissed(signature: string): boolean {
    return this.loadDismissedKeys().has(signature);
  }

  public dismiss(signatures: ReadonlyArray<string>): void {
    if (signatures.length === 0) return;
    const keys = this.loadDismissedKeys();
    for (const s of signatures) keys.add(s);
    this.saveDismissedKeys(keys);
  }

  public undismiss(signature: string): void {
    const keys = this.loadDismissedKeys();
    if (keys.delete(signature)) {
      this.saveDismissedKeys(keys);
    }
  }

  public markAllRead(category?: AgentNotificationCategory): void {
    const next = this.notificationsSubject.value.map(n =>
      !category || n.category === category ? { ...n, read: true } : n
    );
    this.notificationsSubject.next(next);
    this.persist();
  }

  public clear(category?: AgentNotificationCategory): void {
    const next = category ? this.notificationsSubject.value.filter(n => n.category !== category) : [];
    this.notificationsSubject.next(next);
    this.persist();
  }

  private persist(): void {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(this.notificationsSubject.value));
    } catch {
      // Storage may be unavailable (private mode, quota); ignore.
    }
  }

  private persistSettings(): void {
    try {
      localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify(this.settingsSubject.value));
    } catch {
      // Storage may be unavailable (private mode, quota); ignore.
    }
  }

  private static loadSettingsFromStorage(): AgentNotificationSettings {
    try {
      const raw = localStorage.getItem(SETTINGS_STORAGE_KEY);
      if (!raw) return { ...DEFAULT_NOTIFICATION_SETTINGS };
      const parsed = JSON.parse(raw) as Partial<AgentNotificationSettings>;
      return { ...DEFAULT_NOTIFICATION_SETTINGS, ...parsed };
    } catch {
      return { ...DEFAULT_NOTIFICATION_SETTINGS };
    }
  }

  private static loadFromStorage(): AgentNotification[] {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) return [];
      return parsed.filter(FloatingAgentService.isValidNotification).slice(0, MAX_NOTIFICATIONS);
    } catch {
      return [];
    }
  }

  private static isValidNotification(value: unknown): value is AgentNotification {
    if (typeof value !== "object" || value === null) return false;
    const n = value as Record<string, unknown>;
    return (
      typeof n.id === "string" &&
      (n.category === "run" || n.category === "social" || n.category === "admin") &&
      (n.level === "info" || n.level === "success" || n.level === "warning" || n.level === "error") &&
      typeof n.title === "string" &&
      typeof n.message === "string" &&
      typeof n.timestamp === "number" &&
      typeof n.read === "boolean"
    );
  }
}

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
import { BehaviorSubject, Observable, Subject } from "rxjs";

export interface AgentReport {
  /** Markdown body extracted from between REPORT_START / REPORT_END markers. */
  markdown: string;
  /** ms since epoch when the report was produced by the agent. */
  timestamp: number;
  /** Agent step / message ID this report came from (for dedup). */
  sourceId: string;
}

const MARKER_RE = /<!--\s*REPORT_START\s*-->([\s\S]*?)<!--\s*REPORT_END\s*-->/i;

/**
 * Splits agent message content on the REPORT_START/REPORT_END markers.
 * Returns the prose to render inline in chat (markers removed) and the
 * extracted report markdown if present.
 */
export function extractReport(content: string): { inline: string; report?: string } {
  const m = MARKER_RE.exec(content);
  if (!m) return { inline: content };
  const report = (m[1] ?? "").trim();
  const inline = (content.slice(0, m.index) + content.slice(m.index + m[0].length)).trim();
  return { inline, report };
}

/**
 * Holds the most recent agent-generated report and broadcasts changes to the
 * Results Dashboard panel. The chat component calls `publish` whenever it
 * detects a report marker in an agent step; the dashboard subscribes to
 * `currentReport$` to render it and `openRequests$` to focus itself when the
 * user clicks "View Report".
 */
@Injectable({ providedIn: "root" })
export class AgentReportService {
  private readonly current = new BehaviorSubject<AgentReport | null>(null);
  private readonly openRequests = new Subject<void>();

  readonly currentReport$: Observable<AgentReport | null> = this.current.asObservable();
  readonly openRequests$: Observable<void> = this.openRequests.asObservable();

  publish(report: AgentReport): void {
    const existing = this.current.value;
    if (existing && existing.sourceId === report.sourceId) return;
    this.current.next(report);
  }

  requestOpen(): void {
    this.openRequests.next();
  }

  clear(): void {
    this.current.next(null);
  }

  get snapshot(): AgentReport | null {
    return this.current.value;
  }
}

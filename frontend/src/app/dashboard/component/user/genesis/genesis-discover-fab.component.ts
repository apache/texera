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

import { CommonModule } from "@angular/common";
import { Component, Injectable, OnInit } from "@angular/core";
import { NavigationEnd, Router } from "@angular/router";
import { NzNotificationService } from "ng-zorro-antd/notification";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { Subject } from "rxjs";
import { filter } from "rxjs/operators";

const FIRST_SEEN_KEY = "genesis_first_seen";
const HELP_SEEN_KEY = "genesis_help_seen";

/**
 * Allows any component (e.g. the thin Workflows-list banner) to programmatically
 * open the floating Discover popover without holding a reference to the FAB component.
 */
@Injectable({ providedIn: "root" })
export class GenesisDiscoverPopoverService {
  private readonly openSubject = new Subject<void>();
  readonly openRequested$ = this.openSubject.asObservable();
  open(): void {
    this.openSubject.next();
  }
}

const DIABETES_SAMPLE = [
  "pregnancies,glucose,blood_pressure,skin_thickness,insulin,bmi,diabetes_pedigree,age,outcome",
  "6,148,72,35,0,33.6,0.627,50,1",
  "1,85,66,29,0,26.6,0.351,31,0",
  "8,183,64,0,0,23.3,0.672,32,1",
  "1,89,66,23,94,28.1,0.167,21,0",
  "0,137,40,35,168,43.1,2.288,33,1",
  "5,116,74,0,0,25.6,0.201,30,0",
  "3,78,50,32,88,31.0,0.248,26,1",
  "10,115,0,0,0,35.3,0.134,29,0",
  "2,197,70,45,543,30.5,0.158,53,1",
  "8,125,96,0,0,0.0,0.232,54,1",
].join("\n");

@Component({
  selector: "texera-genesis-discover-fab",
  standalone: true,
  imports: [CommonModule, NzPopoverModule],
  template: `
    <div
      *ngIf="visible"
      class="genesis-fab-root">
      <button
        type="button"
        class="genesis-fab"
        [class.genesis-fab--open]="popoverOpen"
        nz-popover
        nzPopoverTrigger="click"
        nzPopoverPlacement="topRight"
        [(nzPopoverVisible)]="popoverOpen"
        (nzPopoverVisibleChange)="onVisibleChange($event)"
        [nzPopoverContent]="fabPopover"
        [nzPopoverOverlayClassName]="'genesis-fab-overlay'"
        aria-label="Discover BioFlow Genesis">
        <svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="1.6"
          stroke-linecap="round"
          stroke-linejoin="round"
          aria-hidden="true">
          <path d="M5 3v4" />
          <path d="M3 5h4" />
          <path d="M19 13v4" />
          <path d="M17 15h4" />
          <path d="M13 3l2.5 6.5L22 12l-6.5 2.5L13 21l-2.5-6.5L4 12l6.5-2.5L13 3z" />
        </svg>
        <span
          *ngIf="showPulse"
          class="genesis-fab__pulse"
          aria-hidden="true"></span>
      </button>

      <ng-template #fabPopover>
        <div class="genesis-pop">
          <div class="genesis-pop__head">
            <span class="genesis-pop__eyebrow">AI · Beta</span>
            <h3 class="genesis-pop__title">BioFlow Genesis</h3>
          </div>
          <p class="genesis-pop__body">
            Drag any CSV file onto Texera — AI will recognize the data and build a workflow for you.
          </p>
          <div class="genesis-pop__divider"></div>
          <p class="genesis-pop__try">Try it with our sample:</p>
          <button
            type="button"
            class="genesis-pop__sample"
            (click)="downloadSample()">
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="1.6"
              stroke-linecap="round"
              stroke-linejoin="round"
              aria-hidden="true">
              <path d="M12 3v12" />
              <path d="M7 10l5 5 5-5" />
              <path d="M5 21h14" />
            </svg>
            <span>diabetes.csv</span>
            <span class="genesis-pop__sample-meta">10 rows · 9 cols</span>
          </button>
          <div class="genesis-pop__foot">
            <button
              type="button"
              class="genesis-pop__got-it"
              (click)="closePopover()">
              Got it
            </button>
          </div>
        </div>
      </ng-template>
    </div>
  `,
  styles: [
    `
      :host {
        position: fixed;
        right: 24px;
        bottom: 24px;
        z-index: 999;
        pointer-events: none;
      }

      .genesis-fab-root {
        pointer-events: auto;
      }

      .genesis-fab {
        position: relative;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 44px;
        height: 44px;
        padding: 0;
        border-radius: 999px;
        background: #ffffff;
        border: 1px solid #e5e7eb;
        color: #1e40af;
        cursor: pointer;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
        transition:
          transform 200ms ease-out,
          box-shadow 200ms ease-out,
          border-color 200ms ease-out,
          color 200ms ease-out;
      }

      .genesis-fab:hover,
      .genesis-fab--open {
        transform: translateY(-1px);
        border-color: #c7d2fe;
        color: #1e3a8a;
        box-shadow: 0 4px 12px rgba(30, 64, 175, 0.12);
      }

      .genesis-fab:focus-visible {
        outline: 2px solid rgba(30, 64, 175, 0.45);
        outline-offset: 2px;
      }

      .genesis-fab__pulse {
        position: absolute;
        top: 6px;
        right: 6px;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #a855f7;
        box-shadow: 0 0 0 0 rgba(168, 85, 247, 0.55);
        animation: genesis-fab-pulse 2.4s ease-out infinite;
      }

      @keyframes genesis-fab-pulse {
        0% {
          box-shadow: 0 0 0 0 rgba(168, 85, 247, 0.5);
        }
        70% {
          box-shadow: 0 0 0 10px rgba(168, 85, 247, 0);
        }
        100% {
          box-shadow: 0 0 0 0 rgba(168, 85, 247, 0);
        }
      }
    `,
  ],
})
export class GenesisDiscoverFabComponent implements OnInit {
  public visible = false;
  public popoverOpen = false;
  public showPulse = false;
  private toastShown = false;

  constructor(
    private router: Router,
    private notification: NzNotificationService,
    private discoverPopover: GenesisDiscoverPopoverService
  ) {}

  ngOnInit(): void {
    this.visible = this.isGenesisRoute(this.router.url);
    this.showPulse = localStorage.getItem(HELP_SEEN_KEY) !== "1";

    this.router.events.pipe(filter(e => e instanceof NavigationEnd)).subscribe(e => {
      this.visible = this.isGenesisRoute((e as NavigationEnd).urlAfterRedirects);
    });

    this.discoverPopover.openRequested$.subscribe(() => {
      if (!this.visible) {
        return;
      }
      this.popoverOpen = true;
      localStorage.setItem(HELP_SEEN_KEY, "1");
      this.showPulse = false;
    });

    this.maybeShowFirstSeenToast();
  }

  public onVisibleChange(open: boolean): void {
    this.popoverOpen = open;
    if (open) {
      localStorage.setItem(HELP_SEEN_KEY, "1");
      this.showPulse = false;
    }
  }

  public closePopover(): void {
    this.popoverOpen = false;
  }

  public downloadSample(): void {
    const blob = new Blob([DIABETES_SAMPLE], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "diabetes-sample.csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 500);
  }

  private maybeShowFirstSeenToast(): void {
    if (this.toastShown) {
      return;
    }
    if (localStorage.getItem(FIRST_SEEN_KEY) === "1") {
      return;
    }
    this.toastShown = true;
    setTimeout(() => {
      if (!this.isGenesisRoute(this.router.url)) {
        return;
      }
      this.notification.blank(
        "✨ Try BioFlow Genesis",
        "Drag any CSV file to auto-build an AI workflow.",
        { nzPlacement: "topRight", nzDuration: 5500 }
      );
      localStorage.setItem(FIRST_SEEN_KEY, "1");
    }, 3000);
  }

  private isGenesisRoute(rawUrl: string): boolean {
    const raw = (rawUrl || "/").split("?")[0];
    let path = raw === "" ? "/" : raw;
    if (path.length > 1) {
      path = path.replace(/\/+$/, "");
    }
    if (path === "/") {
      return true;
    }
    if (path === "/dashboard" || path.startsWith("/dashboard/home")) {
      return true;
    }
    return /^\/dashboard\/user\/workflow(\/\d+)?$/.test(path);
  }
}

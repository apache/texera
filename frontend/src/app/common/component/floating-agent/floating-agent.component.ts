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

import { Component, ElementRef, OnDestroy, OnInit } from "@angular/core";
import { CommonModule, DatePipe } from "@angular/common";
import { NavigationEnd, Router } from "@angular/router";
import { CdkDrag, CdkDragEnd, CdkDragHandle } from "@angular/cdk/drag-drop";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Observable, Subscription, BehaviorSubject, combineLatest, of, timer } from "rxjs";
import { catchError, filter, map, switchMap, startWith } from "rxjs/operators";
import { FormsModule } from "@angular/forms";
import { NzBadgeModule } from "ng-zorro-antd/badge";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzSwitchModule } from "ng-zorro-antd/switch";
import { NzDividerModule } from "ng-zorro-antd/divider";
import { MarkdownComponent } from "ngx-markdown";

import { UserService } from "../../service/user/user.service";
import { WorkflowPersistService } from "../../service/workflow-persist/workflow-persist.service";
import { Role, User } from "../../type/user";
import {
  ExecutionState,
  ExecutionStateInfo,
} from "../../../workspace/types/execute-workflow.interface";
import { ExecuteWorkflowService } from "../../../workspace/service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../../workspace/service/workflow-graph/model/workflow-action.service";
import {
  ActionType,
  CountResponse,
  EntityType,
  HubService,
} from "../../../hub/service/hub.service";
import { AdminUserService } from "../../../dashboard/service/admin/user/admin-user.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { SearchResultItem } from "../../../dashboard/type/search-result";
import { SortMethod } from "../../../dashboard/type/sort-method";
import { AgentPanelControlService } from "../../../workspace/service/agent/agent-panel-control.service";
import { AgentService } from "../../../workspace/service/agent/agent.service";
import {
  DASHBOARD_ADMIN_USER,
  DASHBOARD_USER_DATASET,
  DASHBOARD_USER_WORKSPACE,
} from "../../../app-routing.constant";
import {
  AgentNotification,
  AgentNotificationAction,
  AgentNotificationCategory,
  AgentNotificationSettings,
  FloatingAgentService,
} from "./floating-agent.service";

const SOCIAL_POLL_MS = 30_000;
const ADMIN_POLL_MS = 60_000;
const MAX_WORKFLOWS_TO_TRACK = 20;
const MAX_DATASETS_TO_TRACK = 20;
const MAX_SESSION_WORKFLOWS = 20;
const POSITION_STORAGE_KEY = "texera-floating-agent-position";
const EXECUTION_SNAPSHOT_STORAGE_KEY = "texera-floating-agent-execution-snapshot";
const TERMINAL_DEDUP_STORAGE_KEY = "texera-floating-agent-terminal-dedup";
const TERMINAL_DEDUP_WINDOW_MS = 30_000;
const SESSION_WORKFLOWS_STORAGE_KEY = "texera-floating-agent-session-workflows";
const SESSION_WORKFLOWS_DISMISSED_STORAGE_KEY = "texera-floating-agent-session-workflows-dismissed";
/** Must match the key used by AgentPanelComponent (per-workflow agent binding). */
const AGENT_BY_WORKFLOW_STORAGE_KEY = "agent-panel-active-agent-by-workflow";
/** Stop waiting for the agent's reply after this long. */
const AI_SUGGESTION_TIMEOUT_MS = 60_000;
const PANEL_SIZE_STORAGE_KEY = "texera-floating-agent-panel-size";

const PANEL_DEFAULT_WIDTH = 360;
const PANEL_DEFAULT_HEIGHT = 520;
const PANEL_MIN_WIDTH = 320;
const PANEL_MIN_HEIGHT = 360;

interface SessionWorkflow {
  wid?: number;
  name: string;
  state: ExecutionState;
  timestamp: number;
}

const RUN_ERROR_HINTS: Partial<Record<ExecutionState, string>> = {
  [ExecutionState.Failed]:
    "Open the run's console panel to see the operator stack trace. Common causes: bad UDF code, missing input columns, or dataset path typo.",
  [ExecutionState.Killed]:
    "The execution was killed — check whether the computing unit ran out of memory or was stopped manually.",
};

@UntilDestroy()
@Component({
  selector: "texera-floating-agent",
  standalone: true,
  templateUrl: "./floating-agent.component.html",
  styleUrls: ["./floating-agent.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    CdkDrag,
    CdkDragHandle,
    NzBadgeModule,
    NzIconModule,
    NzTabsModule,
    NzButtonModule,
    NzEmptyModule,
    NzTooltipModule,
    NzSwitchModule,
    NzDividerModule,
    MarkdownComponent,
  ],
  providers: [DatePipe],
})
export class FloatingAgentComponent implements OnInit, OnDestroy {
  public isOpen = false;
  public isAdmin = false;
  public isLoggedIn = false;
  public isSettingsOpen = false;
  public isSearchOpen = false;
  public isOnWorkflowPage = false;
  public isAgentPanelOpen = false;
  public searchQuery = "";
  public searchType: "all" | "workflow" | "dataset" = "all";
  public searchResults: SearchResultItem[] = [];
  public searchLoading = false;
  private searchSub?: Subscription;
  /** ID of the operator displayed in the Operator tab. Sticky — does NOT clear on
   *  canvas deselection; only changes when the user selects a different operator. */
  public selectedOperatorId?: string;
  public selectedOperatorType?: string;
  public selectedOperatorProperties?: Record<string, unknown>;
  public operatorExplanation?: string;
  public operatorExplanationLoading = false;
  /** Cache of explanations keyed by operator id so switching between operators
   *  doesn't lose the prior AI response. */
  private operatorExplanationCache: Map<string, string> = new Map();
  /** Panel anchor flip flags — computed when the panel opens based on viewport space. */
  public panelOpensDown = false;
  public panelOpensRight = false;
  public dragPosition: { x: number; y: number } = this.loadPosition();
  public panelWidth = this.loadPanelSize().width;
  public panelHeight = this.loadPanelSize().height;
  public readonly panelMinWidth = PANEL_MIN_WIDTH;
  public readonly panelMinHeight = PANEL_MIN_HEIGHT;
  /** Viewport-relative position of the panel (computed in computePanelAnchor). */
  public panelLeft = 0;
  public panelTop = 0;
  /** Set in cdkDragEnded when a real drag (>4px) occurred; swallows the click the browser fires next. */
  private suppressNextClick = false;

  public readonly settings$ = this.agentService.settings$;

  public readonly unreadTotal$ = this.agentService.unreadCount$;
  public readonly unreadRun$ = this.agentService.unreadCountByCategory$("run");
  public readonly unreadSocial$ = this.agentService.unreadCountByCategory$("social");
  public readonly unreadAdmin$ = this.agentService.unreadCountByCategory$("admin");

  public readonly runs$ = this.agentService.notificationsByCategory$("run");
  public readonly social$ = this.agentService.notificationsByCategory$("social");
  public readonly admin$ = this.agentService.notificationsByCategory$("admin");

  private readonly sessionWorkflowsSubject = new BehaviorSubject<SessionWorkflow[]>(
    FloatingAgentComponent.loadSessionWorkflows()
  );
  public readonly sessionWorkflows$ = this.sessionWorkflowsSubject.asObservable();

  public readonly notifications$ = combineLatest([this.runs$, this.social$]).pipe(
    map(([runs, social]) => [...runs, ...social])
  );

  public readonly unreadNotifications$ = combineLatest([this.unreadRun$, this.unreadSocial$]).pipe(
    map(([runCount, socialCount]) => runCount + socialCount)
  );

  /** Baseline counts captured after the first poll so we only notify on increases. */
  private socialBaseline: Map<string, number> = new Map();
  /** Uids surfaced in this session's admin notifications — used to avoid duplicate pushes
   *  before the user clicks/marks-viewed and the next poll cycle returns. */
  private adminNotifiedThisSession: Set<number> = new Set();
  private socialPollSub?: Subscription;
  private adminPollSub?: Subscription;
  /**
   * Workflow identity captured when execution starts. Used at terminal-state time so we
   * report the right name/wid even if the user navigated away (which resets the live
   * WorkflowActionService metadata to DEFAULT_WORKFLOW / "Untitled Workflow"). Persisted
   * so a page reload mid-run still gives us the right name when the terminal state lands.
   */
  private executionSnapshot?: { wid?: number; name?: string };
  /** Last seen user uid so we only clear notifications on a real identity change. */
  private lastUserUid?: number;

  constructor(
    private agentService: FloatingAgentService,
    private userService: UserService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private datasetService: DatasetService,
    private hubService: HubService,
    private adminUserService: AdminUserService,
    private agentPanelControlService: AgentPanelControlService,
    private workspaceAgentService: AgentService,
    private searchService: SearchService,
    private router: Router,
    private elementRef: ElementRef<HTMLElement>
  ) {}

  ngOnInit(): void {
    this.executionSnapshot = this.loadExecutionSnapshot();
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => this.onUserChanged(user));
    this.subscribeRunEvents();
    this.subscribeRouteChanges();
    // Track the AI agent panel's open state so we can hide the flask button while it's open.
    this.agentPanelControlService.openState$
      .pipe(untilDestroyed(this))
      .subscribe(isOpen => (this.isAgentPanelOpen = isOpen));
    // Track operator selection so the Operator tab can show what's selected and
    // offer to explain it via the bound AI agent.
    this.subscribeOperatorHighlight();
  }

  private subscribeOperatorHighlight(): void {
    const jointWrapper = this.workflowActionService.getJointGraphWrapper();
    const sync = (): void => {
      const ids = jointWrapper.getCurrentHighlightedOperatorIDs();
      const newId = ids.length === 1 ? ids[0] : undefined;
      // Sticky: ignore deselections so the user can keep reading the explanation
      // after they click off the operator. Only react to actual new selections.
      if (!newId) return;
      if (newId === this.selectedOperatorId) return;
      this.selectedOperatorId = newId;
      this.operatorExplanationLoading = false;
      // Load cached explanation for this operator (undefined if never asked).
      this.operatorExplanation = this.operatorExplanationCache.get(newId);
      try {
        const op = this.workflowActionService.getTexeraGraph().getOperator(newId);
        this.selectedOperatorType = op.operatorType;
        this.selectedOperatorProperties = op.operatorProperties as Record<string, unknown>;
      } catch {
        this.selectedOperatorType = undefined;
        this.selectedOperatorProperties = undefined;
      }
    };
    jointWrapper.getJointOperatorHighlightStream().pipe(untilDestroyed(this)).subscribe(() => sync());
    sync();
  }

  private subscribeRouteChanges(): void {
    // Set initial value based on current URL
    this.isOnWorkflowPage = this.urlMatchesWorkflowEditor(this.router.url);
    // Then update on every navigation
    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        untilDestroyed(this)
      )
      .subscribe(event => {
        this.isOnWorkflowPage = this.urlMatchesWorkflowEditor(event.urlAfterRedirects);
      });
  }

  private urlMatchesWorkflowEditor(url: string): boolean {
    // Workflow editor URL pattern: /dashboard/user/workflow/:wid
    return /\/dashboard\/user\/workflow\/\d+/.test(url);
  }

  public openAgentPanel(event?: Event): void {
    event?.stopPropagation();
    if (this.suppressNextClick) {
      this.suppressNextClick = false;
      return;
    }
    this.agentPanelControlService.requestToggle();
  }

  ngOnDestroy(): void {
    this.stopPolling();
  }

  // ---------- UI ----------

  public togglePanel(): void {
    if (this.suppressNextClick) {
      this.suppressNextClick = false;
      return;
    }
    this.isOpen = !this.isOpen;
    if (this.isOpen) {
      this.computePanelAnchor();
      this.agentService.markAllRead();
    }
  }

  /**
   * Decide which side of the floating button the panel should expand toward,
   * then compute the panel's viewport-fixed position. Default is up-left; we
   * flip to down/right whenever the button is too close to the top/left edges
   * of the viewport to fit the panel in the default direction.
   */
  private computePanelAnchor(): void {
    const buttonEl = this.elementRef.nativeElement.querySelector(
      ".agent-button:not(.agent-button-secondary)"
    ) as HTMLElement | null;
    if (!buttonEl) return;

    const rect = buttonEl.getBoundingClientRect();
    const GAP = 12;

    // Vertical: prefer opening upward; flip downward if not enough room above.
    this.panelOpensDown = rect.top < this.panelHeight + GAP;
    // Horizontal: panel default extends to the LEFT of the button; flip if no room.
    this.panelOpensRight = rect.right < this.panelWidth + GAP;

    // Compute viewport-fixed position. The panel is `position: fixed`, so left/top
    // are in viewport coordinates.
    if (this.panelOpensRight) {
      // Panel aligned to the LEFT edge of the button, extending right.
      this.panelLeft = rect.left;
    } else {
      // Panel aligned to the RIGHT edge of the button, extending left.
      this.panelLeft = rect.right - this.panelWidth;
    }
    if (this.panelOpensDown) {
      // Panel below the button.
      this.panelTop = rect.bottom + GAP;
    } else {
      // Panel above the button.
      this.panelTop = rect.top - this.panelHeight - GAP;
    }
  }

  public closePanel(): void {
    this.isOpen = false;
    this.isSettingsOpen = false;
    this.isSearchOpen = false;
  }

  public toggleSettings(event?: Event): void {
    event?.stopPropagation();
    this.isSettingsOpen = !this.isSettingsOpen;
    if (this.isSettingsOpen) this.isSearchOpen = false;
  }

  public toggleSearch(event?: Event): void {
    event?.stopPropagation();
    this.isSearchOpen = !this.isSearchOpen;
    if (this.isSearchOpen) this.isSettingsOpen = false;
  }

  public runSearch(): void {
    const keyword = this.searchQuery.trim();
    if (!keyword) {
      this.searchResults = [];
      return;
    }
    this.searchSub?.unsubscribe();
    this.searchLoading = true;
    const emptyFilters = {
      createDateStart: null,
      createDateEnd: null,
      modifiedDateStart: null,
      modifiedDateEnd: null,
      owners: [],
      ids: [],
      operators: [],
      projectIds: [],
    };
    const type: "workflow" | "dataset" | null = this.searchType === "all" ? null : this.searchType;
    this.searchSub = this.searchService
      .search([keyword], emptyFilters, 0, 20, type, SortMethod.EditTimeDesc, this.isLoggedIn, false)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: result => {
          this.searchResults = result.results.filter(
            r => r.resourceType === "workflow" || r.resourceType === "dataset"
          );
          this.searchLoading = false;
        },
        error: err => {
          console.error("[FloatingAgent] search failed:", err);
          this.searchResults = [];
          this.searchLoading = false;
        },
      });
  }

  public openSearchResult(item: SearchResultItem): void {
    let route: unknown[] | undefined;
    if (item.resourceType === "workflow" && item.workflow?.workflow?.wid !== undefined) {
      route = [DASHBOARD_USER_WORKSPACE, item.workflow.workflow.wid];
    } else if (item.resourceType === "dataset" && item.dataset?.dataset?.did !== undefined) {
      route = [DASHBOARD_USER_DATASET, item.dataset.dataset.did];
    }
    if (route) {
      this.router.navigate(route);
      this.closePanel();
    }
  }

  public updateSetting(key: keyof AgentNotificationSettings, value: boolean): void {
    this.agentService.updateSettings({ [key]: value });
  }

  public clearCategory(category: AgentNotificationCategory, event?: Event): void {
    event?.stopPropagation();
    this.agentService.clear(category);
  }

  public clearByKind(kind: "notifications" | "requests" | undefined, event?: Event): void {
    event?.stopPropagation();
    if (kind === "requests") {
      // Mark every currently-pending request as viewed in the DB (not just the ones
      // currently shown in the panel). This handles the case where a new signup
      // arrived between polls but isn't reflected in the notifications list yet.
      this.adminUserService
        .markAllRequestsViewed()
        .pipe(untilDestroyed(this))
        .subscribe({
          error: err => console.error("Failed to mark all requests as viewed:", err),
        });
      this.adminNotifiedThisSession.clear();
      this.agentService.clear("admin");
    } else {
      // Default: combined notifications tab (runs + social)
      this.agentService.clear("run");
      this.agentService.clear("social");
    }
  }

  public clearSessionWorkflows(event?: Event): void {
    event?.stopPropagation();
    // Remember which workflow ids the user just dismissed so passive state events
    // (websocket replays on reload, state polling, etc.) don't re-populate them.
    // The dismissal is lifted automatically when the workflow next enters the
    // Running state — that's a clear user-initiated re-run.
    const currentWids = this.sessionWorkflowsSubject.value
      .map(w => w.wid)
      .filter((w): w is number => typeof w === "number");
    if (currentWids.length > 0) {
      const dismissed = this.loadDismissedSessionWorkflows();
      currentWids.forEach(wid => dismissed.add(wid));
      this.saveDismissedSessionWorkflows(dismissed);
    }
    this.sessionWorkflowsSubject.next([]);
    this.persistSessionWorkflows();
  }

  private loadDismissedSessionWorkflows(): Set<number> {
    try {
      const raw = localStorage.getItem(SESSION_WORKFLOWS_DISMISSED_STORAGE_KEY);
      if (!raw) return new Set();
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return new Set();
      return new Set(parsed.filter((n): n is number => typeof n === "number"));
    } catch {
      return new Set();
    }
  }

  private saveDismissedSessionWorkflows(set: Set<number>): void {
    try {
      localStorage.setItem(SESSION_WORKFLOWS_DISMISSED_STORAGE_KEY, JSON.stringify([...set]));
    } catch {
      // Storage may be unavailable; ignore.
    }
  }

  public triggerAction(n: AgentNotification, event?: Event): void {
    event?.stopPropagation();
    if (!n.action) return;

    const route = n.action.route[0] as string;

    // Handle special internal actions
    if (route === "__retry-workflow__") {
      const wid = n.action.route[1];
      this.handleRetryWorkflow(wid as number);
      return;
    }

    // Admin request notifications: clicking is an implicit acknowledgement. Mark the
    // request as viewed in the DB and immediately remove this specific notification from
    // the list (so it doesn't reappear from localStorage on refresh).
    if (n.category === "admin") {
      const uid = (n.meta as { uid?: number } | undefined)?.uid;
      if (typeof uid === "number") {
        this.adminUserService
          .markRequestsViewed([uid])
          .pipe(untilDestroyed(this))
          .subscribe({
            error: err => console.error("Failed to mark request as viewed:", err),
          });
        this.adminNotifiedThisSession.delete(uid);
      }
      this.agentService.removeWhere(other => other.id === n.id);
    }

    // Normal navigation
    this.router.navigate(n.action.route);
    this.closePanel();
  }

  public onDragEnded(event: CdkDragEnd): void {
    const { x, y } = event.source.getFreeDragPosition();
    this.dragPosition = { x, y };
    if (Math.hypot(event.distance.x, event.distance.y) > 4) {
      this.suppressNextClick = true;
    }
    try {
      localStorage.setItem(POSITION_STORAGE_KEY, JSON.stringify(this.dragPosition));
    } catch {
      // Storage may be unavailable (private mode, quota); position will reset next reload.
    }
  }

  private loadPosition(): { x: number; y: number } {
    try {
      const raw = localStorage.getItem(POSITION_STORAGE_KEY);
      if (!raw) return { x: 0, y: 0 };
      const parsed = JSON.parse(raw) as { x: unknown; y: unknown };
      if (typeof parsed?.x === "number" && typeof parsed?.y === "number") {
        return { x: parsed.x, y: parsed.y };
      }
    } catch {
      // Ignore malformed stored value.
    }
    return { x: 0, y: 0 };
  }

  private loadPanelSize(): { width: number; height: number } {
    try {
      const raw = localStorage.getItem(PANEL_SIZE_STORAGE_KEY);
      if (!raw) return { width: PANEL_DEFAULT_WIDTH, height: PANEL_DEFAULT_HEIGHT };
      const parsed = JSON.parse(raw) as { width: unknown; height: unknown };
      const width =
        typeof parsed?.width === "number" && parsed.width >= PANEL_MIN_WIDTH
          ? parsed.width
          : PANEL_DEFAULT_WIDTH;
      const height =
        typeof parsed?.height === "number" && parsed.height >= PANEL_MIN_HEIGHT
          ? parsed.height
          : PANEL_DEFAULT_HEIGHT;
      return { width, height };
    } catch {
      return { width: PANEL_DEFAULT_WIDTH, height: PANEL_DEFAULT_HEIGHT };
    }
  }

  /**
   * Resize handles to expose based on which edges of the panel are free
   * (i.e., not anchored to the floating button). The opposite anchor is fixed,
   * so resizing those edges would feel broken.
   */
  public get panelResizeHandles(): string[] {
    if (this.panelOpensDown && this.panelOpensRight) {
      return ["right", "bottom", "bottomRight"];
    }
    if (this.panelOpensDown) {
      return ["left", "bottom", "bottomLeft"];
    }
    if (this.panelOpensRight) {
      return ["right", "top", "topRight"];
    }
    return ["left", "top", "topLeft"];
  }

  /**
   * Start a resize gesture from the given handle direction. The panel is anchored
   * by CSS (right:0 / left:0 / top:* / bottom:*) so we only need to change width and
   * height — the opposite edge stays fixed automatically.
   */
  public startResize(direction: string, event: MouseEvent): void {
    event.preventDefault();
    event.stopPropagation();

    const startX = event.clientX;
    const startY = event.clientY;
    const startWidth = this.panelWidth;
    const startHeight = this.panelHeight;
    const maxSize = 900;

    const movesLeft = direction.toLowerCase().includes("left");
    const movesRight = direction.toLowerCase().includes("right");
    const movesTop = direction.toLowerCase().startsWith("top");
    const movesBottom = direction.toLowerCase().startsWith("bottom");

    const onMouseMove = (e: MouseEvent) => {
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;

      // For LEFT handle: panel's right edge is anchored, so dragging left (negative dx)
      // should grow width. For RIGHT handle: panel's left edge is anchored, dragging
      // right (positive dx) grows width.
      if (movesRight) {
        this.panelWidth = Math.min(maxSize, Math.max(PANEL_MIN_WIDTH, startWidth + dx));
      } else if (movesLeft) {
        this.panelWidth = Math.min(maxSize, Math.max(PANEL_MIN_WIDTH, startWidth - dx));
      }

      if (movesBottom) {
        this.panelHeight = Math.min(maxSize, Math.max(PANEL_MIN_HEIGHT, startHeight + dy));
      } else if (movesTop) {
        this.panelHeight = Math.min(maxSize, Math.max(PANEL_MIN_HEIGHT, startHeight - dy));
      }
    };

    const onMouseUp = () => {
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      try {
        localStorage.setItem(
          PANEL_SIZE_STORAGE_KEY,
          JSON.stringify({ width: this.panelWidth, height: this.panelHeight })
        );
      } catch {
        // Storage may be unavailable; ignore.
      }
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }

  private loadExecutionSnapshot(): { wid?: number; name?: string } | undefined {
    try {
      const raw = localStorage.getItem(EXECUTION_SNAPSHOT_STORAGE_KEY);
      if (!raw) return undefined;
      const parsed = JSON.parse(raw) as { wid?: unknown; name?: unknown };
      const wid = typeof parsed?.wid === "number" ? parsed.wid : undefined;
      const name = typeof parsed?.name === "string" ? parsed.name : undefined;
      if (wid === undefined && name === undefined) return undefined;
      return { wid, name };
    } catch {
      return undefined;
    }
  }

  private persistExecutionSnapshot(): void {
    try {
      if (this.executionSnapshot) {
        localStorage.setItem(EXECUTION_SNAPSHOT_STORAGE_KEY, JSON.stringify(this.executionSnapshot));
      } else {
        localStorage.removeItem(EXECUTION_SNAPSHOT_STORAGE_KEY);
      }
    } catch {
      // Storage may be unavailable; ignore.
    }
  }

  public iconFor(n: AgentNotification): string {
    switch (n.level) {
      case "success":
        return "check-circle";
      case "warning":
        return "exclamation-circle";
      case "error":
        return "close-circle";
      default:
        return "bell";
    }
  }

  public stateIconFor(state: ExecutionState): string {
    switch (state) {
      case ExecutionState.Completed:
        return "check-circle";
      case ExecutionState.Failed:
        return "close-circle";
      case ExecutionState.Killed:
        return "stop";
      case ExecutionState.Running:
        return "loading";
      case ExecutionState.Paused:
        return "pause-circle";
      default:
        return "clock-circle";
    }
  }

  public stateColorFor(state: ExecutionState): string {
    switch (state) {
      case ExecutionState.Completed:
        return "#52c41a";
      case ExecutionState.Failed:
        return "#ff4d4f";
      case ExecutionState.Killed:
        return "#faad14";
      case ExecutionState.Running:
        return "#1677ff";
      default:
        return "#bfbfbf";
    }
  }

  // ---------- User session ----------

  private onUserChanged(user: User | undefined): void {
    const previousUid = this.lastUserUid;
    this.lastUserUid = user?.uid;
    this.isLoggedIn = !!user;
    this.isAdmin = user?.role === Role.ADMIN;
    this.stopPolling();
    // Only wipe persisted state on real identity transitions — not on the initial restore
    // from localStorage (previousUid === undefined && user defined).
    const identityChanged = previousUid !== undefined && previousUid !== user?.uid;
    if (identityChanged) {
      this.agentService.clear();
      this.socialBaseline.clear();
      this.adminNotifiedThisSession.clear();
      this.executionSnapshot = undefined;
      this.persistExecutionSnapshot();
      this.sessionWorkflowsSubject.next([]);
      this.persistSessionWorkflows();
    }
    if (!user) {
      this.isOpen = false;
      return;
    }
    this.startSocialPolling();
    if (this.isAdmin) {
      this.startAdminPolling();
    }
  }

  private stopPolling(): void {
    this.socialPollSub?.unsubscribe();
    this.socialPollSub = undefined;
    this.adminPollSub?.unsubscribe();
    this.adminPollSub = undefined;
  }

  // ---------- Feature 1: workflow run events ----------

  private subscribeRunEvents(): void {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ previous, current }) => this.handleExecutionStateChange(previous, current));
  }

  private handleExecutionStateChange(previous: ExecutionStateInfo, current: ExecutionStateInfo): void {
    // On page reload/HMR, the websocket reconnects and the server replays the current state.
    // This produces a synthetic Uninitialized → [terminal] transition that we must NOT
    // treat as a real event, otherwise we'd push a duplicate notification every refresh.
    const isTerminalState =
      current.state === ExecutionState.Completed ||
      current.state === ExecutionState.Failed ||
      current.state === ExecutionState.Killed;
    if (previous.state === ExecutionState.Uninitialized && isTerminalState) {
      return;
    }

    // Capture identity when execution starts — at this moment WorkflowActionService still
    // holds the live workflow metadata. We need it later because clearWorkflow() (on route
    // change) resets the name to "Untitled Workflow".
    if (previous.state === ExecutionState.Uninitialized && current.state !== ExecutionState.Uninitialized) {
      const metadata = this.workflowActionService.getWorkflowMetadata();
      this.executionSnapshot = { wid: metadata?.wid, name: metadata?.name };
      this.persistExecutionSnapshot();
    }

    // Prefer live metadata over the captured snapshot when both reference the same workflow.
    // This way, if the user renames the workflow mid-run, the notification reflects the new name.
    // Fall back to snapshot only when the editor has been unloaded (user navigated away).
    const liveMetadata = this.workflowActionService.getWorkflowMetadata();
    const snapshotWid = this.executionSnapshot?.wid;
    const useLive = liveMetadata?.wid !== undefined && liveMetadata.wid === snapshotWid;
    const snapshot = useLive
      ? { wid: liveMetadata!.wid, name: liveMetadata!.name }
      : (this.executionSnapshot ?? {
          wid: liveMetadata?.wid,
          name: liveMetadata?.name,
        });
    const workflowName = snapshot.name && snapshot.name.length > 0 ? snapshot.name : "Workflow";

    // Track workflow in session
    this.trackSessionWorkflow(snapshot.wid, workflowName, current.state);

    // Multi-step replay guard: when the websocket reconnects and replays through
    // Uninitialized → Initializing → Running → [terminal], the first guard above
    // doesn't catch the terminal hop because `previous` is no longer Uninitialized.
    // Dedup against (wid, state) within a short window — real reruns take longer
    // than this window, so legitimate notifications still come through.
    if (isTerminalState && this.wasRecentlyNotified(snapshot.wid, current.state)) {
      this.executionSnapshot = undefined;
      this.persistExecutionSnapshot();
      return;
    }

    switch (current.state) {
      case ExecutionState.Completed:
        this.agentService.push({
          category: "run",
          level: "success",
          type: "runSuccess",
          title: `${workflowName} finished`,
          message: "The workflow run completed successfully.",
          action: this.workflowAction(snapshot.wid, "Tap to see result"),
          meta: { wid: snapshot.wid },
        });
        this.recordNotification(snapshot.wid, current.state);
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
      case ExecutionState.Failed: {
        const notificationId = this.agentService.push({
          category: "run",
          level: "error",
          type: "runFailure",
          title: `${workflowName} failed`,
          message: this.summarizeFailure(current),
          hint: RUN_ERROR_HINTS[ExecutionState.Failed],
          action: { label: "Retry", route: ["__retry-workflow__", snapshot.wid] },
          meta: { action: "retry", wid: snapshot.wid },
        });
        // If this workflow has a bound AI agent (per the workflow-agent map), ask it
        // for a remediation suggestion and stream the reply back into the notification.
        if (notificationId && snapshot.wid !== undefined) {
          this.askAgentAboutFailure(notificationId, workflowName, snapshot.wid, current);
        }
        this.recordNotification(snapshot.wid, current.state);
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
      }
      case ExecutionState.Killed:
        this.agentService.push({
          category: "run",
          level: "warning",
          type: "runKilled",
          title: `${workflowName} was killed`,
          message: "Execution stopped before finishing.",
          hint: RUN_ERROR_HINTS[ExecutionState.Killed],
          action: { label: "Retry", route: ["__retry-workflow__", snapshot.wid] },
          meta: { action: "retry", wid: snapshot.wid },
        });
        this.recordNotification(snapshot.wid, current.state);
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
      default:
        return;
    }
  }

  private wasRecentlyNotified(wid: number | undefined, state: ExecutionState): boolean {
    if (wid === undefined) return false;
    const records = this.loadDedupRecords();
    const now = Date.now();
    return records.some(
      r => r.wid === wid && r.state === state && now - r.time < TERMINAL_DEDUP_WINDOW_MS
    );
  }

  private recordNotification(wid: number | undefined, state: ExecutionState): void {
    if (wid === undefined) return;
    const records = this.loadDedupRecords();
    const now = Date.now();
    const filtered = records.filter(r => now - r.time < TERMINAL_DEDUP_WINDOW_MS);
    filtered.push({ wid, state, time: now });
    try {
      localStorage.setItem(TERMINAL_DEDUP_STORAGE_KEY, JSON.stringify(filtered));
    } catch {
      // Storage may be unavailable; ignore.
    }
  }

  private loadDedupRecords(): { wid: number; state: ExecutionState; time: number }[] {
    try {
      const raw = localStorage.getItem(TERMINAL_DEDUP_STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter(
        r => typeof r?.wid === "number" && typeof r?.state === "string" && typeof r?.time === "number"
      );
    } catch {
      return [];
    }
  }

  /**
   * Look up the AI agent bound to the failed workflow (via the workflow→agent map saved
   * by AgentPanelComponent). If one exists and is actively connected, send a debug
   * prompt and stream the reply back into the notification's `aiSuggestion` field.
   * If the workflow has no bound agent, silently skip — only static hint is shown.
   */
  private askAgentAboutFailure(
    notificationId: string,
    workflowName: string,
    wid: number,
    state: ExecutionStateInfo
  ): void {
    if (state.state !== ExecutionState.Failed) return;
    const agentId = this.lookupAgentForWorkflow(wid);
    console.log(`[FloatingAgent] askAgent: wid=${wid}, boundAgentId=${agentId}`);
    if (!agentId) return; // No agent bound to this workflow — skip silently.
    const activeIds = new Set(this.workspaceAgentService.getActivelyConnectedAgentIds());
    if (!activeIds.has(agentId)) {
      console.log(
        `[FloatingAgent] askAgent: bound agent ${agentId} is not actively connected; skipping`
      );
      return;
    }

    const errorDetails = state.errorMessages
      .map(e => (e.operatorId ? `Operator ${e.operatorId}: ${e.message}` : e.message))
      .join("\n");
    const prompt =
      `My workflow "${workflowName}" just failed with the following error(s):\n\n${errorDetails}\n\n` +
      `Provide a concise diagnostic (under 120 words) with:\n` +
      `1. The most likely cause.\n` +
      `2. 1-3 concrete steps to debug or fix it.\n\n` +
      `Do NOT ask me any follow-up questions. Do NOT offer to perform actions or wait ` +
      `for my reply. End your response after the steps.`;

    this.agentService.updateById(notificationId, { aiSuggestionLoading: true });
    const promptSentAt = Date.now();
    let collected = "";
    let completed = false;

    const sub = this.workspaceAgentService
      .getReActStepsObservable(agentId)
      .pipe(untilDestroyed(this))
      .subscribe(steps => {
        if (completed) return;
        // Only consider agent-role steps that arrived after we sent the prompt.
        const after = steps.filter(
          s => s.role === "agent" && new Date(s.timestamp).getTime() >= promptSentAt
        );
        if (after.length === 0) return;
        collected = after.map(s => s.content).filter(Boolean).join("\n").trim();
        const cleaned = this.cleanAiSuggestion(collected);
        if (cleaned) {
          this.agentService.updateById(notificationId, { aiSuggestion: cleaned });
        }
        const last = after[after.length - 1];
        if (last.isEnd) {
          completed = true;
          this.agentService.updateById(notificationId, { aiSuggestionLoading: false });
          sub.unsubscribe();
        }
      });

    // Failsafe: stop showing the spinner after a timeout even if isEnd never arrives.
    setTimeout(() => {
      if (!completed) {
        completed = true;
        this.agentService.updateById(notificationId, { aiSuggestionLoading: false });
        sub.unsubscribe();
      }
    }, AI_SUGGESTION_TIMEOUT_MS);

    try {
      this.workspaceAgentService.sendMessage(agentId, prompt, "chat");
    } catch (err) {
      console.error("[FloatingAgent] sendMessage failed:", err);
      this.agentService.updateById(notificationId, { aiSuggestionLoading: false });
      sub.unsubscribe();
    }
  }

  /**
   * Strip conversational follow-ups (questions to the user, offers to perform
   * actions, etc.) from the trailing portion of the agent's response so the
   * notification stays focused on causes and remediation steps. The agent's full
   * reply remains visible in the AI panel chat.
   */
  private cleanAiSuggestion(text: string): string {
    let cleaned = text.trim();
    // Patterns that suggest a trailing paragraph is conversational and should be dropped.
    const followUpPatterns: RegExp[] = [
      /\?\s*$/, // ends with a question mark
      /^(could|can|would|will|should|do|does)\s+(you|i)\b/i,
      /^(let me know|once you|i'll|i will|i can|i would|please (share|provide|send|tell))\b/i,
      /^(if you (can|could|want))\b/i,
    ];
    // Drop trailing paragraphs/sentences that match a follow-up pattern. Stops as soon
    // as the new last block doesn't match, so legitimate steps with questions inline
    // (e.g., "Is the path correct?" as part of step 2) stay intact.
    for (let i = 0; i < 5; i++) {
      // Try paragraph split first; if there's only one paragraph, fall back to sentences.
      const paraSplit = cleaned.split(/\n\s*\n/);
      const lastPara = paraSplit[paraSplit.length - 1].trim();
      if (lastPara && followUpPatterns.some(p => p.test(lastPara))) {
        cleaned = paraSplit.slice(0, -1).join("\n\n").trim();
        continue;
      }
      // Sentence-level cleanup: strip trailing sentences that match a follow-up.
      const sentences = cleaned.split(/(?<=[.!?])\s+(?=[A-Z])/);
      const lastSent = sentences[sentences.length - 1].trim();
      if (lastSent && followUpPatterns.some(p => p.test(lastSent))) {
        cleaned = sentences.slice(0, -1).join(" ").trim();
        continue;
      }
      break;
    }
    return cleaned;
  }

  /**
   * Ask the workflow's bound AI agent to explain the currently selected operator.
   * Streams the reply into `operatorExplanation` for the Operator tab to render.
   */
  public explainSelectedOperator(): void {
    if (!this.selectedOperatorId || !this.selectedOperatorType) return;
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    const agentId = wid !== undefined ? this.lookupAgentForWorkflow(wid) : undefined;
    if (!agentId) {
      this.operatorExplanation =
        "Bind an AI agent to this workflow (purple flask button) to get operator explanations.";
      return;
    }
    const activeIds = new Set(this.workspaceAgentService.getActivelyConnectedAgentIds());
    if (!activeIds.has(agentId)) {
      this.operatorExplanation = "The bound AI agent isn't currently connected. Open it from the flask button.";
      return;
    }

    const propsJson = JSON.stringify(this.selectedOperatorProperties ?? {}, null, 2);
    const prompt =
      `Briefly explain this Texera operator and its current parameters (under 100 words).\n\n` +
      `Operator type: ${this.selectedOperatorType}\n` +
      `Operator id: ${this.selectedOperatorId}\n` +
      `Current parameters:\n\`\`\`json\n${propsJson}\n\`\`\`\n\n` +
      `Cover: (1) what the operator does, (2) what each non-default parameter is doing here. ` +
      `Do NOT ask follow-up questions or offer to take actions — just explain.`;

    this.operatorExplanation = undefined;
    this.operatorExplanationLoading = true;
    const promptSentAt = Date.now();
    let collected = "";
    let completed = false;

    // Capture the operator id at request time so the cache write later targets
    // the right operator even if the user has clicked something else by then.
    const requestedOperatorId = this.selectedOperatorId;
    const sub = this.workspaceAgentService
      .getReActStepsObservable(agentId)
      .pipe(untilDestroyed(this))
      .subscribe(steps => {
        if (completed) return;
        const after = steps.filter(
          s => s.role === "agent" && new Date(s.timestamp).getTime() >= promptSentAt
        );
        if (after.length === 0) return;
        collected = after.map(s => s.content).filter(Boolean).join("\n").trim();
        const cleaned = this.cleanAiSuggestion(collected);
        if (cleaned) {
          if (requestedOperatorId) this.operatorExplanationCache.set(requestedOperatorId, cleaned);
          // Only update the visible explanation if we're still on the same operator.
          if (requestedOperatorId === this.selectedOperatorId) this.operatorExplanation = cleaned;
        }
        const last = after[after.length - 1];
        if (last.isEnd) {
          completed = true;
          this.operatorExplanationLoading = false;
          sub.unsubscribe();
        }
      });

    setTimeout(() => {
      if (!completed) {
        completed = true;
        this.operatorExplanationLoading = false;
        sub.unsubscribe();
      }
    }, AI_SUGGESTION_TIMEOUT_MS);

    try {
      this.workspaceAgentService.sendMessage(agentId, prompt, "chat");
    } catch (err) {
      console.error("[FloatingAgent] sendMessage (explain) failed:", err);
      this.operatorExplanationLoading = false;
      sub.unsubscribe();
    }
  }

  /** Read the workflow→agent map written by AgentPanelComponent. */
  private lookupAgentForWorkflow(wid: number): string | undefined {
    try {
      const raw = localStorage.getItem(AGENT_BY_WORKFLOW_STORAGE_KEY);
      if (!raw) return undefined;
      const map = JSON.parse(raw) as Record<string, unknown>;
      const value = map[String(wid)];
      return typeof value === "string" ? value : undefined;
    } catch {
      return undefined;
    }
  }

  private workflowAction(wid: number | undefined, label: string): AgentNotificationAction | undefined {
    if (wid === undefined) {
      return undefined;
    }
    return { label, route: [DASHBOARD_USER_WORKSPACE, wid] };
  }

  private summarizeFailure(state: ExecutionStateInfo): string {
    if (state.state !== ExecutionState.Failed) {
      return "The workflow run failed.";
    }
    const errors = state.errorMessages;
    if (errors.length === 0) {
      return "The workflow run failed.";
    }
    const first = errors[0];
    const head = first.operatorId ? `${first.operatorId}: ${first.message}` : first.message;
    return errors.length === 1 ? head : `${head} (+${errors.length - 1} more)`;
  }

  // ---------- Feature 3: hub social events ----------

  private startSocialPolling(): void {
    this.socialPollSub = timer(0, SOCIAL_POLL_MS)
      .pipe(
        switchMap(() => this.fetchHubCounts()),
        untilDestroyed(this)
      )
      .subscribe(snapshot => this.applySocialSnapshot(snapshot));
  }

  private fetchHubCounts(): Observable<{
    counts: CountResponse[];
    nameByEntity: Map<string, string>;
  }> {
    const ownedWorkflows$ = this.workflowPersistService.retrieveWorkflowsBySessionUser().pipe(
      map(list =>
        list
          .filter(w => w.isOwner && w.workflow?.wid !== undefined)
          .slice(0, MAX_WORKFLOWS_TO_TRACK)
          .map(w => ({
            type: EntityType.Workflow,
            id: w.workflow.wid as number,
            name: w.workflow.name ?? `Workflow #${w.workflow.wid}`,
          }))
      ),
      catchError(() => of([] as { type: EntityType; id: number; name: string }[]))
    );
    const ownedDatasets$ = this.datasetService.retrieveAccessibleDatasets().pipe(
      map(list =>
        list
          .filter(d => d.isOwner && d.dataset?.did !== undefined)
          .slice(0, MAX_DATASETS_TO_TRACK)
          .map(d => ({
            type: EntityType.Dataset,
            id: d.dataset.did as number,
            name: d.dataset.name ?? `Dataset #${d.dataset.did}`,
          }))
      ),
      catchError(() => of([] as { type: EntityType; id: number; name: string }[]))
    );
    return combineLatest([ownedWorkflows$, ownedDatasets$]).pipe(
      switchMap(([workflows, datasets]) => {
        const entities = [...workflows, ...datasets];
        const nameByEntity = new Map<string, string>();
        for (const e of entities) {
          nameByEntity.set(this.entityKey(e.type, e.id), e.name);
        }
        if (entities.length === 0) {
          return of({ counts: [] as CountResponse[], nameByEntity });
        }
        const entityTypes = entities.map(e => e.type);
        const entityIds = entities.map(e => e.id);
        return this.hubService
          .getCounts(entityTypes, entityIds, [ActionType.Like, ActionType.Clone])
          .pipe(
            map(counts => ({ counts, nameByEntity })),
            catchError(() => of({ counts: [] as CountResponse[], nameByEntity }))
          );
      }),
      catchError(() =>
        of({ counts: [] as CountResponse[], nameByEntity: new Map<string, string>() })
      )
    );
  }

  private applySocialSnapshot({
    counts,
    nameByEntity,
  }: {
    counts: CountResponse[];
    nameByEntity: Map<string, string>;
  }): void {
    const isFirstPoll = this.socialBaseline.size === 0;
    for (const row of counts) {
      // Clone counts on datasets are not meaningful in Texera today — skip them.
      const trackedActions =
        row.entityType === EntityType.Dataset
          ? [ActionType.Like]
          : [ActionType.Like, ActionType.Clone];
      for (const action of trackedActions) {
        const key = this.socialKey(row.entityType, row.entityId, action);
        const current = row.counts?.[action] ?? 0;
        const previous = this.socialBaseline.get(key) ?? 0;
        if (!isFirstPoll && current > previous) {
          const diff = current - previous;
          const name =
            nameByEntity.get(this.entityKey(row.entityType, row.entityId)) ??
            this.fallbackName(row.entityType, row.entityId);
          this.agentService.push({
            category: "social",
            level: action === ActionType.Like ? "info" : "success",
            type: this.socialNotificationType(row.entityType, action),
            title: action === ActionType.Like ? `New like on ${name}` : `${name} was cloned`,
            message:
              action === ActionType.Like
                ? `+${diff} like${diff === 1 ? "" : "s"} (total ${current}).`
                : `+${diff} clone${diff === 1 ? "" : "s"} (total ${current}).`,
            action: this.socialAction(row.entityType, row.entityId),
            // Include `count` in meta so the dismissal signature changes when the
            // count grows — letting a later increase fire a fresh notification.
            meta: {
              entityType: row.entityType,
              entityId: row.entityId,
              action,
              delta: diff,
              count: current,
            },
          });
        }
        this.socialBaseline.set(key, current);
      }
    }
  }

  private socialKey(type: EntityType, id: number, action: ActionType): string {
    return `${type}:${id}:${action}`;
  }

  private entityKey(type: EntityType, id: number): string {
    return `${type}:${id}`;
  }

  private fallbackName(type: EntityType, id: number): string {
    return type === EntityType.Dataset ? `Dataset #${id}` : `Workflow #${id}`;
  }

  private socialAction(type: EntityType, id: number): AgentNotificationAction | undefined {
    if (type === EntityType.Workflow) {
      return { label: "Tap to open workflow", route: [DASHBOARD_USER_WORKSPACE, id] };
    }
    if (type === EntityType.Dataset) {
      return { label: "Tap to open dataset", route: [DASHBOARD_USER_DATASET, id] };
    }
    return undefined;
  }

  private socialNotificationType(
    type: EntityType,
    action: ActionType
  ): "workflowLikes" | "workflowClones" | "datasetLikes" | undefined {
    if (type === EntityType.Workflow) {
      return action === ActionType.Like ? "workflowLikes" : "workflowClones";
    }
    if (type === EntityType.Dataset) {
      return action === ActionType.Like ? "datasetLikes" : undefined;
    }
    return undefined;
  }

  private trackSessionWorkflow(wid: number | undefined, name: string, state: ExecutionState): void {
    if (wid !== undefined) {
      const dismissed = this.loadDismissedSessionWorkflows();
      if (dismissed.has(wid)) {
        // Workflow was explicitly cleared by the user. Only re-admit when the user
        // visibly starts a new run (state hits Running) — otherwise stay hidden.
        if (state !== ExecutionState.Running) return;
        dismissed.delete(wid);
        this.saveDismissedSessionWorkflows(dismissed);
      }
    }

    const workflows = [...this.sessionWorkflowsSubject.value];
    // Match by wid (the stable identifier) — name can change via rename and shouldn't
    // create a duplicate session entry. Fall back to name-match only for unsaved workflows.
    const existingIndex =
      wid !== undefined
        ? workflows.findIndex(w => w.wid === wid)
        : workflows.findIndex(w => w.wid === undefined && w.name === name);
    if (existingIndex >= 0) {
      // Overwrite name too so renames are reflected in the Workflows tab.
      workflows[existingIndex] = { wid, name, state, timestamp: Date.now() };
    } else {
      workflows.unshift({ wid, name, state, timestamp: Date.now() });
    }
    const updated = workflows.slice(0, MAX_SESSION_WORKFLOWS);
    this.sessionWorkflowsSubject.next(updated);
    this.persistSessionWorkflows();
  }

  private persistSessionWorkflows(): void {
    try {
      localStorage.setItem(
        SESSION_WORKFLOWS_STORAGE_KEY,
        JSON.stringify(this.sessionWorkflowsSubject.value)
      );
    } catch {
      // Storage may be unavailable (private mode, quota); ignore.
    }
  }

  private static loadSessionWorkflows(): SessionWorkflow[] {
    try {
      const raw = localStorage.getItem(SESSION_WORKFLOWS_STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .filter(
          (w): w is SessionWorkflow =>
            typeof w === "object" &&
            w !== null &&
            typeof w.name === "string" &&
            typeof w.state === "string" &&
            typeof w.timestamp === "number"
        )
        .slice(0, MAX_SESSION_WORKFLOWS);
    } catch {
      return [];
    }
  }

  public handleKillWorkflow(): void {
    try {
      this.executeWorkflowService.killWorkflow();
    } catch (error) {
      console.error("Failed to kill workflow:", error);
    }
  }

  public handleRetryWorkflow(wid: number): void {
    if (wid === undefined || wid < 0) {
      return;
    }
    // Navigate to the workflow in the editor and let the user re-run it
    this.router.navigate([DASHBOARD_USER_WORKSPACE, wid]);
    this.closePanel();
  }

  // ---------- Feature 4: admin pending users ----------

  private startAdminPolling(): void {
    this.adminPollSub = timer(0, ADMIN_POLL_MS)
      .pipe(
        switchMap(() =>
          this.adminUserService.getUserList().pipe(catchError(() => of([] as ReadonlyArray<User>)))
        ),
        untilDestroyed(this)
      )
      .subscribe(users => this.applyAdminSnapshot(users));
  }

  private applyAdminSnapshot(users: ReadonlyArray<User>): void {
    // Only INACTIVE requests the admin hasn't already viewed (persisted in DB) count
    // as fresh. This survives page reloads, browser switches, and offline periods.
    const pendingUnseen = users.filter(u => u.role === Role.INACTIVE && !u.requestViewed);
    const stillPending = new Set(pendingUnseen.map(u => u.uid));

    // Auto-clean stale admin notifications: anyone in our notification list whose
    // user is no longer pending+unseen (approved, deleted, or already viewed by
    // another admin) should have their notification removed.
    this.agentService.removeWhere(n => {
      if (n.category !== "admin") return false;
      const uid = (n.meta as { uid?: number } | undefined)?.uid;
      return typeof uid === "number" && !stillPending.has(uid);
    });

    for (const user of pendingUnseen) {
      if (!this.adminNotifiedThisSession.has(user.uid)) {
        this.agentService.push({
          category: "admin",
          level: "warning",
          type: "adminRequests",
          title: `Approval needed: ${user.name}`,
          message: this.buildAdminMessage(user),
          action: { label: "Review user", route: [DASHBOARD_ADMIN_USER] },
          meta: { uid: user.uid, email: user.email },
        });
        this.adminNotifiedThisSession.add(user.uid);
      }
    }

    // Drop in-session tracking for users no longer pending so a re-INACTIVE flip would
    // notify again next poll.
    for (const uid of [...this.adminNotifiedThisSession]) {
      if (!stillPending.has(uid)) {
        this.adminNotifiedThisSession.delete(uid);
      }
    }
  }

  private buildAdminMessage(user: User): string {
    const parts = [user.email];
    if (user.joiningReason && user.joiningReason.trim().length > 0) {
      parts.push(`Reason: ${user.joiningReason.trim()}`);
    }
    return parts.join(" — ");
  }
}

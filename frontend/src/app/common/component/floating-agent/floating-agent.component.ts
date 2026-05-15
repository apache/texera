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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { CommonModule, DatePipe } from "@angular/common";
import { Router } from "@angular/router";
import { CdkDrag, CdkDragEnd, CdkDragHandle } from "@angular/cdk/drag-drop";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Observable, Subscription, BehaviorSubject, combineLatest, of, timer } from "rxjs";
import { catchError, map, switchMap, startWith } from "rxjs/operators";
import { FormsModule } from "@angular/forms";
import { NzBadgeModule } from "ng-zorro-antd/badge";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzSwitchModule } from "ng-zorro-antd/switch";
import { NzDividerModule } from "ng-zorro-antd/divider";

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
import { DASHBOARD_USER_DATASET, DASHBOARD_USER_WORKSPACE } from "../../../app-routing.constant";
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
  ],
  providers: [DatePipe],
})
export class FloatingAgentComponent implements OnInit, OnDestroy {
  public isOpen = false;
  public isAdmin = false;
  public isLoggedIn = false;
  public isSettingsOpen = false;
  public dragPosition: { x: number; y: number } = this.loadPosition();
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

  private readonly sessionWorkflowsSubject = new BehaviorSubject<SessionWorkflow[]>([]);
  public readonly sessionWorkflows$ = this.sessionWorkflowsSubject.asObservable();

  public readonly notifications$ = combineLatest([this.runs$, this.social$]).pipe(
    map(([runs, social]) => [...runs, ...social])
  );

  public readonly unreadNotifications$ = combineLatest([this.unreadRun$, this.unreadSocial$]).pipe(
    map(([runCount, socialCount]) => runCount + socialCount)
  );

  /** Baseline counts captured after the first poll so we only notify on increases. */
  private socialBaseline: Map<string, number> = new Map();
  private adminSeenInactive: Set<number> = new Set();
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
    private router: Router
  ) {}

  ngOnInit(): void {
    this.executionSnapshot = this.loadExecutionSnapshot();
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(user => this.onUserChanged(user));
    this.subscribeRunEvents();
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
      this.agentService.markAllRead();
    }
  }

  public closePanel(): void {
    this.isOpen = false;
    this.isSettingsOpen = false;
  }

  public toggleSettings(event?: Event): void {
    event?.stopPropagation();
    this.isSettingsOpen = !this.isSettingsOpen;
  }

  public updateSetting(key: keyof AgentNotificationSettings, value: boolean): void {
    this.agentService.updateSettings({ [key]: value });
  }

  public clearCategory(category: AgentNotificationCategory, event?: Event): void {
    event?.stopPropagation();
    this.agentService.clear(category);
  }

  public clearAllNotifications(event?: Event): void {
    event?.stopPropagation();
    this.agentService.clear("run");
    this.agentService.clear("social");
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
      this.adminSeenInactive.clear();
      this.executionSnapshot = undefined;
      this.persistExecutionSnapshot();
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
    // On page reload, the websocket reconnects and the server replays the current state.
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

    const snapshot = this.executionSnapshot ?? {
      wid: this.workflowActionService.getWorkflowMetadata()?.wid,
      name: this.workflowActionService.getWorkflowMetadata()?.name,
    };
    const workflowName = snapshot.name && snapshot.name.length > 0 ? snapshot.name : "Workflow";

    // Track workflow in session
    this.trackSessionWorkflow(snapshot.wid, workflowName, current.state);

    switch (current.state) {
      case ExecutionState.Completed:
        this.agentService.push({
          category: "run",
          level: "success",
          type: "runSuccess",
          title: `${workflowName} finished`,
          message: "The workflow run completed successfully.",
          action: this.workflowAction(snapshot.wid, "Tap to see result"),
        });
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
      case ExecutionState.Failed:
        this.agentService.push({
          category: "run",
          level: "error",
          type: "runFailure",
          title: `${workflowName} failed`,
          message: this.summarizeFailure(current),
          hint: RUN_ERROR_HINTS[ExecutionState.Failed],
          action: { label: "Retry", route: ["__retry-workflow__", snapshot.wid] },
          meta: { action: "retry", wid: snapshot.wid },
        });
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
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
        this.executionSnapshot = undefined;
        this.persistExecutionSnapshot();
        return;
      default:
        return;
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
            meta: { entityType: row.entityType, entityId: row.entityId, action, delta: diff },
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
    const workflows = this.sessionWorkflowsSubject.value;
    const existingIndex = workflows.findIndex(w => w.wid === wid && w.name === name);
    if (existingIndex >= 0) {
      workflows[existingIndex] = { ...workflows[existingIndex], state, timestamp: Date.now() };
    } else {
      workflows.unshift({ wid, name, state, timestamp: Date.now() });
    }
    const updated = workflows.slice(0, MAX_SESSION_WORKFLOWS);
    this.sessionWorkflowsSubject.next(updated);
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
    const inactive = users.filter(u => u.role === Role.INACTIVE);
    const isFirstPoll = this.adminSeenInactive.size === 0;
    for (const user of inactive) {
      if (!this.adminSeenInactive.has(user.uid)) {
        if (!isFirstPoll) {
          this.agentService.push({
            category: "admin",
            level: "warning",
            type: "adminRequests",
            title: `Approval needed: ${user.name}`,
            message: this.buildAdminMessage(user),
            meta: { uid: user.uid, email: user.email },
          });
        }
        this.adminSeenInactive.add(user.uid);
      }
    }
    // Drop any users that have been approved/removed so future re-INACTIVE flips notify again.
    const stillInactive = new Set(inactive.map(u => u.uid));
    for (const uid of [...this.adminSeenInactive]) {
      if (!stillInactive.has(uid)) {
        this.adminSeenInactive.delete(uid);
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

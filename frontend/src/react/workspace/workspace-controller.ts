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
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
import type { OperatorMetadata } from "../../core/workspace/types/operator-schema.interface";
import type { User } from "../../core/common/type/user";
import { Role } from "../../core/common/type/user";
import { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import type { GuiConfig } from "../core/runtime-config";
import { ResourceApi, type ComputingUnit, type Warehouse } from "../resources/api";
import { WorkflowRepository, emptyWorkflow } from "../workflows/workflow-repository";
import { WorkflowSession } from "./workflow-session";
import { WorkflowRuntime } from "./workflow-runtime";
import { WorkflowSocket, type WorkflowSocketOptions } from "./workflow-socket";

export interface WorkspaceSnapshot {
  loading: boolean;
  error?: string;
  metadata?: OperatorMetadata;
  session?: WorkflowSession;
  runtime?: WorkflowRuntime;
  units: ComputingUnit[];
  warehouses: Warehouse[];
  selectedCuid?: number;
  selectedWarehouseId?: number;
}
export interface WorkspaceControllerOptions {
  workflowId: number;
  versionId?: number;
  engineVersion: string;
  getToken: () => string | null;
  autosaveMs?: number;
  pollIntervalMs?: number;
  createSharedModel?: (wid: number | undefined, user: User) => SharedModel;
  createSocket?: (options: WorkflowSocketOptions) => WorkflowSocket;
}

/** Owns the shared workflow and execution connection across canvas/form view changes. */
export class WorkspaceController {
  private snapshot: WorkspaceSnapshot = { loading: true, units: [], warehouses: [] };
  private readonly listeners = new Set<() => void>();
  private readonly repository: WorkflowRepository;
  private readonly resources: ResourceApi;
  private loading?: Promise<void>;
  private disposal?: Promise<void>;
  private disposed = false;
  private loadController?: AbortController;
  private pollController?: AbortController;
  private pollTimer?: ReturnType<typeof setInterval>;
  private stopSession?: () => void;
  private latest: { cuId?: number; whId?: number } = {};
  private initialUnitSelectionComplete = false;
  private manualWarehousePick = false;
  private warehouseEnabled = false;

  constructor(
    private readonly api: ApiClient,
    private readonly user: SessionUser,
    private readonly config: Readonly<GuiConfig>,
    private readonly options: WorkspaceControllerOptions
  ) {
    identifier(options.workflowId);
    identifier(user.uid);
    if (options.versionId !== undefined) identifier(options.versionId);
    if (
      options.pollIntervalMs !== undefined &&
      (!Number.isFinite(options.pollIntervalMs) || options.pollIntervalMs <= 0)
    )
      throw new Error("The workspace polling interval must be positive.");
    this.repository = new WorkflowRepository(api);
    this.resources = new ResourceApi(api, options.getToken);
  }

  get session(): WorkflowSession | undefined {
    return this.snapshot.session;
  }
  get runtime(): WorkflowRuntime | undefined {
    return this.snapshot.runtime;
  }
  getSnapshot = (): WorkspaceSnapshot => this.snapshot;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  load(): Promise<void> {
    if (this.disposed) return Promise.reject(new Error("The workspace is closed."));
    if (this.loading) return this.loading;
    if (this.session) return Promise.resolve();
    this.loading = this.initialize().finally(() => {
      this.loading = undefined;
    });
    return this.loading;
  }

  selectComputingUnit(cuid: number | undefined): void {
    this.open();
    if (cuid !== undefined) {
      identifier(cuid);
      if (!this.snapshot.units.some(unit => unit.computingUnit.cuid === cuid))
        throw new Error("The computing unit is not available.");
    }
    this.initialUnitSelectionComplete = true;
    this.replaceRuntime(cuid);
    try {
      if (cuid === undefined) localStorage.removeItem(this.storageKey());
      else localStorage.setItem(this.storageKey(), String(cuid));
    } catch {
      /* Remembering a selection is optional when browser storage is unavailable. */
    }
  }

  selectWarehouse(whid: number | undefined): void {
    this.open();
    if (whid !== undefined) {
      identifier(whid);
      if (!this.warehouseEnabled || !this.snapshot.warehouses.some(warehouse => warehouse.whid === whid))
        throw new Error("The warehouse is not available.");
    }
    this.manualWarehousePick = true;
    this.update({ selectedWarehouseId: whid });
  }

  /** Disconnect immediately, then let the final edited snapshot drain through the persist queue. */
  dispose(): Promise<void> {
    if (this.disposal) return this.disposal;
    this.disposed = true;
    this.loadController?.abort();
    this.pollController?.abort();
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.stopSession?.();
    this.runtime?.dispose();
    this.listeners.clear();
    const session = this.session;
    this.disposal = (async () => {
      try {
        // Let child view cleanup release replay ownership before draining the live draft.
        await Promise.resolve();
        if (session) {
          session.exitPreview();
          const state = session.getSnapshot();
          if (
            !state.workflow.readonly &&
            !state.autosaveSuspended &&
            (state.saveState === "unsaved" || state.saveState === "error" || state.saveState === "saving")
          )
            await session.save();
          await session.whenSavesDrained();
        }
        await this.repository.whenSavesDrained();
      } finally {
        session?.dispose();
      }
    })();
    return this.disposal;
  }

  private async initialize(): Promise<void> {
    const controller = new AbortController();
    this.loadController = controller;
    this.update({ loading: true, error: undefined });
    let shared: SharedModel | undefined;
    try {
      const [metadata, workflow] = await Promise.all([
        this.api.get<OperatorMetadata>("resources/operator-metadata", { signal: controller.signal }),
        this.options.versionId === undefined
          ? this.repository.get(this.options.workflowId, controller.signal)
          : this.repository.getVersion(this.options.workflowId, this.options.versionId),
      ]);
      if (this.disposed) return;
      if (!metadata || !Array.isArray(metadata.operators) || !Array.isArray(metadata.groups))
        throw new Error("Malformed operator metadata.");
      const historical = this.options.versionId !== undefined;
      const localUser: User = {
        ...this.user,
        role: Role[this.user.role],
        email: this.user.email ?? "",
        comment: this.user.comment ?? "",
        joiningReason: this.user.joiningReason ?? "",
      };
      const room = historical ? undefined : this.options.workflowId;
      shared =
        this.options.createSharedModel?.(room, localUser) ??
        new SharedModel(room, localUser, this.config.productionSharedEditingServer, { connect: !historical });
      if (historical) shared.wsProvider.disconnect();
      const session = new WorkflowSession(historical ? { ...workflow, readonly: true } : workflow, {
        shared,
        autosaveMs: this.options.autosaveMs,
        defaultSettings: emptyWorkflow(this.config).settings,
        save: value => this.repository.persist(value),
      });
      this.update({ metadata, session });
      this.stopSession = session.subscribe(() => this.update({ session }));
      const latest = await this.api
        .get<{
          cuId?: number;
          whId?: number;
        }>(`executions/${this.options.workflowId}/latest`, { signal: controller.signal })
        .catch(() => ({}));
      if (this.disposed) return;
      this.latest = latest ?? {};
      await this.refreshResources();
      if (this.disposed) return;
      // A disconnected runtime still supports compilation before a unit is available.
      if (!this.runtime) this.replaceRuntime(undefined);
      this.update({ loading: false });
      this.pollTimer = setInterval(() => {
        void this.refreshResources();
      }, this.options.pollIntervalMs ?? 2000);
    } catch (error) {
      if (!this.disposed) {
        if (!this.session) {
          shared?.undoManager.destroy();
          shared?.destroy();
        }
        this.update({ loading: false, error: message(error, "The workflow could not be loaded.") });
      }
    } finally {
      if (this.loadController === controller) this.loadController = undefined;
    }
  }

  private async refreshResources(): Promise<void> {
    if (this.disposed || this.pollController) return;
    const controller = new AbortController();
    this.pollController = controller;
    try {
      const [unitResult, warehouseResult] = await Promise.allSettled([
        this.resources.computingUnits(false, { signal: controller.signal }),
        this.config.warehouseEnabled
          ? this.resources.warehouseStatus({ signal: controller.signal })
          : Promise.resolve({ enabled: false, warehouses: [] }),
      ]);
      if (this.disposed) return;
      const errors: string[] = [];
      if (unitResult.status === "fulfilled") {
        this.update({ units: unitResult.value });
        const selected = this.snapshot.selectedCuid;
        if (selected !== undefined && !unitResult.value.some(unit => unit.computingUnit.cuid === selected))
          this.replaceRuntime(undefined);
        if (!this.initialUnitSelectionComplete && unitResult.value.length) this.selectInitialUnit();
      } else errors.push(message(unitResult.reason, "Computing units could not be loaded."));
      if (warehouseResult.status === "fulfilled") {
        this.warehouseEnabled = !!this.config.warehouseEnabled && warehouseResult.value.enabled;
        const warehouses = this.warehouseEnabled ? [...warehouseResult.value.warehouses] : [];
        const manual =
          this.manualWarehousePick &&
          (this.snapshot.selectedWarehouseId === undefined ||
            warehouses.some(warehouse => warehouse.whid === this.snapshot.selectedWarehouseId));
        const selectedWarehouseId =
          manual && this.warehouseEnabled
            ? this.snapshot.selectedWarehouseId
            : (warehouses.find(warehouse => warehouse.whid === this.latest.whId) ?? warehouses[0])?.whid;
        this.update({ warehouses, selectedWarehouseId });
      } else errors.push(message(warehouseResult.reason, "Warehouses could not be loaded."));
      this.update({ error: errors.length ? errors.join(" ") : undefined });
    } catch (error) {
      if (!this.disposed) this.update({ error: message(error, "Workspace resources could not be loaded.") });
    } finally {
      if (this.pollController === controller) this.pollController = undefined;
    }
  }

  private selectInitialUnit(): void {
    const units = this.snapshot.units;
    let remembered: number | undefined;
    try {
      const stored = localStorage.getItem(this.storageKey());
      const value = Number(stored);
      if (stored && Number.isSafeInteger(value) && value > 0) remembered = value;
      if (remembered !== undefined && !units.some(unit => unit.computingUnit.cuid === remembered)) {
        localStorage.removeItem(this.storageKey());
        remembered = undefined;
      }
    } catch {
      /* The last execution and running-unit fallback work without browser storage. */
    }
    const selected =
      units.find(unit => unit.computingUnit.cuid === remembered) ??
      units.find(unit => unit.computingUnit.cuid === this.latest.cuId) ??
      units.find(unit => unit.status === "Running" && unit.accessPrivilege === "WRITE");
    this.initialUnitSelectionComplete = true;
    if (selected) this.replaceRuntime(selected.computingUnit.cuid);
  }

  private replaceRuntime(cuid: number | undefined): void {
    const session = this.session;
    if (!session) throw new Error("The workflow has not been loaded.");
    if (this.runtime && this.snapshot.selectedCuid === cuid) return;
    this.runtime?.dispose();
    const socketOptions: WorkflowSocketOptions = {
      wid: this.options.workflowId,
      uid: this.user.uid,
      cuid,
      getToken: this.options.getToken,
    };
    const socket = this.options.createSocket?.(socketOptions) ?? new WorkflowSocket(socketOptions);
    const runtime = new WorkflowRuntime(session, socket, this.api, {
      engineVersion: this.options.engineVersion,
      config: this.config,
      getComputingUnit: () => {
        const unit = this.snapshot.units.find(value => value.computingUnit.cuid === this.snapshot.selectedCuid);
        return unit
          ? {
              cuid: unit.computingUnit.cuid,
              status: unit.status,
              isOwner: unit.isOwner,
              accessPrivilege: unit.accessPrivilege,
            }
          : undefined;
      },
      getWarehouseId: () => this.snapshot.selectedWarehouseId,
    });
    this.update({ selectedCuid: cuid, runtime });
    if (cuid !== undefined) socket.connect();
    void runtime.compile().catch(() => undefined);
  }

  private storageKey(): string {
    return `computing-unit-of-workflow-${this.options.workflowId}`;
  }
  private open(): void {
    if (this.disposed) throw new Error("The workspace is closed.");
  }
  private update(partial: Partial<WorkspaceSnapshot>): void {
    if (this.disposed) return;
    this.snapshot = { ...this.snapshot, ...partial };
    this.listeners.forEach(listener => listener());
  }
}

function identifier(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error("Invalid workspace identifier.");
}
function message(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

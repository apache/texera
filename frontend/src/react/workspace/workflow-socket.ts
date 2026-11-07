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

import type {
  TexeraWebsocketEvent,
  TexeraWebsocketEventTypeMap,
  TexeraWebsocketEventTypes,
  TexeraWebsocketRequestTypeMap,
  TexeraWebsocketRequestTypes,
} from "../../app/workspace/types/workflow-websocket.interface";

interface SocketConnection {
  readonly readyState: number;
  onopen: (() => void) | null;
  onmessage: ((event: { data: string }) => void) | null;
  onerror: (() => void) | null;
  onclose: (() => void) | null;
  send(value: string): void;
  close(): void;
}
export interface WorkflowSocketOptions {
  wid: number;
  uid: number;
  cuid?: number;
  baseUrl?: string;
  getToken: () => string | null;
  createSocket?: (url: string) => SocketConnection;
}
export interface ConnectionSnapshot {
  status: "connecting" | "connected" | "disconnected" | "refused";
  numWorkers: number;
  error?: string;
}

/** One execution connection per mounted workflow session, never one per compile. */
export class WorkflowSocket {
  private connection?: SocketConnection;
  private heartbeat?: ReturnType<typeof setInterval>;
  private reconnect?: ReturnType<typeof setTimeout>;
  private closed = true;
  private generation = 0;
  private snapshot: ConnectionSnapshot = { status: "disconnected", numWorkers: -1 };
  private readonly listeners = new Set<() => void>();
  private readonly events = new Set<(event: TexeraWebsocketEvent) => void>();
  private readonly pending = new Set<(reason: string) => void>();

  constructor(private readonly options: WorkflowSocketOptions) {
    for (const id of [options.wid, options.uid, options.cuid])
      if (id !== undefined && (!Number.isSafeInteger(id) || id < 0))
        throw new Error("Invalid workflow connection identifier.");
  }
  getSnapshot = (): ConnectionSnapshot => this.snapshot;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  subscribeEvents = (listener: (event: TexeraWebsocketEvent) => void): (() => void) => {
    this.events.add(listener);
    return () => {
      this.events.delete(listener);
    };
  };

  connect(): void {
    this.close();
    this.closed = false;
    this.dial();
    this.heartbeat = setInterval(() => {
      if (this.connection?.readyState === 1) this.send("HeartBeatRequest", {});
    }, 10000);
  }

  send<T extends TexeraWebsocketRequestTypes>(type: T, payload: TexeraWebsocketRequestTypeMap[T]): void {
    if (this.closed || this.connection?.readyState !== 1)
      throw new Error("The workflow is not connected to a computing unit.");
    this.connection.send(JSON.stringify({ ...payload, type }));
  }

  request<T extends TexeraWebsocketRequestTypes, R extends TexeraWebsocketEventTypes>(
    type: T,
    payload: TexeraWebsocketRequestTypeMap[T],
    responseType: R,
    requestID: string,
    timeoutMs = 20000
  ): Promise<{ type: R } & TexeraWebsocketEventTypeMap[R]> {
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timeout);
        stop();
        this.pending.delete(fail);
      };
      const fail = (reason: string) => {
        cleanup();
        reject(new Error(reason));
      };
      const stop = this.subscribeEvents(event => {
        if (event.type === responseType && "requestID" in event && event.requestID === requestID) {
          cleanup();
          resolve(event as unknown as { type: R } & TexeraWebsocketEventTypeMap[R]);
        }
      });
      const timeout = setTimeout(() => fail("The workflow result request timed out."), timeoutMs);
      this.pending.add(fail);
      try {
        this.send(type, payload);
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  }

  close(): void {
    this.closed = true;
    ++this.generation;
    if (this.heartbeat) clearInterval(this.heartbeat);
    if (this.reconnect) clearTimeout(this.reconnect);
    this.heartbeat = undefined;
    this.reconnect = undefined;
    if (this.connection) {
      this.connection.onopen = this.connection.onmessage = this.connection.onerror = this.connection.onclose = null;
      this.connection.close();
      this.connection = undefined;
    }
    this.pending.forEach(fail => fail("The workflow connection was closed."));
    this.update({ status: "disconnected", numWorkers: -1 });
  }

  private dial(): void {
    if (this.closed) return;
    const generation = ++this.generation;
    this.update({ ...this.snapshot, status: "connecting", error: undefined });
    const url = new URL("/wsapi/workflow-websocket", this.options.baseUrl ?? window.location.origin);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("wid", String(this.options.wid));
    url.searchParams.set("uid", String(this.options.uid));
    if (this.options.cuid !== undefined) url.searchParams.set("cuid", String(this.options.cuid));
    const token = this.options.getToken();
    if (token) url.searchParams.set("access-token", token);
    let connection: SocketConnection;
    try {
      connection = this.options.createSocket?.(url.toString()) ?? (new WebSocket(url) as unknown as SocketConnection);
    } catch {
      this.scheduleReconnect(generation);
      return;
    }
    this.connection = connection;
    let received = false;
    let failed = false;
    let disconnected = false;
    connection.onopen = () => {
      if (generation === this.generation && !this.closed) this.send("HeartBeatRequest", {});
    };
    connection.onmessage = message => {
      if (generation !== this.generation || this.closed) return;
      let event: TexeraWebsocketEvent;
      try {
        const decoded: unknown = JSON.parse(message.data);
        if (!decoded || typeof decoded !== "object" || !("type" in decoded) || typeof decoded.type !== "string")
          throw new Error();
        event = decoded as TexeraWebsocketEvent;
      } catch {
        this.update({ ...this.snapshot, error: "Malformed workflow message received." });
        return;
      }
      received = true;
      this.update({
        status: "connected",
        numWorkers: event.type === "ClusterStatusUpdateEvent" ? event.numWorkers : this.snapshot.numWorkers,
      });
      this.events.forEach(listener => listener(event));
    };
    connection.onerror = () => {
      failed = true;
    };
    connection.onclose = () => {
      if (disconnected || generation !== this.generation || this.closed) return;
      disconnected = true;
      connection.onmessage = connection.onopen = connection.onerror = connection.onclose = null;
      if (this.connection === connection) this.connection = undefined;
      this.pending.forEach(fail => fail("The workflow connection was interrupted."));
      if (received || failed) this.scheduleReconnect(generation);
      else
        this.update({
          status: "refused",
          numWorkers: -1,
          error: "The computing unit refused the workflow connection.",
        });
    };
  }
  private scheduleReconnect(generation: number): void {
    if (generation !== this.generation || this.closed) return;
    this.update({ ...this.snapshot, status: "disconnected", error: "Workflow connection lost. Reconnecting…" });
    this.reconnect = setTimeout(() => {
      if (!this.closed && generation === this.generation) this.dial();
    }, 3000);
  }
  private update(snapshot: ConnectionSnapshot): void {
    this.snapshot = snapshot;
    this.listeners.forEach(listener => listener());
  }
}

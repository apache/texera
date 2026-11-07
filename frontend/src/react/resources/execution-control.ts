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
import { WorkflowSocket, type WorkflowSocketOptions } from "../workspace/workflow-socket";
import type { ExecutionControlRequest } from "./administration";

/** Administrative one-shot controls wait for authentication, send once, and release their socket. */
export class ExecutionControl {
  private closed = false;
  private readonly pending = new Set<() => void>();
  constructor(
    private readonly options: {
      uid: number;
      getToken: () => string | null;
      timeoutMs?: number;
      createSocket?: (options: WorkflowSocketOptions) => WorkflowSocket;
    }
  ) {}
  send(wid: number, request: ExecutionControlRequest): Promise<void> {
    if (this.closed) return Promise.reject(new Error("The execution controls are closed."));
    if (!Number.isSafeInteger(wid) || wid < 0) return Promise.reject(new Error("Invalid workflow identifier."));
    if (!["WorkflowPauseRequest", "WorkflowResumeRequest", "WorkflowKillRequest"].includes(request))
      return Promise.reject(new Error("Invalid execution control request."));
    return new Promise((resolve, reject) => {
      const socketOptions: WorkflowSocketOptions = { wid, uid: this.options.uid, getToken: this.options.getToken };
      const socket = this.options.createSocket?.(socketOptions) ?? new WorkflowSocket(socketOptions);
      let settled = false;
      let stop = () => {};
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        stop();
        this.pending.delete(cancel);
        socket.close();
        if (error) reject(error);
        else resolve();
      };
      const cancel = () => finish(new Error("The execution controls were closed."));
      const timer = setTimeout(
        () => finish(new Error("The workflow control connection timed out.")),
        this.options.timeoutMs ?? 20000
      );
      this.pending.add(cancel);
      stop = socket.subscribe(() => {
        const state = socket.getSnapshot();
        if (state.status === "refused") finish(new Error(state.error ?? "The workflow connection was refused."));
        if (state.status !== "connected") return;
        try {
          socket.send(request, {});
          finish();
        } catch (error) {
          finish(error instanceof Error ? error : new Error("The execution control failed."));
        }
      });
      socket.connect();
    });
  }
  dispose(): void {
    this.closed = true;
    this.pending.forEach(cancel => cancel());
    this.pending.clear();
  }
}

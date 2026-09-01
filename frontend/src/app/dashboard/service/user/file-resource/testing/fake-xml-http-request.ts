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
/**
 * Stand-in for XMLHttpRequest, which the multipart engine uses directly for per-part upload
 * progress. Install with `vi.stubGlobal("XMLHttpRequest", FakeXMLHttpRequest)`.
 */
export class FakeXMLHttpRequest {
  static instances: FakeXMLHttpRequest[] = [];

  /** Capturing upload target so tests can drive `upload.progress` events. */
  readonly upload = {
    listeners: new Map<string, EventListener[]>(),
    addEventListener(type: string, listener: EventListener): void {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    },
  };
  status = 0;
  url = "";
  aborted = false;
  readonly requestHeaders = new Map<string, string>();
  private listeners = new Map<string, EventListener[]>();

  static reset(): void {
    FakeXMLHttpRequest.instances = [];
  }

  open(_method: string, url: string): void {
    this.url = url;
  }

  setRequestHeader(name: string, value: string): void {
    this.requestHeaders.set(name, value);
  }

  send(): void {
    FakeXMLHttpRequest.instances.push(this);
  }

  abort(): void {
    this.aborted = true;
  }

  addEventListener(type: string, listener: EventListener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  /** Drives the `upload.progress` listener registered by the engine. */
  emitProgress(loaded: number, lengthComputable = true): void {
    const event = { lengthComputable, loaded } as unknown as Event;
    for (const listener of this.upload.listeners.get("progress") ?? []) {
      listener(event);
    }
  }

  respond(status: number): void {
    this.status = status;
    this.emit("load");
  }

  fail(): void {
    this.emit("error");
  }

  /** Query params of the URL this request was opened with. */
  params(): URLSearchParams {
    return new URL(this.url, "http://localhost").searchParams;
  }

  private emit(type: string): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(new Event(type));
    }
  }
}

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

import { TestBed } from "@angular/core/testing";
import { WorkflowWebsocketService } from "./workflow-websocket.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

/** Browser-like WebSocket test double used to verify websocket reopen and subscription cleanup behavior. */
class FakeWebSocket extends EventTarget {
  public static readonly CONNECTING = 0;
  public static readonly OPEN = 1;
  public static readonly CLOSING = 2;
  public static readonly CLOSED = 3;

  public readyState = FakeWebSocket.CONNECTING;

  constructor(public readonly url: string) {
    super();
    Promise.resolve().then(() => {
      this.readyState = FakeWebSocket.OPEN;
      const onopen = this.onopen;
      onopen?.(new Event("open"));
      this.dispatchEvent(new Event("open"));
    });
  }

  public onopen: ((ev: Event) => unknown) | null = null;
  public onclose: ((ev: CloseEvent) => unknown) | null = null;
  public onerror: ((ev: Event) => unknown) | null = null;
  public onmessage: ((ev: MessageEvent) => unknown) | null = null;

  public send() {}

  public close() {
    if (this.readyState === FakeWebSocket.CLOSED) {
      return;
    }
    this.readyState = FakeWebSocket.CLOSED;
    const closeEvent = new CloseEvent("close", { wasClean: true, code: 1000, reason: "" });
    const onclose = this.onclose;
    onclose?.(closeEvent);
    this.dispatchEvent(closeEvent);
  }
}

describe("WorkflowWebsocketService", () => {
  let service: WorkflowWebsocketService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowWebsocketService, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should close the previous status subscription when openWebsocket is called again", () => {
    const originalWebSocket = window.WebSocket;
    window.WebSocket = FakeWebSocket as unknown as typeof WebSocket;

    try {
      service.openWebsocket(1, 1, 1);
      const firstStatusSubscription = (service as any).statusUpdateSubscription;
      expect(firstStatusSubscription.closed).toBeFalse();

      service.openWebsocket(1, 1, 1);
      expect(firstStatusSubscription.closed).toBeTrue();

      const secondStatusSubscription = (service as any).statusUpdateSubscription;
      expect(secondStatusSubscription.closed).toBeFalse();

      service.closeWebsocket();
      expect(secondStatusSubscription.closed).toBeTrue();
    } finally {
      window.WebSocket = originalWebSocket;
    }
  });
});

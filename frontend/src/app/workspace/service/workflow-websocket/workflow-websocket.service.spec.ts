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

import { discardPeriodicTasks, fakeAsync, flushMicrotasks, TestBed, tick } from "@angular/core/testing";
import { Subscription } from "rxjs";
import {
  WorkflowWebsocketService,
  WS_HEARTBEAT_INTERVAL_MS,
  WS_RECONNECT_INTERVAL_MS,
} from "./workflow-websocket.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { AuthService } from "../../../common/service/user/auth.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";

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

  /** Frames written to the socket, so what the service actually put on the wire can be asserted. */
  public readonly sent: string[] = [];

  public send(frame: string) {
    this.sent.push(frame);
  }

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

/** Every socket the service constructs while `RecordingWebSocket` is installed, in creation order. */
const openedSockets: RecordingWebSocket[] = [];

class RecordingWebSocket extends FakeWebSocket {
  constructor(url: string) {
    super(url);
    openedSockets.push(this);
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
      expect(firstStatusSubscription.closed).toBe(false);

      service.openWebsocket(1, 1, 1);
      expect(firstStatusSubscription.closed).toBe(true);

      const secondStatusSubscription = (service as any).statusUpdateSubscription;
      expect(secondStatusSubscription.closed).toBe(false);

      service.closeWebsocket();
      expect(secondStatusSubscription.closed).toBe(true);
    } finally {
      window.WebSocket = originalWebSocket;
    }
  });

  it("should reset the cached worker count when the websocket is closed", () => {
    // numWorkers is populated from ClusterStatusUpdateEvent on the live connection;
    // once the socket is closed the count is stale and must reset.
    service.numWorkers = 5;
    service.closeWebsocket();
    expect(service.numWorkers).toBe(-1);
  });

  it("websocketEvent surfaces events pushed onto the response stream", () => {
    const received: unknown[] = [];
    const sub = service.websocketEvent().subscribe(event => received.push(event));

    const event = { type: "WorkflowStateEvent", state: "RUNNING" };
    (service as any).webSocketResponseSubject.next(event);
    sub.unsubscribe();

    expect(received).toEqual([event]);
  });

  it("getConnectionStatusStream reflects updateConnectionStatus transitions and guards duplicates", () => {
    const emissions: boolean[] = [];
    const sub = service.getConnectionStatusStream().subscribe(value => emissions.push(value));

    // BehaviorSubject seeds `false`; a repeated value is guarded and does not re-emit.
    (service as any).updateConnectionStatus(true);
    (service as any).updateConnectionStatus(true);
    (service as any).updateConnectionStatus(false);
    sub.unsubscribe();

    expect(emissions).toEqual([false, true, false]);
    expect(service.isConnected).toBe(false);
  });

  it("openWebsocket routes an incoming socket message to websocketEvent and marks the connection up", async () => {
    const originalWebSocket = window.WebSocket;
    const sockets: FakeWebSocket[] = [];
    class CapturingWebSocket extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    }
    window.WebSocket = CapturingWebSocket as unknown as typeof WebSocket;

    const subscriptions: Subscription[] = [];
    try {
      const events: unknown[] = [];
      subscriptions.push(service.websocketEvent().subscribe(event => events.push(event)));
      let connected: boolean | undefined;
      subscriptions.push(service.getConnectionStatusStream().subscribe(value => (connected = value)));

      service.openWebsocket(1, 1, 1);
      await Promise.resolve(); // let the fake socket transition to OPEN

      const socket = sockets[sockets.length - 1];
      const event = { type: "WorkflowStateEvent", state: "RUNNING" };
      socket.onmessage?.(new MessageEvent("message", { data: JSON.stringify(event) }));

      expect(events).toContainEqual(event);
      expect(connected).toBe(true);
    } finally {
      // The service's subjects never complete, so unsubscribe our observers
      // explicitly rather than leaving them attached past the test.
      subscriptions.forEach(subscription => subscription.unsubscribe());
      service.closeWebsocket();
      window.WebSocket = originalWebSocket;
    }
  });

  /**
   * The handshake URL is the only place the caller's identity reaches the server, and the
   * retry pipeline is what keeps a dropped connection from ending the session. Both were
   * previously unexercised: the suite above only ever opened a socket with every argument
   * supplied and never faulted one.
   */
  describe("handshake URL and reconnection", () => {
    let originalWebSocket: typeof WebSocket;

    beforeEach(() => {
      openedSockets.length = 0;
      originalWebSocket = window.WebSocket;
      // Without this the real jsdom WebSocket dials ws://localhost:3000 and the test would be
      // asserting on whatever happens to be listening on this machine.
      window.WebSocket = RecordingWebSocket as unknown as typeof WebSocket;
    });

    afterEach(() => {
      service.closeWebsocket();
      window.WebSocket = originalWebSocket;
      AuthService.removeAccessToken();
      vi.restoreAllMocks();
    });

    it("defaults the user id to 1 and says so when openWebsocket is called without one", () => {
      const logSpy = vi.spyOn(console, "log");

      service.openWebsocket(7);

      expect(logSpy).toHaveBeenCalledWith("uId is undefined, defaulting to uId = 1");
      // The substituted default has to reach the server, not just the log line. Anchored at the
      // endpoint rather than at `?` so a wrong TEXERA_WEBSOCKET_ENDPOINT is caught too; the host
      // is deliberately left out, since getWebsocketUrl derives it from document.baseURI.
      expect(openedSockets[0].url).toContain("/wsapi/workflow-websocket?wid=7&uid=1");
    });

    it("carries the computing-unit id in the handshake URL, and leaves the parameter out when none is given", () => {
      // 5 is distinct from both wid and uid, so a swapped slot cannot masquerade as the right value.
      service.openWebsocket(3, 9, 5);
      expect(openedSockets[0].url).toContain("/wsapi/workflow-websocket?wid=3&uid=9&cuid=5");

      // The other leg. NOTE: this records what the frontend does today, not a handshake the server
      // accepts — WorkflowWebsocketResource.myOnOpen reads `cuid` with no Option guard, and
      // admin-execution.component calls openWebsocket(wid) with no computing unit at all. If that
      // gap is closed, this half of the test is meant to change with it.
      service.openWebsocket(3, 9);
      expect(openedSockets[1].url).toContain("/wsapi/workflow-websocket?wid=3&uid=9");
      expect(openedSockets[1].url).not.toContain("cuid");
    });

    it("appends the stored access token to the handshake URL, and nothing when there is none", () => {
      AuthService.setAccessToken("tok-abc");
      service.openWebsocket(1, 1, 1);
      expect(openedSockets[0].url).toContain("&access-token=tok-abc");

      AuthService.removeAccessToken();
      service.openWebsocket(1, 1, 1);
      expect(openedSockets[1].url).not.toContain("access-token");
    });

    it("reports the drop, waits out the reconnect delay, then redials and resends a heartbeat", fakeAsync(() => {
      const logSpy = vi.spyOn(console, "log");
      const statuses: boolean[] = [];
      const statusSubscription = service.getConnectionStatusStream().subscribe(value => statuses.push(value));

      try {
        service.openWebsocket(1, 1, 1);
        flushMicrotasks(); // the fake socket reaches OPEN on a microtask

        // an inbound frame is what marks the connection up, so drive one through first —
        // otherwise the `false` below would be indistinguishable from the seeded state.
        openedSockets[0].onmessage?.(
          new MessageEvent("message", { data: JSON.stringify({ type: "WorkflowStateEvent", state: "RUNNING" }) })
        );
        expect(statuses).toEqual([false, true]);

        openedSockets[0].onerror?.(new Event("error"));

        // the drop is published immediately...
        expect(statuses).toEqual([false, true, false]);
        expect(logSpy).toHaveBeenCalledWith("websocket connection lost, reconnecting in 3 seconds");

        // ...but the redial is held back until the delay elapses
        tick(WS_RECONNECT_INTERVAL_MS - 1);
        expect(openedSockets.length).toBe(1);
        tick(1);
        flushMicrotasks();
        expect(openedSockets.length).toBe(2);

        // the heartbeat queued on reconnect is flushed to the new socket once it opens
        expect(openedSockets[1].sent).toContain(JSON.stringify({ type: "HeartBeatRequest" }));
      } finally {
        statusSubscription.unsubscribe();
        service.closeWebsocket();
      }
    }));

    it("merges the request payload into the frame it puts on the wire", async () => {
      service.openWebsocket(1, 1, 1);
      await Promise.resolve(); // the fake socket reaches OPEN on a microtask

      service.send("RetryRequest", { workers: ["worker-a", "worker-b"] });

      // The type alone is not enough: everything the caller passed has to survive into the frame.
      expect(openedSockets[0].sent).toEqual([
        JSON.stringify({ type: "RetryRequest", workers: ["worker-a", "worker-b"] }),
      ]);
    });

    it("keeps the connection alive by sending a heartbeat once every interval", fakeAsync(() => {
      // The keepalive timer is started by the constructor, so the service has to be built inside
      // the fake zone for `tick` to reach it — the instance injected in `beforeEach` was
      // constructed in the real zone and its interval is invisible here.
      const heartbeatService = new WorkflowWebsocketService(TestBed.inject(GuiConfigService));
      const heartbeats = () =>
        openedSockets[0].sent.filter(frame => frame === JSON.stringify({ type: "HeartBeatRequest" })).length;

      try {
        heartbeatService.openWebsocket(1, 1, 1);
        flushMicrotasks();

        // nothing before the interval elapses...
        tick(WS_HEARTBEAT_INTERVAL_MS - 1);
        expect(heartbeats()).toBe(0);

        // ...one on the tick, and it keeps repeating rather than firing once
        tick(1);
        expect(heartbeats()).toBe(1);
        tick(WS_HEARTBEAT_INTERVAL_MS);
        expect(heartbeats()).toBe(2);
      } finally {
        heartbeatService.closeWebsocket();
        // the constructor's interval is never unsubscribed, so drain it or fakeAsync fails the test
        discardPeriodicTasks();
      }
    }));

    it("adopts the worker count carried by a ClusterStatusUpdateEvent", async () => {
      service.openWebsocket(1, 1, 1);
      await Promise.resolve();

      // 7 is neither the -1 initializer nor the value any other test leaves behind.
      openedSockets[0].onmessage?.(
        new MessageEvent("message", { data: JSON.stringify({ type: "ClusterStatusUpdateEvent", numWorkers: 7 }) })
      );

      expect(service.numWorkers).toBe(7);
    });

    it("leaves the worker count alone for events that are not cluster status updates", async () => {
      service.openWebsocket(1, 1, 1);
      await Promise.resolve();
      service.numWorkers = 7;

      openedSockets[0].onmessage?.(
        new MessageEvent("message", { data: JSON.stringify({ type: "WorkflowStateEvent", numWorkers: 99 }) })
      );

      expect(service.numWorkers).toBe(7);
    });
  });
});

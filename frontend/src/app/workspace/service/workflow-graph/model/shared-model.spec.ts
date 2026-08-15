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

import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import { CoeditorState, User } from "../../../../common/type/user";

// The real provider opens a websocket and schedules reconnects, which would make this
// spec depend on the network and leak timers into later tests. This double keeps the
// same surface SharedModel uses (awareness, connection flags, disconnect) and records
// the room name so the room-suffix branch can be asserted.
const constructorCalls: { url: string; room: string }[] = [];

class FakeWebsocketProvider {
  awareness: Awareness;
  shouldConnect = true;
  wsconnected = true;
  disconnect = vi.fn();

  constructor(url: string, room: string, doc: Y.Doc) {
    constructorCalls.push({ url, room });
    this.awareness = new Awareness(doc);
  }
}

vi.mock("y-websocket", () => ({
  WebsocketProvider: class {
    constructor(...args: unknown[]) {
      return new FakeWebsocketProvider(args[0] as string, args[1] as string, args[2] as Y.Doc);
    }
  },
}));

// Imported after the mock so SharedModel picks up the double.
import { SharedModel } from "./shared-model";

const user: User = { uid: 7, name: "alice", email: "alice@test.com", role: "REGULAR" } as unknown as User;

describe("SharedModel", () => {
  let model: SharedModel | undefined;

  const build = (wid?: number, withUser?: User): SharedModel => {
    model = new SharedModel(wid, withUser);
    return model;
  };

  beforeEach(() => {
    constructorCalls.length = 0;
  });

  afterEach(() => {
    model?.destroy();
    model = undefined;
  });

  describe("room suffix", () => {
    it("uses the workflow id as the room name when one is given", () => {
      build(42);
      expect(constructorCalls).toHaveLength(1);
      expect(constructorCalls[0].room).toBe("42");
    });

    it("falls back to a random room name when there is no workflow id", () => {
      build(undefined);
      expect(constructorCalls[0].room).toMatch(/^[0-9a-f-]{36}$/);
    });
  });

  describe("local awareness", () => {
    it("publishes the local coeditor state when a user is provided", () => {
      const sharedModel = build(1, user);

      const state = sharedModel.awareness.getLocalState() as unknown as CoeditorState;
      expect(state.user).toEqual({ ...user, clientId: sharedModel.clientId });
      expect(state.isActive).toBe(true);
      expect(state.userCursor).toEqual({ x: 0, y: 0 });
    });

    it("publishes no coeditor state when constructed anonymously", () => {
      const sharedModel = build(1);
      // Awareness starts out with an empty local state; without a user nothing is added.
      expect(sharedModel.awareness.getLocalState()).toEqual({});
    });

    it("updateAwareness writes the field when a user is provided", () => {
      const sharedModel = build(1, user);

      sharedModel.updateAwareness("userCursor", { x: 5, y: 6 });

      expect((sharedModel.awareness.getLocalState() as unknown as CoeditorState).userCursor).toEqual({ x: 5, y: 6 });
    });

    it("updateAwareness is a no-op when constructed anonymously", () => {
      const sharedModel = build(1);

      sharedModel.updateAwareness("userCursor", { x: 5, y: 6 });

      expect(sharedModel.awareness.getLocalState()).toEqual({});
    });
  });

  it("transact runs the callback inside a yDoc transaction", () => {
    const sharedModel = build(1, user);
    let ranInsideTransaction = false;

    sharedModel.transact(() => {
      sharedModel.operatorIDMap.set("op-1", undefined as never);
      ranInsideTransaction = true;
    });

    expect(ranInsideTransaction).toBe(true);
    expect(sharedModel.operatorIDMap.has("op-1")).toBe(true);
  });

  describe("destroy", () => {
    // `destroy` only disconnects when the provider both wants to connect and is connected.
    const cases: { shouldConnect: boolean; wsconnected: boolean; disconnects: boolean }[] = [
      { shouldConnect: true, wsconnected: true, disconnects: true },
      { shouldConnect: false, wsconnected: true, disconnects: false },
      { shouldConnect: true, wsconnected: false, disconnects: false },
    ];

    cases.forEach(({ shouldConnect, wsconnected, disconnects }) => {
      it(`${disconnects ? "disconnects" : "does not disconnect"} when shouldConnect=${shouldConnect} and wsconnected=${wsconnected}`, () => {
        const sharedModel = build(1, user);
        const provider = sharedModel.wsProvider as unknown as FakeWebsocketProvider;
        provider.shouldConnect = shouldConnect;
        provider.wsconnected = wsconnected;

        sharedModel.destroy();
        model = undefined; // already destroyed; keep afterEach from destroying twice

        expect(provider.disconnect).toHaveBeenCalledTimes(disconnects ? 1 : 0);
      });
    });
  });
});

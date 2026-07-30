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

import { getWebsocketUrl } from "./url";

/**
 * Overrides `document.baseURI` (an inherited, read-only accessor) with an own
 * property so each test can pin the page's base URL, then restores it.
 */
function setBaseURI(value: string): void {
  Object.defineProperty(document, "baseURI", {
    configurable: true,
    get: () => value,
  });
}

describe("getWebsocketUrl", () => {
  afterEach(() => {
    // Remove the own property so the inherited jsdom accessor is restored.
    delete (document as any).baseURI;
  });

  describe("when no explicit port is given", () => {
    it("rewrites an http base URI to the ws protocol", () => {
      setBaseURI("http://example.com/app/");
      expect(getWebsocketUrl("/api/websocket", "")).toBe("ws://example.com/api/websocket");
    });

    it("rewrites an https base URI to the wss protocol", () => {
      setBaseURI("https://example.com/app/");
      expect(getWebsocketUrl("/api/websocket", "")).toBe("wss://example.com/api/websocket");
    });

    it("resolves a relative endpoint against the base URI path", () => {
      setBaseURI("http://example.com/app/");
      expect(getWebsocketUrl("sub/path", "")).toBe("ws://example.com/app/sub/path");
    });

    it("preserves a non-default port already present in the base URI", () => {
      setBaseURI("http://example.com:9000/app/");
      expect(getWebsocketUrl("/api/websocket", "")).toBe("ws://example.com:9000/api/websocket");
    });
  });

  describe("when an explicit port is given", () => {
    it("targets the base URI hostname on that port", () => {
      setBaseURI("http://example.com/app/");
      expect(getWebsocketUrl("/api/websocket", "8080")).toBe("ws://example.com:8080/api/websocket");
    });

    it("always uses the unencrypted ws protocol regardless of the base scheme", () => {
      // The port branch hard-codes an http:// origin, so even an https page
      // yields ws:// (not wss://) on the explicit port.
      setBaseURI("https://secure.example.com/app/");
      expect(getWebsocketUrl("/api/websocket", "8080")).toBe("ws://secure.example.com:8080/api/websocket");
    });

    it("uses only the hostname of the base URI, dropping its original port", () => {
      setBaseURI("http://example.com:1234/app/");
      expect(getWebsocketUrl("/api/websocket", "5678")).toBe("ws://example.com:5678/api/websocket");
    });
  });
});

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

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReactPreview } from "./app";
import { ApiClient } from "./core/api-client";
import { AuthStore } from "./core/auth";
import { RuntimeConfig } from "./core/runtime-config";

afterEach(cleanup);

describe("React migration preview bootstrap", () => {
  it("shows the configured sign-in form after public config loads", async () => {
    const fetch = vi.fn().mockImplementation(async () => Response.json({ localLogin: true }));
    const api = new ApiClient({ fetch });
    const auth = new AuthStore(api, new RuntimeConfig(api));
    render(<ReactPreview auth={auth} />);
    await screen.findByLabelText("Username");
    expect(fetch).toHaveBeenCalledTimes(1);
    auth.dispose();
  });

  it("exposes a retry when config cannot load, then recovers without reloading", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response("Configuration unavailable", { status: 503 }))
      .mockImplementation(async () => Response.json({ localLogin: true }));
    const api = new ApiClient({ fetch });
    const auth = new AuthStore(api, new RuntimeConfig(api));
    render(<ReactPreview auth={auth} />);
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Configuration unavailable"));
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await screen.findByLabelText("Username");
    expect(fetch).toHaveBeenCalledTimes(2);
    auth.dispose();
  });
});

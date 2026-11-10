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

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { GoogleButton } from "./google-button";
import type { SocialAuth } from "./social-auth";
afterEach(cleanup);
describe("Google sign-in button lifecycle", () => {
  it("mounts the real provider adapter and disposes it on unmount", async () => {
    const dispose = vi.fn(),
      mountGoogle = vi.fn(async () => dispose);
    const view = render(<GoogleButton social={{ mountGoogle } as unknown as SocialAuth} />);
    await waitFor(() => expect(mountGoogle).toHaveBeenCalledOnce());
    view.unmount();
    expect(dispose).toHaveBeenCalledOnce();
  });
  it("reports SDK failure as text and disposes a provider that resolves after unmount", async () => {
    const failed = vi.fn(async () => {
      throw new Error("Provider unavailable <script>");
    });
    const view = render(<GoogleButton social={{ mountGoogle: failed } as unknown as SocialAuth} />);
    await screen.findByText("Provider unavailable <script>");
    expect(document.querySelector("script")).toBeNull();
    view.unmount();
    let resolve!: (dispose: () => void) => void;
    const dispose = vi.fn();
    const mounting = vi.fn(
      () =>
        new Promise<() => void>(done => {
          resolve = done;
        })
    );
    const next = render(<GoogleButton social={{ mountGoogle: mounting } as unknown as SocialAuth} />);
    next.unmount();
    resolve(dispose);
    await waitFor(() => expect(dispose).toHaveBeenCalledOnce());
  });
});

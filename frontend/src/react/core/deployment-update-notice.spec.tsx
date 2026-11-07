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

import { afterEach, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { DeploymentUpdateNotice } from "./deployment-update-notice";
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});
it("gates polling by config, shows a sticky dismissible update and reloads at most once", async () => {
  vi.useFakeTimers();
  const fetch = vi.fn(async () => Response.json({ buildNumber: "new" })),
    reload = vi.fn();
  const { rerender, unmount } = render(
    <DeploymentUpdateNotice
      enabled={false}
      fetch={fetch}
      reload={reload}
      intervalMs={1000}
      buildNumber="dev"
    />
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1000);
  });
  expect(fetch).not.toHaveBeenCalled();
  rerender(
    <DeploymentUpdateNotice
      enabled
      fetch={fetch}
      reload={reload}
      intervalMs={1000}
      buildNumber="dev"
    />
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(1000);
  });
  expect(screen.getByRole("status").textContent).toContain("refresh");
  const button = screen.getByRole("button", { name: "New version available" });
  fireEvent.click(button);
  fireEvent.click(button);
  expect(reload).toHaveBeenCalledOnce();
  fireEvent.click(screen.getByRole("button", { name: "Dismiss version notification" }));
  expect(screen.queryByRole("status")).toBeNull();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(10000);
  });
  expect(fetch).toHaveBeenCalledOnce();
  unmount();
});

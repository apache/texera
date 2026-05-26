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

import { formatCount, formatRelativeTime, formatSpeed, formatTime } from "./format.util";

describe("formatSpeed", () => {
  it('returns "0.0 MB/s" for zero, negative, or undefined input', () => {
    expect(formatSpeed(0)).toBe("0.0 MB/s");
    expect(formatSpeed(-1)).toBe("0.0 MB/s");
    expect(formatSpeed(undefined)).toBe("0.0 MB/s");
  });

  it("converts bytes/s to MB/s with one decimal place", () => {
    // exactly 1 MiB/s
    expect(formatSpeed(1024 * 1024)).toBe("1.0 MB/s");
    // 2.5 MiB/s
    expect(formatSpeed(2.5 * 1024 * 1024)).toBe("2.5 MB/s");
  });

  it("handles sub-MB throughput by rounding to one decimal", () => {
    // 512 KiB/s ≈ 0.5 MB/s
    expect(formatSpeed(512 * 1024)).toBe("0.5 MB/s");
  });

  it("handles very large throughput without overflow", () => {
    const result = formatSpeed(10 * 1024 * 1024 * 1024); // 10 GiB/s
    expect(result).toBe("10240.0 MB/s");
  });
});

describe("formatTime", () => {
  it('returns "1s" for undefined, zero, or negative input', () => {
    expect(formatTime(undefined)).toBe("1s");
    expect(formatTime(0)).toBe("1s");
    expect(formatTime(-5)).toBe("1s");
  });

  it("formats sub-minute durations in seconds", () => {
    expect(formatTime(1)).toBe("1s");
    expect(formatTime(45)).toBe("45s");
    expect(formatTime(59)).toBe("59s");
  });

  it("rounds fractional seconds", () => {
    expect(formatTime(1.4)).toBe("1s");
    expect(formatTime(1.6)).toBe("2s");
  });

  it("formats durations under one hour as minutes with optional seconds", () => {
    expect(formatTime(60)).toBe("1m");
    expect(formatTime(90)).toBe("1m30s");
    expect(formatTime(125)).toBe("2m05s"); // seconds zero-padded
    expect(formatTime(3599)).toBe("59m59s");
  });

  it("formats durations of one hour or more as hours with optional minutes", () => {
    expect(formatTime(3600)).toBe("1h");
    expect(formatTime(3660)).toBe("1h1m");
    expect(formatTime(7200)).toBe("2h");
    expect(formatTime(7260)).toBe("2h1m");
    // residual seconds are dropped once we hit the hour bucket
    expect(formatTime(3600 + 59)).toBe("1h");
  });
});

describe("formatRelativeTime", () => {
  const NOW = new Date("2026-05-26T12:00:00Z").getTime();

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(NOW));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns "Unknown" when timestamp is undefined', () => {
    expect(formatRelativeTime(undefined)).toBe("Unknown");
  });

  it("formats sub-hour differences in minutes", () => {
    expect(formatRelativeTime(NOW - 5 * 60 * 1000)).toBe("5 minutes ago");
    expect(formatRelativeTime(NOW - 59 * 60 * 1000)).toBe("59 minutes ago");
    // boundary: just-now floors to 0
    expect(formatRelativeTime(NOW)).toBe("0 minutes ago");
  });

  it("formats sub-day differences in hours", () => {
    expect(formatRelativeTime(NOW - 60 * 60 * 1000)).toBe("1 hours ago");
    expect(formatRelativeTime(NOW - 23 * 60 * 60 * 1000)).toBe("23 hours ago");
  });

  it("formats sub-week differences in days", () => {
    expect(formatRelativeTime(NOW - 24 * 60 * 60 * 1000)).toBe("1 days ago");
    expect(formatRelativeTime(NOW - 6 * 24 * 60 * 60 * 1000)).toBe("6 days ago");
  });

  it("formats sub-month differences in weeks", () => {
    expect(formatRelativeTime(NOW - 7 * 24 * 60 * 60 * 1000)).toBe("1 weeks ago");
    expect(formatRelativeTime(NOW - 3 * 7 * 24 * 60 * 60 * 1000)).toBe("3 weeks ago");
  });

  it("falls back to a locale date string for differences beyond four weeks", () => {
    const oldTimestamp = NOW - 5 * 7 * 24 * 60 * 60 * 1000;
    const expected = new Date(oldTimestamp).toLocaleDateString();
    expect(formatRelativeTime(oldTimestamp)).toBe(expected);
  });
});

describe("formatCount", () => {
  it("renders counts under 1000 as plain integers", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(1)).toBe("1");
    expect(formatCount(999)).toBe("999");
  });

  it("abbreviates counts of 1000+ to one-decimal thousands", () => {
    expect(formatCount(1000)).toBe("1.0k");
    expect(formatCount(1500)).toBe("1.5k");
    expect(formatCount(12345)).toBe("12.3k");
    expect(formatCount(999999)).toBe("1000.0k");
  });
});

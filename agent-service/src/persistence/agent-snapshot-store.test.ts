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

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile, readdir } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { AgentSnapshotStore } from "./agent-snapshot-store";
import { OperatorResultSerializationMode } from "../types/agent";
import type { AgentSnapshot } from "../types/agent";

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "agent-snap-"));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

function snapshot(agentId: string): AgentSnapshot {
  return {
    version: 1,
    agentId,
    agentName: "Bob",
    modelType: "m",
    createdAt: "2024-01-01T00:00:00.000Z",
    head: "step-initial",
    stepCounter: 0,
    messageCounter: 0,
    settings: {
      disabledTools: [],
      maxOperatorResultCharLimit: 2000,
      maxOperatorResultCellCharLimit: 2000,
      operatorResultSerializationMode: OperatorResultSerializationMode.TSV,
      toolTimeoutMs: 240000,
      executionTimeoutMs: 240000,
      maxSteps: 100,
      allowedOperatorTypes: [],
    },
    steps: [],
    messageGroups: {},
    workflowContent: {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400 },
    },
  };
}

describe("AgentSnapshotStore - save/load", () => {
  test("save then load returns an equal snapshot", async () => {
    const store = new AgentSnapshotStore(dir);
    const snap = snapshot("agent-1");
    await store.save(snap);
    expect(await store.load("agent-1")).toEqual(snap);
  });

  test("load returns null for an unknown agent", async () => {
    const store = new AgentSnapshotStore(dir);
    expect(await store.load("missing")).toBeNull();
  });

  test("creates the target directory on first save", async () => {
    const nested = join(dir, "does", "not", "exist");
    const store = new AgentSnapshotStore(nested);
    await store.save(snapshot("agent-1"));
    expect(await store.load("agent-1")).not.toBeNull();
  });

  test("save overwrites a previous snapshot for the same agent", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.save(snapshot("agent-1"));
    const updated = { ...snapshot("agent-1"), stepCounter: 5 };
    await store.save(updated);
    expect((await store.load("agent-1"))?.stepCounter).toBe(5);
    expect(await readdir(dir)).toHaveLength(1); // not a second file
  });
});

describe("AgentSnapshotStore - loadAll", () => {
  test("returns every saved snapshot", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.save(snapshot("agent-1"));
    await store.save(snapshot("agent-2"));
    const all = await store.loadAll();
    expect(all.map(s => s.agentId).sort()).toEqual(["agent-1", "agent-2"]);
  });

  test("returns empty when the directory does not exist", async () => {
    const store = new AgentSnapshotStore(join(dir, "nope"));
    expect(await store.loadAll()).toEqual([]);
  });

  test("skips corrupt and unsupported files", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.save(snapshot("good"));
    await writeFile(join(dir, "broken.agent.json"), "{not json", "utf-8");
    await writeFile(join(dir, "wrongversion.agent.json"), JSON.stringify({ agentId: "x", version: 99 }), "utf-8");
    await writeFile(join(dir, "ignored.txt"), "ignore me", "utf-8");

    const all = await store.loadAll();
    expect(all.map(s => s.agentId)).toEqual(["good"]);
  });
});

describe("AgentSnapshotStore - remove", () => {
  test("removes a saved snapshot", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.save(snapshot("agent-1"));
    await store.remove("agent-1");
    expect(await store.load("agent-1")).toBeNull();
  });

  test("removing a missing agent does not throw", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.remove("missing");
    expect(await store.load("missing")).toBeNull();
  });
});

describe("AgentSnapshotStore - debounced scheduleSave", () => {
  test("coalesces rapid updates and flush writes the latest", async () => {
    const store = new AgentSnapshotStore(dir, { debounceMs: 1000 });
    store.scheduleSave({ ...snapshot("agent-1"), stepCounter: 1 });
    store.scheduleSave({ ...snapshot("agent-1"), stepCounter: 2 });
    store.scheduleSave({ ...snapshot("agent-1"), stepCounter: 3 });

    await store.flush();

    const loaded = await store.load("agent-1");
    expect(loaded?.stepCounter).toBe(3);
    expect(await readdir(dir)).toHaveLength(1);
  });

  test("flush with nothing pending is a no-op", async () => {
    const store = new AgentSnapshotStore(dir);
    await store.flush();
    expect(await store.loadAll()).toEqual([]);
  });
});

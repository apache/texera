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

import {
  ExecutionState,
  isNotInExecution,
  isWebDataUpdate,
  isWebPaginationUpdate,
  WebDataUpdate,
  WebPaginationUpdate,
} from "./execute-workflow.interface";

const paginationUpdate: WebPaginationUpdate = {
  mode: { type: "PaginationMode" },
  totalNumTuples: 10,
  dirtyPageIndices: [1, 2],
};

const snapshotUpdate: WebDataUpdate = {
  mode: { type: "SetSnapshotMode" },
  table: [{ a: 1 }],
};

const deltaUpdate: WebDataUpdate = {
  mode: { type: "SetDeltaMode" },
  table: [{ b: 2 }],
};

describe("isWebPaginationUpdate", () => {
  it("returns true for a pagination-mode update", () => {
    expect(isWebPaginationUpdate(paginationUpdate)).toBe(true);
  });

  it("returns false for snapshot- and delta-mode updates", () => {
    expect(isWebPaginationUpdate(snapshotUpdate)).toBe(false);
    expect(isWebPaginationUpdate(deltaUpdate)).toBe(false);
  });
});

describe("isWebDataUpdate", () => {
  it("returns true for snapshot- and delta-mode updates", () => {
    expect(isWebDataUpdate(snapshotUpdate)).toBe(true);
    expect(isWebDataUpdate(deltaUpdate)).toBe(true);
  });

  it("returns false for a pagination-mode update", () => {
    expect(isWebDataUpdate(paginationUpdate)).toBe(false);
  });
});

describe("isNotInExecution", () => {
  it("returns true for terminal or uninitialized states", () => {
    expect(isNotInExecution(ExecutionState.Uninitialized)).toBe(true);
    expect(isNotInExecution(ExecutionState.Failed)).toBe(true);
    expect(isNotInExecution(ExecutionState.Killed)).toBe(true);
    expect(isNotInExecution(ExecutionState.Completed)).toBe(true);
  });

  it("returns false for active execution states", () => {
    expect(isNotInExecution(ExecutionState.Initializing)).toBe(false);
    expect(isNotInExecution(ExecutionState.Running)).toBe(false);
    expect(isNotInExecution(ExecutionState.Pausing)).toBe(false);
    expect(isNotInExecution(ExecutionState.Paused)).toBe(false);
    expect(isNotInExecution(ExecutionState.Resuming)).toBe(false);
    expect(isNotInExecution(ExecutionState.Recovering)).toBe(false);
    expect(isNotInExecution(ExecutionState.Terminated)).toBe(false);
  });
});

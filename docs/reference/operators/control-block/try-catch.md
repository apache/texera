<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

---
title: "Try Catch"
description: "Run a subgraph as one attempt; on any failure, replay the same input through a fallback subgraph"
category: "Control Block"
operator_type: "TryCatch"
tags: [control-block]
---

[Home](../../) > [Control Block](../)

Try Catch guards a whole subgraph. Everything downstream of the **Try** port is
one *attempt*: if any operator in it fails at runtime, the attempt is abandoned
and the **same input** is replayed through the subgraph downstream of the
**Catch** port. Pair it with a [Finally](../finally/) operator to reconverge
both branches into a single result stream.

This is block-level, not per-row: one failing row abandons the whole attempt,
exactly as one thrown exception abandons a `try { ... }` block in a programming
language. It is a *fallback*, not a retry — the catch branch is a different
pipeline over the same data.

### Example

Fetch enrichment data from a remote API in the Try branch; if the API is down,
the Catch branch falls back to a local cached table. Both branches feed a
Finally, so everything after the frame is written once, from whichever branch
succeeded.

### Semantics

- **One attempt.** A failure anywhere in the Try subgraph — including in a
  branch that was succeeding — abandons the whole attempt.
- **Replay, not re-read.** The frame snapshots its input, so the Catch branch
  sees exactly the rows the Try branch was given, without re-running upstream
  operators.
- **The Catch branch runs only on failure.** On success it receives no rows and
  finishes immediately.
- **Unconnected Catch port = rethrow.** A Try Catch with nothing wired to Catch
  propagates the failure outward (to an enclosing frame, or to the execution) —
  it does not swallow it. To swallow deliberately, wire Catch to an operator
  that discards its input.
- **Nesting.** Frames may nest. A failure is handled by the innermost frame
  containing the failing operator. If that frame's *catch* branch also fails,
  the failure escalates to the next enclosing frame — the dataflow equivalent
  of rethrowing from a catch block.
- **Side effects are not rolled back.** Rows the failed attempt already wrote
  (to a result table, an external sink) remain written, just as statements that
  ran before a `throw` are not undone. Put side effects *after* a Finally if you
  need them to happen exactly once.

### What is caught

Caught: runtime errors raised by operator logic — a Python or Java UDF raising,
a bad column reference, a malformed value, a failing `open()` (e.g. wrong
database credentials).

**Not** caught: infrastructure failures — a worker process dying, a lost cluster
node, out-of-memory. Those end the execution, the same as before; a `kill -9`
does not run your `finally` block either. Recovering from infrastructure
failures is the job of fault-tolerance/replay, not of control flow.

### Wiring rules

The compiler rejects a workflow that breaks these:

- The Try and Catch subgraphs must be **disjoint** — no operator may be in both
  (they are separate blocks, like `try { }` and `catch { }`).
- If a Finally is present, its `From Try` input must come from the Try subgraph
  and its `From Catch` input from the Catch subgraph of the **same** Try Catch.
- If a Finally is present, the Catch port must be connected.

Data from outside the frame may be joined into either subgraph freely; the
frame only guards operators reachable from its own ports.

### Input Ports

| Port | Description |
|------|-------------|
| Data | The rows to guard |

### Output Ports

| Port | Description | Mode |
|------|-------------|------|
| Try | The attempt: connect the subgraph to guard | [Set Snapshot](../../output-modes/#set-snapshot) |
| Catch | The fallback: receives a replay of the input if the attempt fails | [Set Snapshot](../../output-modes/#set-snapshot) |

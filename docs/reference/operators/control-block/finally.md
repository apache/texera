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
title: "Finally"
description: "Emit the winning branch of a Try Catch frame: try results on Try Result when the attempt succeeds, catch results on Catch Result when it fails"
category: "Control Block"
operator_type: "Finally"
tags: [control-block]
---

[Home](../../) > [Control Block](../)

Finally is the reconvergence point of a [Try Catch](../try-catch/) frame. Wire
the tail of the Try subgraph into **From Try** and the tail of the Catch
subgraph into **From Catch**; Finally emits whichever branch actually ran, out
of the output port named after it — **Try Result** on success, **Catch
Result** on failure — so downstream can both consume the winner and observe
which side won.

It is the dataflow equivalent of a `try`/`catch` *expression*: the construct
evaluates to the try result on success and to the catch result on failure, and
the code after it does not care which.

### Semantics

- **All or nothing.** Downstream of Finally sees exactly one branch's complete
  output, never a mixture and never duplicates. Results are staged until the
  outcome is known, then released in one go.
- **The outcome is part of the output.** The winner's rows leave through
  **Try Result** when the attempt succeeded and through **Catch Result** when
  it failed; the other port completes empty. Connect one port to react to the
  outcome (e.g. alert only on recovery), or connect **both ports to the same
  downstream input** — a Union — to get "the winner, whichever it was".
- **Deterministic outcome.** `From Catch` is consumed only after `From Try`
  fully resolves, so the release decision never depends on timing.
- **Each result port carries its own branch's schema.** Rows never cross
  ports, so the branches need not agree — a try branch fetching web content
  can fall back to a catch branch producing plain lines. Unioning the two
  ports downstream requires compatible schemas, as with any Union.
- **On double failure** (the catch branch fails too) Finally emits nothing and
  the failure escalates to any enclosing frame.
- **Optional.** A Try Catch works without a Finally: use it when the branches
  end in their own sinks/result tables and there is nothing to reconverge.

### Placing side effects

Because Finally releases only the winning branch's complete output, operators
placed *after* it run exactly once, on data from exactly one attempt. That makes
it the right place for writes you do not want a failed attempt to have made.
Writes placed *inside* a branch are not rolled back if that branch fails.

### Input Ports

| Port | Description |
|------|-------------|
| From Try | Tail of the Try subgraph |
| From Catch | Tail of the Catch subgraph (consumed after From Try resolves) |

### Output Ports

| Port | Description | Mode |
|------|-------------|------|
| Try Result | The try branch's results, when the attempt succeeded (empty otherwise) | [Set Snapshot](../../output-modes/#set-snapshot) |
| Catch Result | The catch branch's results, when the attempt failed (empty otherwise) | [Set Snapshot](../../output-modes/#set-snapshot) |

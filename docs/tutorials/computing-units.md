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
title: "Computing Units"
weight: 15
---

A **computing unit** is the machine that runs your workflows. You pick one in the workflow editor's top bar before clicking **Run** ([how](../guide-for-how-to-use-texera/#run-a-workflow)). You can see all of yours under **Your Work → Compute**.

## Create one

Click **+ Computing Unit** in the top bar, or **Create Computing Unit** on the Compute page. What you see depends on how your Texera is set up:

- **Create Computing Unit** (hosted Texera): Texera starts a new machine for you. Choose:

  | Field | What to pick |
  |---|---|
  | **Select RAM Size** | Memory. Start with the smallest; pick more if a run fails on large data. |
  | **Select #CPU Core(s)** | More cores help only when operators run with several workers. |
  | **Select #GPU(s)** | Only for deep learning in Python. Shown only if GPUs are available. |
  | **Image** | The software installed on the unit. Keep **Default** unless told otherwise. Fixed for the unit's lifetime. |
  | **Advanced Settings** | Shared memory (raise it for PyTorch) and JVM heap. Leave the defaults unless an admin tells you otherwise. |

- **Connect to a Local Computing Unit** (Texera on your own computer): enter a name and keep the suggested address.

A new unit shows a gold dot while it starts. You can select it once the dot turns green.

## Status dots

| Dot | Meaning |
|---|---|
| Green | Running: ready to use |
| Gold | Starting up, or shutting down |
| Red | Unavailable. Pick or create another unit |

## Tips

- **Reuse one unit** for all your workflows. You don't need one per workflow.
- **A new unit usually won't fix an error in your workflow**, such as a mistake in Python code or an operator setting. Read the error first ([where to find errors](../guide-for-how-to-use-texera/#where-to-find-errors)). A different unit *can* help when the problem is the unit itself: not enough memory, CPU or GPU, or a missing Python package or software image.
- **Terminating a unit deletes the results stored on it.** Your workflows and datasets are kept. Run the workflow again to get results back.
- **Details**: click the eye icon next to a unit to see its CPU, memory and GPU limits. Local units have no limits and show `NaN`.
- **Python packages**: the **+** icon next to a unit manages its Python environments.

## Common messages

| Message | What to do |
|---|---|
| `You may only have N computing-unit(s) running at the same time` | Reuse one of your running units, or terminate one you no longer need (trash icon). |
| `Insufficient CPU / memory / GPU available in the server` | Ask for less RAM, fewer cores or GPUs, or try again later. |
| **Run** is disabled | You only have read access to the selected unit. Pick a unit you own. |

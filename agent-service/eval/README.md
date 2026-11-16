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

# Agent Evaluation

The agent's unit tests use a mock model, so they cannot tell whether a change makes the agent better or worse. This evaluation runs a fixed set of cases against a real model on a running Texera, and prints how many runs passed and what they cost.

## The Cases

| Case | What the user does | The run passes when |
| --- | --- | --- |
| `build-workflow` | Asks for the three categories with the highest total sales | The answer names them, in order |
| `fix-failing-operator` | Asks to fix a filter on a misspelled column | The saved filter uses `amount`, and the answer gives the right count |
| `explain-execution-error` | Asks why a Python operator fails | The answer names the order the error names |
| `docs-question` | Asks which output port modes an operator can have | The answer names the three modes in the docs |
| `user-edit-between-prompts` | Asks for two changes, and adds a Limit in between | The user's Limit is still in the saved workflow |
| `user-execution-failed` | Runs the workflow, then asks why it failed, without running anything | The answer names the order the error names, and the agent ran nothing |

The cases work on 240 orders that `orders.ts` makes the same way every time, and every expected answer is computed from those rows. The failing Python operator names the order it stops at, an id that is in the execution's error and nowhere in the workflow, so an answer that names it has read the execution.

## How To Run It

You need a running Texera and a model its LLM endpoint serves. The evaluation starts the agent service of this checkout on a free port, so it measures your working copy. It reads the variables the agent service reads (`src/config/env.ts`) and passes them on, and puts a recorder in front of `LLM_ENDPOINT` that counts every model call: its size in bytes and the tokens the provider reports.

With `bin/local-dev.sh` the defaults reach every service; name the model and, where warehouses are on, a warehouse:

```bash
cd agent-service
EVAL_MODEL=claude-haiku-4.5 EVAL_WAREHOUSE_ID=1 bun run eval
```

Behind a gateway such as `bin/single-node.sh`, send everything to the gateway except executions, which go to the computing unit:

```bash
TEXERA_DASHBOARD_SERVICE_ENDPOINT=http://localhost:8080 \
WORKFLOW_COMPILING_SERVICE_ENDPOINT=http://localhost:8080 \
LLM_ENDPOINT=http://localhost:8080 \
EVAL_FILE_SERVICE_ENDPOINT=http://localhost:8080 \
WORKFLOW_EXECUTION_SERVICE_ENDPOINT=http://localhost:8085 \
EVAL_MODEL=claude-haiku-4.5 EVAL_PASSWORD=... EVAL_WAREHOUSE_ID=1 \
bun run eval
```

`bin/single-node.sh` does not publish the computing unit's port 8085; publish it with a Compose override first.

| Variable | Default | Meaning |
| --- | --- | --- |
| `EVAL_MODEL` | (required) | The model, as `/api/models` lists it |
| `EVAL_USERNAME`, `EVAL_PASSWORD` | `texera`, `texera` | The user the cases run as |
| `EVAL_FILE_SERVICE_ENDPOINT` | `http://localhost:9092` | Where the orders dataset is made |
| `EVAL_COMPUTING_UNIT_ID` | `1` | A running computing unit of the user |
| `EVAL_WAREHOUSE_ID` | none | The warehouse executions write to, where warehouses are on |
| `EVAL_REPEAT` | `3` | Runs per case, since a real model does not answer the same way twice |
| `EVAL_CASES` | all | Comma-separated case ids |
| `EVAL_OUT` | none | A file for the full report as JSON |

Each run gets its own workflow and agent, and both are deleted when the run ends. The orders stay in a dataset named `agent-eval-orders`, which later runs reuse.

## What It Prints

One line per run, then a table with the costs averaged per run. This is `main` with Claude Haiku 5.5 on November 16, 2026:

```
Case                       Passed  Model calls  Input tokens  Output tokens  KB per call  Max KB
-------------------------  ------  -----------  ------------  -------------  -----------  ------
build-workflow                3/3          4.3        56,984          1,402         37.9    39.8
fix-failing-operator          3/3          2.7        34,732            914         37.7    39.3
explain-execution-error       3/3          2.0        26,013            854         37.8    38.6
docs-question                 0/3          1.0        12,205            839         35.8    35.8
user-edit-between-prompts     0/3          4.0        52,409          1,037         37.8    39.8
user-execution-failed         0/3          1.0        12,693            739         37.0    37.0
-------------------------  ------  -----------  ------------  -------------  -----------  ------
total                        9/18          2.5        32,506            964         37.6    39.8

Score: 9 of 18 runs passed (50%). Costs are per run, averaged.
```

## Adding A Case

A case is an entry of `CASES` in `cases.ts`: the workflow it starts from, its prompts, what the user does before or between them, and a check that reads the answers and the saved workflow. Compute what the check expects from the orders rather than writing it down, and make the check pass only on what the agent could not have guessed.

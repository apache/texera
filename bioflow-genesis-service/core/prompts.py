"""LLM-driven agent prompts for Texera workflows (native Sklearn + Python UDF only when needed)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_AGENT_MODEL = "claude-haiku-4.5"

# Built from Texera `LogicalOp` sklearn entries + Iris example charts. No KMeans OpDesc — use PythonUDFV2 for KMeans.
ALLOWED_OPERATOR_TYPES = [
    "CSVFileScan",
    "Projection",
    "Filter",
    "Split",
    "SklearnLogisticRegression",
    "SklearnLogisticRegressionCV",
    "SklearnDecisionTree",
    "SklearnLinearRegression",
    "SklearnRandomForest",
    "SklearnKNN",
    "SklearnPerceptron",
    "SklearnSVM",
    "SklearnGradientBoosting",
    "SklearnGaussianNaiveBayes",
    "SklearnPrediction",
    "Aggregate",
    "Sort",
    "Limit",
    "Distinct",
    "Scatterplot",
    "ScatterMatrixChart",
    "BarChart",
    "PieChart",
    "LineChart",
    "WordCloud",
    "PythonUDFV2",
]


def _iris_example_text() -> str:
    path = Path(__file__).resolve().parent / "iris_ml_example.json"
    return path.read_text(encoding="utf-8")


IRIS_EXAMPLE_WORKFLOW = _iris_example_text()


def _iris_single_branch_template() -> str:
    """One valid Iris subgraph: CSV → Projection → 2× Split → Sklearn trainer → SklearnPrediction → Scatterplot."""
    wf = json.loads(_iris_example_text())
    ids = frozenset(
        {
            "CSVFileScan-operator-e6f7be3d-492b-4cd1-8d74-dafa9fab94e9",
            "Projection-operator-f333f5a9-2894-428b-a51b-f90fa4b5d2de",
            "Split-operator-fb8bfb7e-a5af-4b25-8579-de02ae834f9f",
            "Split-operator-ad9de8f5-6a3f-4737-8870-7f9dbb75c996",
            "SklearnPerceptron-operator-a7b059fb-1e65-4b80-8d26-7c9aa64b4b1e",
            "SklearnPrediction-operator-a6479757-22d1-4138-ade3-4a09675633fa",
            "Scatterplot-operator-d3688238-2bd8-4aec-bec8-c5b0c9c5c566",
        }
    )
    ops = [o for o in wf["operators"] if o["operatorID"] in ids]
    links = [
        L
        for L in wf["links"]
        if L["source"]["operatorID"] in ids and L["target"]["operatorID"] in ids
    ]
    positions = {k: v for k, v in wf["operatorPositions"].items() if k in ids}
    mini: dict[str, Any] = {
        "operators": ops,
        "operatorPositions": positions,
        "links": links,
        "commentBoxes": [],
        "settings": wf.get("settings", {"dataTransferBatchSize": 400}),
    }
    return json.dumps(mini, indent=2)


IRIS_SINGLE_BRANCH_TEMPLATE = _iris_single_branch_template()

# Verbatim link pattern from `iris_ml_example.json` (SklearnPrediction + trainer + Split).
# Frontend / AJV expects this link shape — NOT fromPortId/toPortId, NOT numeric ports.
IRIS_SKLEARN_PREDICTION_LINK_EXAMPLES = """
=== GROUND-TRUTH LINK JSON (Iris discipline — match this **shape**; invent your own operatorIDs / linkIDs) ===

Texera workflow `links` are objects with **`linkID`**, **`source`**, **`target`** only.
Each of `source` and `target` is **`{"operatorID": "<exact-id-from-operators>", "portID": "<string>"}`**.

**portID strings are ALWAYS** one of: `"input-0"`, `"input-1"`, `"output-0"`, `"output-1"`, etc.
Never use integers, never use `fromPortId` / `toPortId`, never omit the `input-` / `output-` prefix.

**Sklearn trainers** (`SklearnLogisticRegression`, `SklearnRandomForest`, …) have **two** inputs (see Iris):
- Connect **Split `output-0` (training)** → trainer **`input-0`**
- Connect **Split `output-1` (testing/holdout)** → trainer **`input-1`**
Then trainer **`output-0`** carries the fitted **`model`** tuple for SklearnPrediction.

=== EVERY SKLEARN TRAINER: TWO INPUT EDGES (NON-NEGOTIABLE) ===

**EVERY** `SklearnLogisticRegression`, `SklearnDecisionTree`, `SklearnRandomForest`, `SklearnLinearRegression`, and every other **Sklearn\\<Algorithm\\>** **trainer** MUST have **TWO** incoming edges from the **same** **Split** (train/test):

- **`inputOperatorIds["0"]`** (training) ← **Split `output-0`**
- **`inputOperatorIds["1"]`** (testing / holdout) ← **Split `output-1`**

**THIS IS NON-NEGOTIABLE.** If you have **3 trainers in parallel** (AutoML-style: LogReg + Tree + Forest), **ALL THREE** MUST list **both** `"0"` and `"1"` in **`inputOperatorIds`**. The **first** trainer in the plan is **not** exempt — LLMs often forget **`"1"`** on the first branch; **check it explicitly.**

Missing the **testing** edge (`input-1`) causes **Invalid Workflow** in Texera and **0 tuples** on the testing port.

**SklearnPrediction** (engine port order: `input-0` = **model**, `input-1` = **data rows to score**):

[
  {
    "linkID": "link-trainer-to-sklearnpred-model",
    "source": {
      "operatorID": "SklearnLogisticRegression-operator-REPLACE-WITH-YOUR-UUID",
      "portID": "output-0"
    },
    "target": {
      "operatorID": "SklearnPrediction-operator-REPLACE-WITH-YOUR-UUID",
      "portID": "input-0"
    }
  },
  {
    "linkID": "link-split-test-to-sklearnpred-data",
    "source": {
      "operatorID": "Split-operator-REPLACE-WITH-YOUR-UUID",
      "portID": "output-1"
    },
    "target": {
      "operatorID": "SklearnPrediction-operator-REPLACE-WITH-YOUR-UUID",
      "portID": "input-1"
    }
  }
]

Swapping `input-0` / `input-1` on **SklearnPrediction** or connecting **data** to **`input-0`** produces **Invalid Workflow** in the UI.

The full Iris file above shows real UUIDs — copy its linking discipline exactly.
"""

# Visualization + AI Insights: Iris uses Scatterplot after SklearnPrediction; exact props from
# `bin/single-node/examples/workflows/[Example] Machine Learning on Iris Dataset.json`.
POST_IRIS_VIZ_AND_INSIGHTS_GUIDANCE = """
## VISUALIZATION — MANDATORY AFTER SklearnPrediction (Iris example)

The **Iris** machine-learning example workflow uses **`Scatterplot`** immediately downstream of **`SklearnPrediction`**.

**`Scatterplot`** — use this `operatorType` and **`operatorProperties` shape** (replace column names with **this dataset**'s attributes):
- **`xColumn`**, **`yColumn`** — numeric columns to plot.
- **`colorColumn`** — optional string; for **classification** after scoring, often **`prediction`** (or your **Output Attribute Name**) or the label column so the chart encodes outcome / predicted class.
- **`xLogScale`**, **`yLogScale`** — booleans (typically `false`).
- **`alpha`** — e.g. `1`.

**Classification:** Prefer a **Scatterplot** with two informative features on the axes and **`colorColumn`** set to **`prediction`** or the label to show separation / prediction quality; or use **`BarChart`** (`categoryColumn`, `fields`, `value`, `horizontalOrientation`) to summarize **prediction distribution** or class counts.

**Regression:** Use **`Scatterplot`** with **`xColumn` = the ground-truth target column** and **`yColumn` = `prediction`** (actual vs predicted; quality = tightness around the diagonal).

**Port wiring:** `Scatterplot` has **`input-0` only**. Connect **`SklearnPrediction` `output-0`** → **`Scatterplot` `input-0`**.

You **must** add **one** native visualization (**`Scatterplot`** or **`BarChart`**) in every supervised workflow **after** **`SklearnPrediction`** — **never stop at step 5.**

---

## AI INSIGHTS — `PythonUDFV2` code starting point (adapt; not copy-paste)

Add a final **`PythonUDFV2`** with **`customDisplayName`**: **"AI Insights"**.

**`operatorProperties`** must include valid Python UDF config: use **`process_table`**, and **`outputColumns`**: `[{"attributeName": "insight", "attributeType": "string"}]`.

Skeleton (**fill in** real label/prediction column names, model description, and accuracy logic — wrap in `try`/`except` per OPERATOR & LINK RULES §B):

```python
from pytexera import *
import pandas as pd

class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        try:
            df = pd.DataFrame(table)
            # Scored rows: label + prediction columns (use YOUR attribute names from Projection / SklearnPrediction).
            total = len(df)
            label_col = "<TARGET_COLUMN_NAME>"
            pred_col = "prediction"
            if label_col in df.columns and pred_col in df.columns:
                correct = int((df[label_col].astype(str) == df[pred_col].astype(str)).sum())
                pct = round(100.0 * correct / total, 1) if total else 0.0
            else:
                correct, pct = 0, 0.0
            model_name = "<e.g. logistic regression>"
            insight = (
                f"This {model_name} correctly classified {pct}% of rows ({correct}/{total}). "
                f"Strongest predictors depend on the features you kept in Projection—see the chart. "
                f"To use on new data, feed rows through the same Projection → SklearnPrediction chain. "
                f"For high-stakes decisions, tune thresholds to balance false positives vs false negatives."
            )
            yield pd.DataFrame({"insight": [insight]})
        except Exception as e:
            yield pd.DataFrame({"insight": [f"{type(e).__name__}: {str(e)[:200]}"]})
```

**Regression** workflows: rewrite the insight string to cite MAE / R² / predicted-vs-actual in plain English instead of "classified …%".

**DAG order:** Prefer **`SklearnPrediction` → `Scatterplot` (or BarChart) → AI Insights`** so Insights **`input-0`** is fed from **`Visualization` `output-0`** when that operator passes through the scored table. If the chart output schema is unsuitable, connect **AI Insights `input-0`** directly from **`SklearnPrediction` `output-0`**, but still add **Visualization** on its own branch from **`SklearnPrediction`**. **AI Insights** must remain the **last** operator in your executed plan.

Set **`viewResult`: true** on **Visualization** and **AI Insights**.
"""


# Appended after Iris JSON (plain string — avoids f-string brace issues with embedded JSON examples).
STRICT_PROD_REQUIREMENTS = """
=== OPERATOR & LINK RULES (runnable workflow) ===

A. PORT WIRING (CRITICAL for SklearnPrediction):

SklearnPrediction has TWO input ports in JSON as **`"portID": "input-0"`** (model) and **`"portID": "input-1"`** (data):

- **`input-0` = MODEL** — link from the Sklearn trainer's **`output-0`** (the emitted `model`).
- **`input-1` = DATA** — link rows to score, **same schema** as test/holdout (typically from **Split `output-1`**).

If these are swapped or use the wrong `portID` strings, the workflow will not behave correctly in Texera.
Re-read **GROUND-TRUTH LINK JSON** above for port-ID discipline; mirror that **shape** when you wire your own operators.

A2. **EVERY SKLEARN TRAINER** (`SklearnLogisticRegression`, `SklearnDecisionTree`, `SklearnRandomForest`, …) **MUST** have **TWO** inputs from the **same** **Split**: training → **`input-0`** (**Split `output-0`**), testing → **`input-1`** (**Split `output-1`**). In **addOperator**, that means **`inputOperatorIds` includes BOTH `"0"` and `"1"`**. Parallel AutoML with **3** trainers ⇒ **all 3** need **both** edges. Missing testing on **any** trainer (**especially the first / LogReg**) ⇒ **Invalid Workflow**.

B. EVERY PythonUDFV2 NODE MUST:

- Use the **Table** `process_table` API:
  - Import from `pytexera` (e.g. `UDFTableOperator`, `Table`, `TableLike`, `overrides`, `Iterator`, `Optional`).
  - `class ProcessTableOperator(UDFTableOperator):` with `@overrides` and only
    `def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:` (do not use `process_tuple` for table UDFs).

- Wrap the **entire** body in `try` / `except`. On error, **yield ONE diagnostic row** as a DataFrame instead of raising (so the worker does not crash).

- **`outputColumns` MUST EXACTLY match** the columns you yield. Example: if you yield a DataFrame with columns `metric` (string) and `value` (double), `outputColumns` must be:
  `[{"attributeName": "metric", "attributeType": "string"}, {"attributeName": "value", "attributeType": "double"}]`.
  If the error branch yields column `error` but `outputColumns` expect `metric` / `value`, the **Result panel can be empty**.

- **SAFEST:** in `except`, `import pandas as pd` if needed and yield a DataFrame with the **same columns** as the success path (e.g. put `f"{type(e).__name__}: {str(e)[:200]}"` in `metric` and `-1.0` in `value`).

- **NEVER** call `table.to_pandas()` — `Table` is already pandas-like; use `pd.DataFrame(table)` or `table.copy()`.

C. AI INSIGHTS NODE STANDARDIZATION:

The final **"AI Insights"** `PythonUDFV2` must:

- **outputColumns:** `[{"attributeName": "insight", "attributeType": "string"}]`
- **Success path:** `yield` ONE row: `pd.DataFrame({"insight": [your_natural_language_string]})`
- The string (**English only**, 3–5 sentences) must explain:
  1. What the workflow did (e.g. compared three classifiers on the dataset),
  2. Key result (e.g. best accuracy),
  3. What it means for a non-expert,
  4. How to use on new data (e.g. connect a new CSV through **Projection** into **SklearnPrediction `"input-1"`**).

D. GRAPH STRUCTURE YOU ARE AIMING FOR

- **CSVFileScan** has no input link.
- Every Sklearn trainer **`output-0`** that should score rows feeds **SklearnPrediction `input-0`**; **`input-1`** receives the aligned scoring table (**usually Split `output-1`**).
- Set **`viewResult`: true** on **leaf** operators (terminal nodes) the user should see in the Result panel.
- Each link's **`operatorID`** refers to an operator in **`operators`**; **`portID`** matches that operator's `inputPorts` / `outputPorts`.

E. REQUIRED `operatorProperties` SHAPES (missing or wrong → broken workflow in UI):

- **Split** — MUST include integer **`k`** (1–99, percent of rows to **`output-0`** / training branch), plus **`random`** (boolean) and **`seed`** (int). Example: `"k": 70, "random": true, "seed": 1`. (JSON field is **`k`**, not `partitionPercentage`.)
- **Projection** — MUST include non-empty **`attributes`**: `[{"originalAttribute": "ColA"}, {"alias": "", "originalAttribute": "Label"}, ...]` and **`isDrop`**: false unless intentionally dropping.
- **Sklearn classifiers** — MUST set JSON **`target`** to the label column name. Include **`countVectorizer`**: false, **`tfidfTransformer`**: false unless doing text.
- **SklearnPrediction** — MUST set **`Model Attribute`**: `"model"`, **`Output Attribute Name`**: `"prediction"` (or your name), and **`Ground Truth Attribute Name to Ignore`** to the label column when evaluating (or `""` for pure scoring).
- **Scatterplot** — MUST set **`xColumn`**, **`yColumn`**, optional **`colorColumn`**, **`xLogScale`**, **`yLogScale`**, **`alpha`** (see VISUALIZATION section).

F. OPERATOR TYPES:

Only use **`operatorType`** values from **ALLOWED_OPERATOR_TYPES**. If you need an algorithm or transform not listed, implement it in **PythonUDFV2** instead of inventing a type name.

===
"""


def _samples_as_dicts(
    columns: list[str] | None,
    sample_rows: list[Any] | None,
) -> list[dict[str, Any]]:
    if not sample_rows or not columns:
        return []
    first = sample_rows[0]
    if isinstance(first, dict):
        return [dict(x) for x in sample_rows if isinstance(x, dict)]
    out: list[dict[str, Any]] = []
    for row in sample_rows:
        if isinstance(row, (list, tuple)):
            out.append(dict(zip(columns, list(row))))
    return out


def _build_agent_prompt(
    *,
    goal_for_agent: str,
    dataset_path: str,
    dataset_id: int,
    columns: list[str] | None,
    sample_rows: list[Any] | None,
    dataset_summary: str,
    scenario_label: str,
    analysis_card_title: str | None = None,
    analysis_card_description: str | None = None,
) -> str:
    cols_str = ", ".join(columns) if columns else "(unknown — infer from CSV scan)"
    samples = _samples_as_dicts(columns, sample_rows)[:3]
    sample_txt = json.dumps(samples, default=str, indent=2)
    extra = ""
    if analysis_card_title:
        extra += f"\n## Selected card (user-facing)\n**{analysis_card_title}**\n"
    if analysis_card_description:
        extra += f"{analysis_card_description}\n"

    allowed_txt = json.dumps(ALLOWED_OPERATOR_TYPES)

    plan_protocol = """## Workflow building protocol — STRICT

PHASE 0 — TARGET COMPLETENESS (read before planning):
A complete supervised ML workflow for non-technical users **includes all seven** stages below. This is a **completeness checklist**, not a template: you pick the Sklearn algorithm, operator ids (`op1`…), and labels from **this CSV** and the user's goal.

1. **CSVFileScan** — load data  
2. **Projection** — select features + target  
3. **Split** — train/test partition  
4. **Sklearn&lt;algorithm&gt;** — train (e.g. binary classification → `SklearnLogisticRegression`; regression → `SklearnLinearRegression`)  
5. **SklearnPrediction** — score the holdout/test split  
6. **Visualization** — **`Scatterplot`** (Iris-style) or **`BarChart`**; see **VISUALIZATION** section below  
7. **PythonUDFV2** — **"AI Insights"** — one English **insight** row with accuracy (classification) or error-fit summary (regression)

**Build all 7. Do not stop at step 5.** Stopping after `SklearnPrediction` without visualization and insights is **incomplete**.

PHASE 1 (planning, text only):
Before any tool call, write your full plan as plain text:
  OPERATORS: every operator you will add, in order, with `operatorType` and id (`op1`, `op2`, … per the agent `addOperator` tool).
  LINKS: every connection: source operator id → target operator id, specifying which output feeds which target input port index.
  **SKLEARN TRAINERS (mandatory checklist):** For **each** `Sklearn*` **trainer**, write exactly one line:
    `<TrainerOpId> (<algorithm>): trainingFrom=<SplitOpId> output-0 (train), testingFrom=<SplitOpId> output-1 (test)`
    Example: `op4 (SklearnLogisticRegression): trainingFrom=op3 output-0, testingFrom=op3 output-1`
    If you plan **parallel** trainers (e.g. 3 for AutoML), you need **three** such lines — **every** line must show **both** training and testing. If any line omits `testingFrom`, fix the plan before PHASE 2.

The Iris JSON below is a **topology reference** for `portID` style and JSON shape — not a template to copy.

PHASE 2 (execution — tool calls only):
Call **addOperator** once per planned operator **in order**, using **`inputOperatorIds`** so incoming edges match your LINKS plan (Texera encodes links inside **addOperator** / **modifyOperator**; there is no separate add_link tool). For **each** Sklearn trainer, **`inputOperatorIds` MUST include BOTH keys `"0"` and `"1"`** mapped to the Split's training and testing branches respectively.

Do not inspect or second-guess partial graph state between calls beyond tool return values.
Do not call **deleteOperator** or **deleteLink** (disabled).
Do not re-plan mid-execution.
Do not add **retry** or **delete-and-recreate** cycles to "fix" wiring — plan correctly once in PHASE 1 and execute in PHASE 2.
If a tool returns an error, note it in one line and continue with the next planned call. At most **one** retry for a single failed call if absolutely necessary.

PHASE 3 (done):
**Before** you respond with exactly: `Workflow built.` — perform a **silent mental check only** (no tool call, no workflow validation API):
  - Does **every** Sklearn trainer have **BOTH** training (`input-0` / `inputOperatorIds["0"]`) **and** testing (`input-1` / `inputOperatorIds["1"]`) edges from the **Split**? In an AutoML graph with **3** parallel trainers, confirm **all 3** — especially the **first** listed (e.g. Logistic Regression), which is the most often under-wired.

Then respond with exactly: Workflow built.
"""

    head = f"""You are a senior data scientist designing a Texera workflow for a non-technical user.

{plan_protocol}
## USER'S GOAL (authoritative — implement this)
{goal_for_agent}
{extra}
## DATASET CONTEXT
- File path (CSV inside Texera storage): `{dataset_path}`
- Dataset ID: `{dataset_id}`
- Scenario label: `{scenario_label}`
- Columns: {cols_str}
- Summary: {dataset_summary}
- Sample rows (first 3, as records): 
{sample_txt}

## DESIGN PRINCIPLES

1. **Prefer native Texera Sklearn operators** (`SklearnLogisticRegression`, `SklearnDecisionTree`, `SklearnRandomForest`, `SklearnLinearRegression`, `SklearnKNN`, `SklearnPerceptron`, etc.) for training. They are visual and idiomatic. Use **SklearnPrediction** to apply the fitted model to held-out or new rows. Avoid `PythonUDFV2` unless the task cannot be expressed natively (e.g. KMeans clustering, custom AutoML comparison tables).

2. **Close the loop to stage 7.** Supervised pipelines must reach: `CSVFileScan` → `Projection` → `Split` → `Sklearn<Algo>` → `SklearnPrediction` → **Visualization** (`Scatterplot` or `BarChart`) → **PythonUDFV2 "AI Insights"**. **Without steps 6–7, the demo is incomplete** for non-experts.

3. **SklearnPrediction wiring (critical)**  
   - Port 0: model from the trainer.  
   - Port 1: test data **with the same feature columns**; set **Ground Truth Attribute Name** when you want evaluation against a label column.  
   - Output schema: **input columns plus** the prediction column (default name `prediction` unless you change **Output Attribute Name**).

### EVERY SKLEARN TRAINER NEEDS TWO SPLIT EDGES (READ BEFORE addOperator)

**EVERY** Sklearn **trainer** (`SklearnLogisticRegression`, `SklearnDecisionTree`, `SklearnRandomForest`, …) MUST receive **both** **Split** branches:

| Trainer input | Meaning | Wire from |
|---------------|---------|-----------|
| **`input-0`** / `inputOperatorIds[\"0\"]` | **Training** rows | **Split `output-0`** |
| **`input-1`** / `inputOperatorIds[\"1\"]` | **Testing / holdout** rows | **Split `output-1`** |

**ALL** parallel trainers in an AutoML-style graph MUST have **both** — **including the first** (LogReg is the common mistake). Missing **`input-1`** ⇒ **Invalid Workflow** and **0** tuples on the testing port. **This rule is non-negotiable.**

4. **Visualize after predictions, then narrate.** After `SklearnPrediction`, add **`Scatterplot`** (see Iris / **VISUALIZATION** section) or **`BarChart`** for class or prediction summaries. Then add **"AI Insights"** `PythonUDFV2` using the **AI INSIGHTS — code starting point** block below.

5. **Adapt to this dataset.** Infer feature vs target columns from names and samples. Binary / multi-class → classifiers, not regression. Continuous target → `SklearnLinearRegression` or similar. Dataset size is modest — no deep learning.

6. Set **`viewResult`: true** on **Visualization**, **AI Insights**, and other **leaf** operators the user should open in the Result panel.

7. **Operator property shapes (Texera JSON)**  
   - **Filter** — `predicates`: list of `{{"attribute": "", "condition": "", "value": ""}}`.  
   - **Aggregate** — `groupByKeys`, `aggregations` with `{{"aggFunction": "count", "attribute": "", "result attribute": ""}}` (space in `result attribute`).  
   - **BarChart** — `categoryColumn`, `fields`, `value`, `horizontalOrientation`.  
   - **PieChart** — `name`, `value`.  
   - **Sort** — `keys`: `{{"attributeName": "", "sortPreference": "ASC"|"DESC"}}`.  
   - **Sklearn trainers** — JSON key **`target`** = label column (maps to UI "Target Attribute").  
   - **Split** — JSON keys **`k`**, **`random`**, **`seed`** (see OPERATOR & LINK RULES section E).  
   - **PythonUDFV2** — `Table` API: `class ProcessTableOperator(UDFTableOperator)` with `process_table`; `Table` behaves like a pandas DataFrame (`pd.DataFrame(table)` or `table.copy()`); never `to_pandas()`.

8. **Link JSON** must use **`source` / `target` / `portID`** exactly like the Iris file — never `fromPortId` / `toPortId`.

## FINAL OPERATORS — VISUALIZATION + "AI INSIGHTS"
After **`SklearnPrediction`**, you **must** add **Visualization** (`Scatterplot` or `BarChart`) and a final **`PythonUDFV2`** named **"AI Insights"** per the **VISUALIZATION** and **AI INSIGHTS — code starting point** sections below (§B/C in OPERATOR & LINK RULES still apply).

## REFERENCE WORKFLOW (Iris ML example — port wiring reference; design freely for this dataset)
Use this JSON for **shape** and **portID** habits — your operators and graph layout should reflect **this CSV** and the user's goal, not a fixed Iris clone:

"""

    minimal_instructions = (
        """
## Iris reference subgraph (**shape** / port discipline — not a mandatory layout)
The fragment below is one valid supervised subgraph from Iris. Use it to learn Texera JSON keys and Sklearn/Split/Prediction port wiring. Choose algorithms, branch counts, and operator counts **based on the data and goal** (e.g. regression vs classification changes the whole story).

```json
"""
        + IRIS_SINGLE_BRANCH_TEMPLATE
        + """
```

"""
    )

    tail = f"""{STRICT_PROD_REQUIREMENTS}
{IRIS_SKLEARN_PREDICTION_LINK_EXAMPLES}

## AVAILABLE OPERATOR TYPES (must only use these)
{allowed_txt}

After building the workflow graph, briefly explain in **2–3 English sentences** what the user will see and how to read the results.
"""
    return head + IRIS_EXAMPLE_WORKFLOW + minimal_instructions + POST_IRIS_VIZ_AND_INSIGHTS_GUIDANCE + tail


def _agent_response_dict(
    *,
    agent_prompt: str,
    workflow_name: str,
    suggestion_id: str,
    file_path: str,
    target_column: str = "",
) -> dict[str, Any]:
    return {
        "mode": "agent",
        "agent_prompt": agent_prompt,
        "allowed_operator_types": list(ALLOWED_OPERATOR_TYPES),
        "model": DEFAULT_AGENT_MODEL,
        "workflow_name": workflow_name,
        "suggestion_id": suggestion_id,
        "dataset_path": file_path,
        "target_column": target_column,
    }


def render_for_suggestion(
    suggestion: dict[str, Any],
    *,
    dataset_summary: str,
    scenario_label: str,
    columns: list[str] | None,
    sample_rows: list[Any] | None,
    file_path: str,
    dataset_id: int,
) -> dict[str, Any]:
    """Natural-language agent prompt from an analyze-card suggestion."""
    goal = (suggestion.get("goal_for_agent") or "").strip()
    if not goal:
        raise ValueError("suggestion missing goal_for_agent")
    public_sid = str(suggestion.get("id", "suggestion"))
    title = suggestion.get("title", "Analysis")
    desc = suggestion.get("description", "")
    body = _build_agent_prompt(
        goal_for_agent=goal,
        dataset_path=file_path,
        dataset_id=dataset_id,
        columns=columns,
        sample_rows=sample_rows,
        dataset_summary=dataset_summary or "",
        scenario_label=scenario_label or "",
        analysis_card_title=str(title) if title else None,
        analysis_card_description=str(desc) if desc else None,
    )
    tc = suggestion.get("target_column")
    target = "" if tc is None else str(tc)
    return _agent_response_dict(
        agent_prompt=body,
        workflow_name=f"[Genesis] {title}".strip(),
        suggestion_id=public_sid,
        file_path=file_path,
        target_column=target,
    )


def render_custom_goal(
    custom_goal: str,
    *,
    dataset_summary: str,
    scenario_label: str,
    columns: list[str] | None,
    sample_rows: list[Any] | None,
    file_path: str,
    dataset_id: int,
) -> dict[str, Any]:
    """Free-text user goal from the dashboard textarea."""
    goal = custom_goal.strip()
    if not goal:
        raise ValueError("custom_goal is empty")
    body = _build_agent_prompt(
        goal_for_agent=goal,
        dataset_path=file_path,
        dataset_id=dataset_id,
        columns=columns,
        sample_rows=sample_rows,
        dataset_summary=dataset_summary or "",
        scenario_label=scenario_label or "",
    )
    return _agent_response_dict(
        agent_prompt=body,
        workflow_name="[Genesis] Custom analysis",
        suggestion_id="custom_goal",
        file_path=file_path,
        target_column="",
    )

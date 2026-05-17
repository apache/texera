# BioFlow Genesis
> AI guesses what you want to do, from the dataset itself.

![demo](docs/demo.gif)

---

## The Problem

Existing AI-Texera tools generate workflows from user intent. The user types
"train a classifier on diabetes data," the LLM picks operators, the wiring
succeeds or fails based on prompt luck. This works for engineers who already
know what they want.

For a biology PhD with 60 datasets and no Python, even formulating the
question is the hard part. They don't know what task type fits their data,
which target column matters, or whether the result will be interpretable.
A "describe your workflow" box looks helpful but solves the easy half of
the problem.

Genesis flips this: the AI reads the data first, then proposes what to ask.
No prompt. No menu. Drop the file, get four typed recommendations grounded
in the actual columns, click one, run.

---

## What It Does

1. Drop a CSV → AI profiles data → 4 recommendation cards.
2. One click → wired Texera workflow with sklearn training.
3. Run → AI Insight node writes 5-section clinical interpretation.

---

## Cross-Domain Verification

| Diabetes (Pima Indians, NIDDK) | California Housing |
| --- | --- |
| 768 rows, target=Outcome | 20,640 rows, target=median_house_value |
| Auto-detected: **classification** | Auto-detected: **regression** |
| Algorithm: LogisticRegression | Algorithm: LinearRegression |
| Workflow: 6 nodes | Workflow: 7 nodes (with NaN imputation) |
| Result: **72% accuracy** | Result: **R²=0.62, MAE=$50,900** |

Same product. Two completely different domains. Zero user code. The AI
didn't follow a template — it understood the data.

---

## Architecture: LLM Reads, Python Builds

```
User drops CSV
     │
     ▼
[LLM: Claude Haiku]  ← reads, profiles, recommends
     │
     ▼
4 cards (task-typed, target-inferred, algorithm-chosen)
     │
     ▼
User clicks one
     │
     ▼
[Python workflow_builder]  ← builds deterministically
     │
     ▼
Texera JSON (CSVFileScan → Projection → ... → AI Insight)
     │
     ▼
Texera Amber engine executes
```

LLM creativity is bounded to text generation (card descriptions, insight
summaries). Workflow wiring is deterministic Python — operator IDs, port
mappings, links are emitted by a tested code path, not invented by an LLM.
Result: zero hallucinated wirings, ever.

This contrasts with prompt-then-pray approaches that retry on validation
failure. Genesis can't fail validation because generation can't produce
invalid output.

---

## Five Verified Skeletons

| Skeleton | Nodes | When chosen | Verified on |
| --- | --- | --- | --- |
| Classification | 6 | Binary/multiclass target | Pima Indians Diabetes — 72% |
| Regression | 7 | Continuous target | California Housing — R²=0.62 |
| Exploration | 4 | "find drivers" intent | Diabetes — Glucose r=0.47 |
| AutoML | 10 | "compare models" intent | 3 trainers (LogReg/DT/RF) |
| Visualization | 5 | Distribution intent | Pie chart aggregations |

---

## Demo Video

Watch the 2-minute demo: [YouTube link](https://youtu.be/REPLACE_ME)

---

## Technical Components

### Backend (`bioflow-genesis-service/`)
- FastAPI service on port 9099.
- `core/classifier.py` — LLM card generation with task-mix enforcement.
- `core/workflow_builder.py` — 5 deterministic skeleton builders.
- `core/texera_client.py` — wraps Texera persist API.
- `core/iris_ml_example.json` — reference Texera workflow JSON.
- 10 passing unit tests, all skeletons verified.

### Frontend (`texera/frontend/`)
- 2×2 card grid modal (Linear-style progress panel).
- AI Insight 5-section card rendering (replacing default `nz-table`).
- `/api/genesis/build` integration via `genesis-orchestrator.service`.
- Free-text input box for natural language goals.
- Auto-naming workflows from card titles.

---

## Why It Fits the Hackathon

- **Data / AI for Science** — end-to-end on NIDDK-originated biomedical
  data (Pima Indians Database). Same NIH agency that funds dkNET.
- **Human-Agent Collaboration** — AI doesn't ask "what do you want?" first.
  It reads the data and proposes options the user wouldn't know to ask for.
- **Production-ready** — no engine changes, no new operators, drops into
  existing Texera workspace via one new endpoint.

---

## Try It

```bash
~/hackathon/venv312/bin/pip install -r requirements.txt
~/hackathon/venv312/bin/python -m uvicorn main:app --reload --port 9099
# Open Texera → /dashboard/user/workflow → drag a CSV onto the canvas.
```

---

## Acknowledgments

Built for the dkNET-AI · Apache Texera Agent Hackathon. The Pima Indians
Diabetes Database originates from NIDDK's 1965 epidemiological study.

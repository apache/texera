# AI-Augmented Macro Operators for Texera

## Problem
Texera workflows grow into 20–50+ operator DAGs with no encapsulation. Users copy-paste the same subgraphs across projects, and pipelines run slower than they need to because of inter-operator serialization.

## What we'll build
- **Macro operators** — collapse a selection of operators into one reusable, version-pinned node (KNIME wrapped-metanode style). Drill-down to edit; drag from a library to reuse.
- **Agent tool: `suggestMacros`** — the agent inspects the `LogicalPlan` and proposes ranked subgraphs to encapsulate, each with a one-line rationale ("looks like a reusable text-preprocessing block"). Highlights candidates on the canvas; one click materializes.
- **Agent tool: `fuseMacro`** — for a macro whose internals the user no longer needs to inspect, the agent synthesizes an equivalent `PythonUDFOpDescV2`, runs original and fused on a sample, diffs outputs, and only swaps in after verification passes.

## Why it fits the Agent Hackathon
- Plugs straight into the existing `agent-service` (Vercel AI SDK + ReAct loop + tool framework). Two new tools, no new LLM plumbing.
- The agent doesn't just suggest — it **verifies** (sample-run diff for fusion) and rolls back on mismatch. Concrete, measurable correctness.
- Showcases capabilities a generic chatbot can't: structural reasoning over a DAG, codegen for a known runtime, and a built-in verification harness.

## Demo (~3 min)
1. Open a 15-operator workflow → "Suggest Macros (AI)" → three highlighted candidates appear with rationales.
2. Accept one → subgraph collapses into a single macro node.
3. Run → note baseline time.
4. Right-click macro → "Fuse for performance" → agent generates UDF, verifies ("matched on 1000 sample rows"), swaps in.
5. Re-run → show **2–5× speedup** on the stateless chain.

## Stretch
- Cross-workflow pattern mining: "you've built this subgraph 4 times — save as a macro?"
- Auto-publish recurring patterns to the workflow hub as community macros.

# Design: Unified `WorkflowCompiler`

## Why

Two independent `WorkflowCompiler` copies existed — one in amber (execution path) and one in
workflow-compiling-service (editing path) — with near-identical logical→physical expansion that
drifted and had to be hand-synced. `LogicalPlan`, `LogicalPlanPojo`, and `LogicalLink` were
duplicated too. Their only real differences were the error mode (collect vs. throw), amber's
extra `outputPortsNeedingStorage`, and amber returning the engine `Workflow`. None justified two
codebases.

## Approach

One compiler source, reused **in-process** by both callers — no service-to-service RPC, no
serializing physical plans across a process boundary. The duplication is solved at the code
level, not by extracting a runtime compilation service (considered and rejected).

```
common/workflow-compiler   .dependsOn(WorkflowOperator)
        │   WorkflowCompiler + WorkflowCompilationResult
        │   LogicalPlan / LogicalPlanPojo / LogicalLink   (package org.apache.texera.common.compiler)
        │
   ┌────┴─────┐
   ▼          ▼
compiling-    amber (execution)
service       thin adapter → engine Workflow
(Lenient)     (Strict)
```

The module depends only on `WorkflowOperator` (Dropwizard/HTTP never leak into it), so there is
no service-to-service dependency. Source lives under
[`src/main/scala/org/apache/texera/common/compiler/`](src/main/scala/org/apache/texera/common/compiler/):
[`WorkflowCompiler.scala`](src/main/scala/org/apache/texera/common/compiler/WorkflowCompiler.scala),
[`CompilationErrorHandling.scala`](src/main/scala/org/apache/texera/common/compiler/CompilationErrorHandling.scala),
and [`model/`](src/main/scala/org/apache/texera/common/compiler/model/).

## Key decisions

- **One strict/lenient API, not two entry points.** `compile(pojo, errorHandling)` takes
  `Lenient` (editing — accumulate per-operator errors, `physicalPlan = None` when any exist) or
  `Strict` (execution — fail-fast). The internal `expandLogicalPlan` already parameterized this
  via `Option[errorList]`; the mode just names it. See `compile` in `WorkflowCompiler.scala`.

- **`outputPortsNeedingStorage` is always computed and returned**, never gated behind the mode.
  Cost is one extra topological pass; the compiling-service caller simply ignores the field.
  This keeps both paths on a single code path rather than branching the compiler.

- **The engine `Workflow` stays in amber.**
  `…engine.architecture.coordinator.Workflow` cannot descend into a `common/*` module, so amber
  keeps a thin adapter that wraps the result into a `Workflow` (Strict guarantees a defined
  `physicalPlan`). Call sites: `SyncExecutionResource`, `WorkflowExecutionService`, `TestUtils`.

- **Package unified** to `org.apache.texera.common.compiler`, dropping the misleading
  `org.apache.texera.amber.compiler`.

## Tests

Compiler and model unit specs live with the code under
[`src/test/scala/org/apache/texera/common/compiler/`](src/test/scala/org/apache/texera/common/compiler/):
`WorkflowCompilerSpec` (both modes, physical-plan shape, storage-port collection, schema
propagation), `LogicalPlanSpec`, and `model/LogicalLinkSpec`.

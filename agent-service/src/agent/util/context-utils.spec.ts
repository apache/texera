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

import { describe, expect, test } from "bun:test";
import type { ModelMessage } from "ai";
import { assembleContext, DOCS_CONTEXT_CHAR_LIMIT } from "./context-utils";
import { WorkflowState } from "../workflow-state";
import type { ReActStep } from "../../types/agent";
import type { OperatorLink, OperatorPredicate } from "../../types/workflow";
import type { WorkflowCompilationResponse } from "../../api/compile-api";
import { WorkflowFatalErrorType } from "../../types/execution";

function step(messageId: string, role: "user" | "agent", stepId: number, content: string, isEnd: boolean): ReActStep {
  return {
    id: `${messageId}-${stepId}`,
    messageId,
    stepId,
    timestamp: 0,
    role,
    content,
    isBegin: true,
    isEnd,
  };
}

function makeOperator(id: string, overrides: Partial<OperatorPredicate> = {}): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: [],
    outputPorts: [],
    showAdvanced: false,
    ...overrides,
  };
}

function makeLink(source: string, sourcePort: string, target: string, targetPort: string): OperatorLink {
  return {
    linkID: `${source}.${sourcePort}->${target}.${targetPort}`,
    source: { operatorID: source, portID: sourcePort },
    target: { operatorID: target, portID: targetPort },
  };
}

// assembleContext always returns a single user message whose content is a joined string.
function contentOf(result: ModelMessage[]): string {
  expect(result).toHaveLength(1);
  expect(result[0].role).toBe("user");
  expect(typeof result[0].content).toBe("string");
  return result[0].content as string;
}

describe("assembleContext", () => {
  test("returns a single user message with no task sections for empty steps", () => {
    const result = assembleContext([], new WorkflowState(), new Map());
    expect(result).toHaveLength(1);
    expect(result[0].role).toBe("user");
    expect(contentOf(result)).toBe("");
  });

  test("renders a completed task under a Completed Tasks heading", () => {
    const steps = [step("m1", "user", 0, "do X", true), step("m1", "agent", 1, "thinking", true)];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("# Completed Tasks");
    expect(content).toContain("## Task (completed)");
    expect(content).toContain("do X"); // the user request
    expect(content).toContain("### Turn 1"); // the agent step
  });

  test("renders an unfinished task under an Ongoing Task heading", () => {
    const steps = [step("m1", "user", 0, "do Y", true), step("m1", "agent", 1, "working", false)];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("# Ongoing Task");
    expect(content).toContain("## Task (ongoing)");
  });

  test("appends a Current Dataflow section when the workflow has operators", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const content = contentOf(assembleContext([], workflowState, new Map()));
    expect(content).toContain("# Current Dataflow");
    expect(content).toContain("## Operators");
  });

  test("groups steps sharing a messageId into a single task", () => {
    const steps = [
      step("m1", "user", 0, "req", true),
      step("m1", "agent", 1, "turn one", false),
      step("m1", "agent", 2, "turn two", true),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect((content.match(/## Task/g) ?? []).length).toBe(1);
    expect(content).toContain("### Turn 1");
    expect(content).toContain("### Turn 2");
  });

  test("labels each tool call with the status of the result at the same index", () => {
    const agentStep: ReActStep = {
      ...step("m1", "agent", 1, "acting", true),
      toolCalls: [
        { toolName: "addOperator", toolCallId: "c1", input: {} },
        { toolName: "executeOperator", toolCallId: "c2", input: {} },
        { toolName: "deleteOperator", toolCallId: "c3", input: {} },
      ],
      // Deliberately shorter than toolCalls: a call still awaiting its result
      // must not be reported as failed.
      toolResults: [
        { toolCallId: "c1", output: "ok" },
        { toolCallId: "c2", output: "bad", isError: true },
      ],
    };
    const content = contentOf(
      assembleContext([step("m1", "user", 0, "req", true), agentStep], new WorkflowState(), new Map())
    );
    expect(content).toContain("- addOperator (succeeded)");
    expect(content).toContain("- executeOperator (failed)");
    expect(content).toContain("- deleteOperator (succeeded)");
  });
});

describe("assembleContext — documentation output", () => {
  type Call = { tool: string; input: unknown; output?: unknown; isError?: boolean };

  /** An agent step whose tool calls carry the given outputs; a call without `output` has no result yet. */
  function toolStep(messageId: string, stepId: number, calls: Call[], isEnd = false): ReActStep {
    return {
      ...step(messageId, "agent", stepId, `turn ${stepId}`, isEnd),
      toolCalls: calls.map((c, i) => ({ toolName: c.tool, toolCallId: `${messageId}-${stepId}-${i}`, input: c.input })),
      toolResults: calls.flatMap((c, i) =>
        c.output === undefined
          ? []
          : [{ toolCallId: `${messageId}-${stepId}-${i}`, output: c.output, ...(c.isError ? { isError: true } : {}) }]
      ),
    };
  }

  const dropped = (n: number) =>
    `(${n} earlier output${n === 1 ? "" : "s"} dropped to save space, call the tool again if you need ${n === 1 ? "it" : "them"})`;
  const REJECTED = "(no output, the tool is unavailable or its arguments were invalid)";
  /** What one entry costs against the budget: its label, its indented output and the blank line before it. */
  const cost = (label: string, output: string) =>
    label.length +
    1 +
    output
      .split("\n")
      .map(l => `  ${l}`)
      .join("\n").length +
    2;
  const readLabel = (turn: number, path: string) => `## Turn ${turn}: readDoc {"path":"${path}"}`;
  /** A one-line output sized so that its entry costs exactly `total`. */
  const sized = (turn: number, path: string, total: number, fill = "x") =>
    fill.repeat(total - cost(readLabel(turn, path), ""));
  const sectionOf = (content: string) => content.slice(content.indexOf("# Documentation Read"));
  const HEADER = "# Documentation Read\nOutput of your searchDocs and readDoc calls in this task, oldest first.";
  const search = (query: string, output: unknown): Call => ({ tool: "searchDocs", input: { query }, output });
  const read = (path: string, output: unknown): Call => ({ tool: "readDoc", input: { path }, output });

  test("renders the output of a docs call of the ongoing task under its turn, tool and input", () => {
    const steps = [
      step("m1", "user", 0, "how do I upload a dataset", true),
      toolStep("m1", 1, [search("upload dataset", "1. Datasets > Uploading files\n   path: getting-started/datasets")]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(
      `${HEADER}\n\n## Turn 1: searchDocs {"query":"upload dataset"}\n  1. Datasets > Uploading files\n     path: getting-started/datasets`
    );
  });

  test("places the section after the ongoing task and before the dataflow", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const steps = [step("m1", "user", 0, "q", true), toolStep("m1", 1, [search("x", "found")])];
    const content = contentOf(assembleContext(steps, workflowState, new Map()));
    const ongoing = content.indexOf("# Ongoing Task");
    const docs = content.indexOf("# Documentation Read");
    const dataflow = content.indexOf("# Current Dataflow");
    expect(ongoing).toBeGreaterThanOrEqual(0);
    expect(docs).toBeGreaterThan(ongoing);
    expect(dataflow).toBeGreaterThan(docs);
  });

  test("lists several outputs oldest first, one entry per call", () => {
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [search("a", "search output")]),
      toolStep("m1", 2, [read("p/one", "first page"), read("p/two", "second page")]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    const order = [
      "## Turn 1: searchDocs",
      '## Turn 2: readDoc {"path":"p/one"}',
      '## Turn 2: readDoc {"path":"p/two"}',
    ];
    const positions = order.map(label => content.indexOf(label));
    expect(positions.every(p => p >= 0)).toBe(true);
    expect([...positions].sort((a, b) => a - b)).toEqual(positions);
    expect(content).toContain("  first page");
    expect(content).toContain("  second page");
  });

  test("includes a failed docs call, since its text says what to do next", () => {
    // A tool that fails returns a string starting with [ERROR]; the stored result is not flagged.
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [{ tool: "readDoc", input: { path: "nope" }, output: '[ERROR] Unknown page "nope".' }]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain('## Turn 1: readDoc {"path":"nope"}\n  [ERROR] Unknown page "nope".');
  });

  test("renders no section when the ongoing task made no docs call", () => {
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [{ tool: "addOperator", input: { operatorId: "op1" }, output: "Added operator op1" }]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).not.toContain("# Documentation Read");
    expect(content).not.toContain("Added operator op1");
  });

  test("says that a docs call without a result was rejected, so the model can correct it", () => {
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [{ tool: "searchDocs", input: { limit: "five" } }]),
      toolStep("m1", 2, [search("upload", "found it")]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(`## Turn 1: searchDocs {"limit":"five"}\n  ${REJECTED}`);
    expect(content).toContain('## Turn 2: searchDocs {"query":"upload"}\n  found it');
  });

  test("shows the section for a rejected docs call even when it is the only one", () => {
    const steps = [step("m1", "user", 0, "q", true), toolStep("m1", 1, [{ tool: "readDoc", input: {} }])];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("# Documentation Read");
    expect(content).toContain(`## Turn 1: readDoc {}\n  ${REJECTED}`);
  });

  test("renders a result without output as having none instead of failing", () => {
    const withUndefined: ReActStep = {
      ...step("m1", "agent", 1, "t", false),
      toolCalls: [{ toolName: "searchDocs", toolCallId: "a", input: { query: "x" } }],
      toolResults: [{ toolCallId: "a", output: undefined }],
    };
    const content = contentOf(
      assembleContext([step("m1", "user", 0, "q", true), withUndefined], new WorkflowState(), new Map())
    );
    expect(content).toContain('## Turn 1: searchDocs {"query":"x"}\n  (no output)');
  });

  test("does not report a rejected call of another tool in the docs section", () => {
    const steps = [step("m1", "user", 0, "q", true), toolStep("m1", 1, [{ tool: "addOperator", input: {} }])];
    expect(contentOf(assembleContext(steps, new WorkflowState(), new Map()))).not.toContain("# Documentation Read");
  });

  test("drops the docs output of a completed task", () => {
    const done = [
      step("m1", "user", 0, "first question", true),
      toolStep("m1", 1, [search("old", "OLD SEARCH OUTPUT")]),
      step("m1", "agent", 2, "the answer", true),
    ];
    const alone = contentOf(assembleContext(done, new WorkflowState(), new Map()));
    expect(alone).not.toContain("# Documentation Read");
    expect(alone).not.toContain("OLD SEARCH OUTPUT");

    const next = [
      ...done,
      step("m2", "user", 0, "second question", true),
      toolStep("m2", 1, [search("new", "NEW OUTPUT")]),
    ];
    const content = contentOf(assembleContext(next, new WorkflowState(), new Map()));
    expect(content).toContain("  NEW OUTPUT");
    expect(content).not.toContain("OLD SEARCH OUTPUT");
    expect((content.match(/## Turn \d+: searchDocs/g) ?? []).length).toBe(1);
  });

  test("takes the docs output of the latest task only when an earlier one was left unfinished", () => {
    const steps = [
      step("m1", "user", 0, "stopped question", true),
      toolStep("m1", 1, [search("old", "STOPPED TASK OUTPUT")]),
      step("m2", "user", 0, "new question", true),
      toolStep("m2", 1, [search("new", "LATEST OUTPUT")]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain("  LATEST OUTPUT");
    expect(content).not.toContain("STOPPED TASK OUTPUT");
    expect((content.match(/# Documentation Read/g) ?? []).length).toBe(1);
  });

  test("matches an output to its call by id, not by position", () => {
    const stepWithSwappedResults: ReActStep = {
      ...step("m1", "agent", 1, "t", false),
      toolCalls: [
        { toolName: "searchDocs", toolCallId: "a", input: { query: "alpha" } },
        { toolName: "readDoc", toolCallId: "b", input: { path: "beta" } },
      ],
      toolResults: [
        { toolCallId: "b", output: "BETA PAGE" },
        { toolCallId: "a", output: "ALPHA HITS" },
      ],
    };
    const content = contentOf(
      assembleContext([step("m1", "user", 0, "q", true), stepWithSwappedResults], new WorkflowState(), new Map())
    );
    expect(content).toContain('## Turn 1: searchDocs {"query":"alpha"}\n  ALPHA HITS');
    expect(content).toContain('## Turn 1: readDoc {"path":"beta"}\n  BETA PAGE');
  });

  test("JSON-encodes an output that is not a string and shortens a very long input", () => {
    const steps = [step("m1", "user", 0, "q", true), toolStep("m1", 1, [search("q".repeat(500), { hits: 2 })])];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain('\n  {"hits":2}');
    const label = content.split("\n").find(l => l.startsWith("## Turn 1: searchDocs"))!;
    expect(label.length).toBeLessThan(260);
    expect(label.endsWith("…")).toBe(true);
  });

  test("keeps every output while the entries together cost exactly the budget", () => {
    const half = DOCS_CONTEXT_CHAR_LIMIT / 2;
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [read("a", sized(1, "a", half, "a"))]),
      toolStep("m1", 2, [read("b", sized(2, "b", half, "b"))]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).not.toContain("dropped to save space");
    expect(content).toContain(`${readLabel(1, "a")}\n  aaaa`);
    expect(content).toContain(`${readLabel(2, "b")}\n  bbbb`);
  });

  test("drops the oldest output when the entries cost one character more than the budget", () => {
    const half = DOCS_CONTEXT_CHAR_LIMIT / 2;
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [read("a", sized(1, "a", half, "a"))]),
      toolStep("m1", 2, [read("b", sized(2, "b", half + 1, "b"))]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(`oldest first.\n${dropped(1)}\n\n${readLabel(2, "b")}\n  bbbb`);
    expect(content).not.toContain(readLabel(1, "a"));
    expect(content).not.toContain("aaaa");
  });

  test("over the budget, keeps the newest outputs whole and sums up the older ones in one line", () => {
    const big = DOCS_CONTEXT_CHAR_LIMIT / 2;
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [read("a", sized(1, "a", big, "a"))]),
      toolStep("m1", 2, [read("b", sized(2, "b", big + 1, "b"))]),
      toolStep("m1", 3, [read("c", sized(3, "c", big, "c"))]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(`oldest first.\n${dropped(2)}\n\n${readLabel(3, "c")}\n  cccc`);
    expect((content.match(/dropped to save space/g) ?? []).length).toBe(1);
    expect(content).not.toContain(readLabel(1, "a"));
    expect(content).not.toContain(readLabel(2, "b"));
    expect(content).not.toContain("aaaa");
    expect(content).not.toContain("bbbb");
  });

  test("does not bring back an older output that would fit once a newer one was dropped", () => {
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [read("small", "tiny")]),
      toolStep("m1", 2, [read("huge", "h".repeat(DOCS_CONTEXT_CHAR_LIMIT))]),
      toolStep("m1", 3, [read("last", "newest")]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(dropped(2));
    expect(content).toContain("  newest");
    expect(content).not.toContain("tiny");
    expect(content).not.toContain("hhhh");
  });

  test("always keeps the newest output, even when it alone is over the budget", () => {
    const huge = "z".repeat(DOCS_CONTEXT_CHAR_LIMIT + 10);
    const steps = [step("m1", "user", 0, "q", true), toolStep("m1", 1, [read("huge", huge)])];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(huge);
    expect(content).not.toContain("dropped to save space");
  });

  test("keeps the whole section within the budget plus its two fixed lines, however many calls were made", () => {
    const bound = DOCS_CONTEXT_CHAR_LIMIT + 200;
    // 60 reads of a clipped section: about 4,200 characters over a hundred lines each.
    const page = Array.from({ length: 100 }, (_, i) => `line ${i} ${"t".repeat(34)}`).join("\n");
    const reads = [
      step("m1", "user", 0, "q", true),
      ...Array.from({ length: 60 }, (_, i) => toolStep("m1", i + 1, [read(`page-${i}`, page)])),
    ];
    const readSection = sectionOf(contentOf(assembleContext(reads, new WorkflowState(), new Map())));
    expect(readSection.length).toBeLessThanOrEqual(bound);
    expect(readSection).toContain("  line 99 ");

    // 60 searches without a hit, each with a long query and the outline as output.
    const outline = Array.from({ length: 26 }, (_, i) => `- area-${i}/ (12 pages): Area ${i} of the docs`).join("\n");
    const searches = [
      step("m1", "user", 0, "q", true),
      ...Array.from({ length: 60 }, (_, i) => toolStep("m1", i + 1, [search(`${i} ${"q".repeat(400)}`, outline)])),
    ];
    const searchSection = sectionOf(contentOf(assembleContext(searches, new WorkflowState(), new Map())));
    expect(searchSection.length).toBeLessThanOrEqual(bound);
    expect(searchSection).toMatch(
      /\(\d+ earlier outputs dropped to save space, call the tool again if you need them\)/
    );
  });

  test("counts the label and the indentation of an entry against the budget", () => {
    // Forty lines of five hundred characters: the output alone is under the budget, its entry is not.
    const lines = Array.from({ length: 40 }, () => "y".repeat(499)).join("\n");
    expect(lines.length).toBeLessThan(DOCS_CONTEXT_CHAR_LIMIT);
    expect(cost(readLabel(2, "b"), lines) + cost(readLabel(1, "a"), "small")).toBeGreaterThan(DOCS_CONTEXT_CHAR_LIMIT);
    const steps = [
      step("m1", "user", 0, "q", true),
      toolStep("m1", 1, [read("a", "small")]),
      toolStep("m1", 2, [read("b", lines)]),
    ];
    const content = contentOf(assembleContext(steps, new WorkflowState(), new Map()));
    expect(content).toContain(dropped(1));
    expect(content).not.toContain("  small");
  });
});

describe("assembleContext — dataflow serialization", () => {
  test("orders operators and links topologically rather than by insertion order", () => {
    const workflowState = new WorkflowState();
    // Inserted leaf-first, so insertion order is the reverse of the dataflow order.
    workflowState.addOperator(makeOperator("c"));
    workflowState.addOperator(makeOperator("b"));
    workflowState.addOperator(makeOperator("a"));
    workflowState.addLink(makeLink("b", "output-0", "c", "input-0"));
    workflowState.addLink(makeLink("a", "output-0", "c", "input-1"));
    workflowState.addLink(makeLink("a", "output-0", "b", "input-0"));

    const content = contentOf(assembleContext([], workflowState, new Map()));

    expect(content.indexOf("### Operator `a`")).toBeLessThan(content.indexOf("### Operator `b`"));
    expect(content.indexOf("### Operator `b`")).toBeLessThan(content.indexOf("### Operator `c`"));
    // Links are sorted by source rank, then by target rank — a → b precedes a → c.
    const linkLines = content.slice(content.indexOf("## Links")).split("\n").slice(1);
    expect(linkLines).toEqual(["- a → b", "- a → c", "- b → c"]);
  });

  test("renders input schemas by port ordinal and falls through to the first defined output schema", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("src", { outputPorts: [{ portID: "output-0" }] }));
    workflowState.addOperator(makeOperator("dst", { inputPorts: [{ portID: "input-0" }] }));
    workflowState.addLink(makeLink("src", "output-0", "dst", "input-0"));

    const compilationResult: WorkflowCompilationResponse = {
      operatorOutputSchemas: {
        src: {
          "0_false": [
            { attributeName: "a", attributeType: "string" },
            { attributeName: "n", attributeType: "integer" },
          ],
        },
        // Port 0 carries no schema, so the output line must come from port 1.
        dst: { "0_false": undefined, "1_false": [{ attributeName: "flag", attributeType: "boolean" }] },
      },
      operatorErrors: {},
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Input Schema (port 0): [a: string, n: integer]");
    expect(content).toContain("Output Schema: [a: string, n: integer]");
    expect(content).toContain("Output Schema: [flag: boolean]");
    // The wire port id "0_false" is trimmed down to the bare ordinal.
    expect(content).not.toContain("port 0_false");
  });

  test("renders non-empty properties and JSON-encodes non-string values", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(
      makeOperator("op1", {
        operatorProperties: { name: "alice", limit: 5, tags: ["x", "y"], blank: "", missing: null, absent: undefined },
      })
    );

    const content = contentOf(assembleContext([], workflowState, new Map()));

    expect(content).toContain("Summary: op1"); // no display name -> falls back to the id
    expect(content).toContain("  name: alice");
    expect(content).toContain("  limit: 5");
    expect(content).toContain('  tags: ["x","y"]');
    expect(content).not.toContain("blank");
    expect(content).not.toContain("missing");
    expect(content).not.toContain("absent");
  });

  test("omits properties under redaction unless the operator's result reports an error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { operatorProperties: { secret: "s3cr3t" } }));

    const redacted = contentOf(assembleContext([], workflowState, new Map(), true));
    expect(redacted).toContain("(TestOp, not-executed)");
    expect(redacted).not.toContain("Properties:");

    // A failing operator keeps its properties even when redacting, so the model
    // can see what caused the failure.
    const failed = contentOf(assembleContext([], workflowState, new Map([["op1", "[ERROR] boom"]]), true));
    expect(failed).toContain("(TestOp, failed)");
    expect(failed).toContain("  secret: s3cr3t");
  });

  test("marks an operator executed and indents every line of its result", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1", { customDisplayName: "My Filter" }));

    const content = contentOf(assembleContext([], workflowState, new Map([["op1", "header\nrow1"]])));

    expect(content).toContain("### Operator `op1` (TestOp, executed)");
    expect(content).toContain("Summary: My Filter");
    expect(content).toContain("Result:\n  header\n  row1");
  });

  test("surfaces a per-operator compilation error", () => {
    const workflowState = new WorkflowState();
    workflowState.addOperator(makeOperator("op1"));
    const compilationResult: WorkflowCompilationResponse = {
      operatorOutputSchemas: {},
      operatorErrors: {
        op1: {
          type: { name: WorkflowFatalErrorType.COMPILATION_ERROR },
          timestamp: { seconds: 0, nanos: 0 },
          message: "attribute x not found",
          details: "",
          operatorId: "op1",
          workerId: "",
        },
      },
    };

    const content = contentOf(assembleContext([], workflowState, new Map(), false, compilationResult));

    expect(content).toContain("Compilation Error: attribute x not found");
  });
});

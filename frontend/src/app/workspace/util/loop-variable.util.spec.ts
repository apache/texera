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

import { extractLoopVariables, loopVariablesInScope } from "./loop-variable.util";
import { CONTROL_VARIABLE_PORT_ID } from "../service/workflow-graph/model/control-variable-port";
import { WorkflowGraphReadonly } from "../service/workflow-graph/model/workflow-graph";

describe("extractLoopVariables", () => {
  it("reads a single assignment, the LoopStart default", () => {
    expect(extractLoopVariables("i = 0")).toEqual(["i"]);
  });

  it("reads several statements on one line separated by semicolons", () => {
    expect(extractLoopVariables("K = 2; prev = float('-inf')")).toEqual(["K", "prev"]);
  });

  it("reads several statements on several lines, ignoring blank lines", () => {
    expect(extractLoopVariables("K = 2\n\nprev = float('-inf')\n")).toEqual(["K", "prev"]);
  });

  it("excludes augmented assignments", () => {
    for (const code of ["x += 1", "x -= 1", "x *= 2", "x /= 2", "x //= 2", "x **= 2", "x %= 2", "x >>= 1", "x <<= 1"]) {
      expect(extractLoopVariables(code), code).toEqual([]);
    }
  });

  it("excludes comparisons", () => {
    for (const code of ["a == b", "a != b", "a <= b", "a >= b"]) {
      expect(extractLoopVariables(code), code).toEqual([]);
    }
  });

  it("reads every name of a tuple target, with or without parentheses", () => {
    expect(extractLoopVariables("a, b = 1, 2")).toEqual(["a", "b"]);
    expect(extractLoopVariables("(a, b) = (1, 2)")).toEqual(["a", "b"]);
    expect(extractLoopVariables("a, *rest = xs")).toEqual(["a", "rest"]);
  });

  it("reads every target of a chained assignment", () => {
    expect(extractLoopVariables("a = b = 0")).toEqual(["a", "b"]);
  });

  it("reads an annotated assignment", () => {
    expect(extractLoopVariables("x: int = 0")).toEqual(["x"]);
  });

  it("returns each name once, in first-assignment order", () => {
    expect(extractLoopVariables("i = 0\nj = 1\ni = 2")).toEqual(["i", "j"]);
  });

  it("is not fooled by separators and equals signs inside string literals", () => {
    expect(extractLoopVariables("s = 'a;b=c'")).toEqual(["s"]);
    expect(extractLoopVariables('s = "x = 1; y = 2"')).toEqual(["s"]);
  });

  it("is not fooled by separators inside brackets", () => {
    expect(extractLoopVariables("d = {'a': 1, 'b': 2}; k = 0")).toEqual(["d", "k"]);
    expect(extractLoopVariables("x = (1 +\n     2)")).toEqual(["x"]);
  });

  it("excludes attribute and subscript targets", () => {
    expect(extractLoopVariables("self.x = 1")).toEqual([]);
    expect(extractLoopVariables("d['k'] = 1")).toEqual([]);
    // a mixed target still yields the plain name
    expect(extractLoopVariables("a[0], b = 1, 2")).toEqual(["b"]);
  });

  it("excludes keyword arguments and default parameters", () => {
    expect(extractLoopVariables("f(k=1)")).toEqual([]);
    expect(extractLoopVariables("g = lambda k=1: k")).toEqual(["g"]);
  });

  // Python has no block scope: the exec'd initialization binds, as globals of the state, whatever an
  // if/else, try/except, for or while body assigns, so those are loop variables too.
  it("reads assignments under a top-level if/else, try/except and while", () => {
    expect(extractLoopVariables("if flag:\n    K = 2\nelse:\n    K = 5")).toEqual(["K"]);
    expect(extractLoopVariables("try:\n    K = int(cfg)\nexcept Exception:\n    K = 2")).toEqual(["K"]);
    expect(extractLoopVariables("while n > 0:\n    n = n - 1\n    last = n")).toEqual(["n", "last"]);
    expect(extractLoopVariables("if a:\n    if b:\n        deep = 1")).toEqual(["deep"]);
  });

  it("reads the targets of a for loop and the assignments in its body", () => {
    expect(extractLoopVariables("for k in range(3):\n    pass")).toEqual(["k"]);
    expect(extractLoopVariables("for i in range(3):\n    total = i")).toEqual(["i", "total"]);
    expect(extractLoopVariables("for i, (a, b) in enumerate(pairs):\n    pass")).toEqual(["i", "a", "b"]);
    expect(extractLoopVariables("for K in [3]: pass")).toEqual(["K"]);
  });

  it("reads the one-line body of a compound statement, not its keyword", () => {
    expect(extractLoopVariables("try: k = 1\nexcept: pass")).toEqual(["k"]);
    expect(extractLoopVariables("if x: y = 2\nelse: z = 3")).toEqual(["y", "z"]);
    expect(extractLoopVariables("if s == 'a:b': y = 1; w = 2")).toEqual(["y", "w"]);
    expect(extractLoopVariables("finally: f = 0")).toEqual(["f"]);
  });

  it("excludes what a def or class body assigns, which is local to it", () => {
    expect(extractLoopVariables("def f():\n    local = 1\n    return local\nK = 2")).toEqual(["K"]);
    expect(extractLoopVariables("def f(): local = 1; other = 2\nK = 2")).toEqual(["K"]);
    expect(extractLoopVariables("class C:\n    attr = 1\nK = 2")).toEqual(["K"]);
    expect(extractLoopVariables("if a:\n    def f():\n        local = 1\n    K = 2")).toEqual(["K"]);
  });

  it("excludes the names of an except clause, a with statement, an import, a def and a class", () => {
    // none of these is a value the loop state can carry (State.to_json raises on a module, a function or
    // a handle), and `except ... as e` unbinds e when the handler ends
    expect(extractLoopVariables("try:\n    pass\nexcept Exception as e:\n    pass")).toEqual([]);
    expect(extractLoopVariables("with open(p) as fh:\n    pass")).toEqual([]);
    expect(extractLoopVariables("import numpy as np\nfrom math import pi")).toEqual([]);
    expect(extractLoopVariables("def f():\n    pass\nclass C:\n    pass")).toEqual([]);
  });

  it("never takes a Python keyword for a name", () => {
    expect(extractLoopVariables("else = 1")).toEqual([]);
    expect(extractLoopVariables("True, k = 1, 2")).toEqual(["k"]);
  });

  it("ignores comments", () => {
    expect(extractLoopVariables("# k = 1")).toEqual([]);
    expect(extractLoopVariables("k = 1  # comment with x = 2")).toEqual(["k"]);
  });

  it("takes the first top-level equals sign as the assignment", () => {
    expect(extractLoopVariables("x = y == 1")).toEqual(["x"]);
  });

  it("accepts the identifier grammar of a $reference and nothing wider", () => {
    expect(extractLoopVariables("_k1 = 1")).toEqual(["_k1"]);
    expect(extractLoopVariables("1k = 1")).toEqual([]);
    // a $reference can only spell ASCII names, so a non-ASCII target is not offered
    expect(extractLoopVariables("é = 1")).toEqual([]);
  });

  it("returns nothing for empty, whitespace-only or non-assignment code", () => {
    expect(extractLoopVariables("")).toEqual([]);
    expect(extractLoopVariables("   \n\t\n")).toEqual([]);
    expect(extractLoopVariables("print(i)")).toEqual([]);
  });
});

describe("loopVariablesInScope", () => {
  const graphOf = (
    enclosing: string[],
    initializations: Record<string, unknown>,
    outputs: Record<string, unknown> = {}
  ): WorkflowGraphReadonly =>
    ({
      getEnclosingLoopStarts: () => enclosing,
      getOperator: (operatorID: string) => ({
        operatorProperties: { initialization: initializations[operatorID], output: outputs[operatorID] },
      }),
    }) as unknown as WorkflowGraphReadonly;

  it("collects the variables of every enclosing LoopStart, outermost first, without duplicates", () => {
    const graph = graphOf(["outer", "inner"], { outer: "K = 2; i = 0", inner: "j = 0; K = 5" });
    expect(loopVariablesInScope(graph, "body")).toEqual(["K", "i", "j"]);
  });

  it("returns undefined for an operator outside every block, telling it from a block that declares nothing", () => {
    expect(loopVariablesInScope(graphOf([], {}), "lonely")).toBeUndefined();
    expect(loopVariablesInScope(graphOf(["s"], { s: "print(1)" }), "body")).toEqual([]);
  });

  it("tolerates a LoopStart whose initialization is missing or not a string", () => {
    const graph = graphOf(["s1", "s2", "s3"], { s1: undefined, s2: 42, s3: "k = 1" });
    expect(loopVariablesInScope(graph, "body")).toEqual(["k"]);
  });

  // Output is a Python cell that runs each iteration before the Loop Start emits its variables, and
  // what it assigns joins them, so the block sees those names as it sees the Initialization's.
  it("adds the names a LoopStart's output assigns after those of its initialization, without duplicates", () => {
    const scope = (initialization: string, output: string) =>
      loopVariablesInScope(graphOf(["s"], { s: initialization }, { s: output }), "body");
    expect(scope("i = 0", "file = D.line[i]")).toEqual(["i", "file"]);
    // statements, then the data the body gets
    expect(scope("i = 0", "file = D.line[i]; D.iloc[[i]]")).toEqual(["i", "file"]);
    // a name both assign is offered once, where the initialization first binds it
    expect(scope("i = 0; file = ''", "file = D.line[i]\ni = i")).toEqual(["i", "file"]);
  });

  it("never offers D or table, which the runtime reserves for the input table", () => {
    expect(loopVariablesInScope(graphOf(["s"], { s: "i = 0" }, { s: "D = D.head(1)" }), "body")).toEqual(["i"]);
    expect(loopVariablesInScope(graphOf(["s"], { s: "i = 0; table = 1" }, { s: "None" }), "body")).toEqual(["i"]);
  });

  it("takes no names from an output that is a single expression, as every output was before", () => {
    for (const output of [
      "table.iloc[i]",
      "table.iloc[[i]]",
      "D.iloc[[i]]",
      "None",
      "D[D.line == D.line[i]]",
      "table if i >= 0 else None",
    ]) {
      expect(loopVariablesInScope(graphOf(["s"], { s: "i = 0" }, { s: output }), "body"), output).toEqual(["i"]);
    }
  });

  it("does not count what a def, lambda or comprehension in the output assigns", () => {
    const output = [
      "def pick(row):",
      "    chosen = row.line",
      "    return chosen",
      "file = pick(D.iloc[i])",
      "longest = max(D.line, key=lambda s: len(s))",
      "first = [x for x in D.line][0]",
      "D.iloc[[j for j in range(i + 1)]]",
    ].join("\n");
    expect(loopVariablesInScope(graphOf(["s"], { s: "i = 0" }, { s: output }), "body")).toEqual([
      "i",
      "file",
      "longest",
      "first",
    ]);
  });

  it("keeps each LoopStart's initialization and output names together, outermost first", () => {
    const graph = graphOf(
      ["outer", "inner"],
      { outer: "i = 0", inner: "j = 0" },
      { outer: "file = D.line[i]", inner: "row = D.iloc[[j]]; D.iloc[[j]]" }
    );
    expect(loopVariablesInScope(graph, "body")).toEqual(["i", "file", "j", "row"]);
  });

  it("tolerates a LoopStart whose output is missing or not a string", () => {
    const graph = graphOf(["s1", "s2"], { s1: "i = 0", s2: "j = 0" }, { s1: undefined, s2: 42 });
    expect(loopVariablesInScope(graph, "body")).toEqual(["i", "j"]);
  });
});

describe("loopVariablesInScope with a control-variable port", () => {
  const link = (source: string, target: string, targetPort: string) => ({
    linkID: `${source}-${target}`,
    source: { operatorID: source, portID: "output-0" },
    target: { operatorID: target, portID: targetPort },
  });
  const graphOf = (
    enclosing: string[],
    operators: Record<string, { operatorType: string; initialization?: string; output?: string }>,
    links: ReturnType<typeof link>[]
  ): WorkflowGraphReadonly =>
    ({
      getEnclosingLoopStarts: () => enclosing,
      getOperator: (operatorID: string) => ({
        operatorType: operators[operatorID]?.operatorType,
        operatorProperties: {
          initialization: operators[operatorID]?.initialization,
          output: operators[operatorID]?.output,
        },
      }),
      getInputLinksByOperatorId: (operatorID: string) => links.filter(l => l.target.operatorID === operatorID),
    }) as unknown as WorkflowGraphReadonly;

  it("offers the variables of a LoopStart that feeds the port, even outside every block", () => {
    const graph = graphOf(
      [],
      {
        start: { operatorType: "LoopStart", initialization: "i = 0; file = D.path[0]" },
        reader: { operatorType: "FileScan" },
      },
      [link("start", "reader", CONTROL_VARIABLE_PORT_ID)]
    );
    expect(loopVariablesInScope(graph, "reader")).toEqual(["i", "file"]);
  });

  it("offers the names the output of a LoopStart that feeds the port assigns", () => {
    // the files example: each iteration's output picks the name of the file the reader reads
    const graphWithOutput = (output: string) =>
      graphOf(
        [],
        {
          start: { operatorType: "LoopStart", initialization: "i = 0", output },
          reader: { operatorType: "FileScan" },
        },
        [link("start", "reader", CONTROL_VARIABLE_PORT_ID)]
      );
    expect(loopVariablesInScope(graphWithOutput("file = D.line[i]"), "reader")).toEqual(["i", "file"]);
    expect(loopVariablesInScope(graphWithOutput("file = D.line[i]; D.iloc[[i]]"), "reader")).toEqual(["i", "file"]);
    expect(loopVariablesInScope(graphWithOutput("D.iloc[[i]]"), "reader")).toEqual(["i"]);
  });

  it("offers the output names of both an enclosing LoopStart and one that feeds the port", () => {
    const graph = graphOf(
      ["outer"],
      {
        outer: { operatorType: "LoopStart", initialization: "i = 0", output: "file = D.line[i]; D.iloc[[i]]" },
        start: { operatorType: "LoopStart", initialization: "j = 0", output: "row = D.iloc[[j]]" },
      },
      [link("start", "reader", CONTROL_VARIABLE_PORT_ID)]
    );
    expect(loopVariablesInScope(graph, "reader")).toEqual(["i", "file", "j", "row"]);
  });

  it("offers the columns of any other operator that feeds the port", () => {
    const graph = graphOf(
      ["start"],
      { start: { operatorType: "LoopStart", initialization: "i = 0" }, stats: { operatorType: "Aggregate" } },
      [link("stats", "filter", CONTROL_VARIABLE_PORT_ID)]
    );
    const columns = (operatorID: string) => (operatorID === "stats" ? ["mean", "not a name"] : undefined);
    expect(loopVariablesInScope(graph, "filter", columns)).toEqual(["i", "mean"]);
  });

  it("ignores a link into a data port", () => {
    const graph = graphOf([], { start: { operatorType: "LoopStart", initialization: "i = 0" } }, [
      link("start", "reader", "input-0"),
    ]);
    expect(loopVariablesInScope(graph, "reader")).toBeUndefined();
  });
});

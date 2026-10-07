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
  const graphOf = (enclosing: string[], initializations: Record<string, unknown>): WorkflowGraphReadonly =>
    ({
      getEnclosingLoopStarts: () => enclosing,
      getOperator: (operatorID: string) => ({ operatorProperties: { initialization: initializations[operatorID] } }),
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
});

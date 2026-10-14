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

import { Subscription } from "rxjs";
import { extractLoopVariables, loopScopeChangeStream, loopVariablesInScope } from "./loop-variable.util";
import { unknownReferenceWarning } from "./loop-variable-field.util";
import { WorkflowGraph } from "../service/workflow-graph/model/workflow-graph";

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

  it("excludes a lambda's default parameters wherever they sit in its parameter list", () => {
    expect(extractLoopVariables("pick = lambda row, col='line': row[col]")).toEqual(["pick"]);
    expect(extractLoopVariables("i = 0\nkey = lambda s, n=1: s[:n]")).toEqual(["i", "key"]);
    expect(extractLoopVariables("f = lambda x=1: x")).toEqual(["f"]);
    expect(extractLoopVariables("g = lambda: 0")).toEqual(["g"]);
    expect(extractLoopVariables("longest = max(names, key=lambda s, n=1: len(s))")).toEqual(["longest"]);
    expect(extractLoopVariables("a = b = lambda x, y=2: x")).toEqual(["a", "b"]);
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

  it("excludes the names of an except clause, a with statement, a def and a class", () => {
    // none of these is a value the loop state can carry (State.to_json raises on a function or a handle),
    // and `except ... as e` unbinds e when the handler ends
    expect(extractLoopVariables("try:\n    pass\nexcept Exception as e:\n    pass")).toEqual([]);
    expect(extractLoopVariables("with open(p) as fh:\n    pass")).toEqual([]);
    expect(extractLoopVariables("def f():\n    pass\nclass C:\n    pass")).toEqual([]);
  });

  // exec puts what an import binds into the globals it keeps as the loop state, like an assignment, so
  // `from math import pi` gives the loop a variable `pi`
  it("reads the names a from-import binds, taking an alias instead of the name it renames", () => {
    expect(extractLoopVariables("from math import pi")).toEqual(["pi"]);
    expect(extractLoopVariables("from m import a, b as c")).toEqual(["a", "c"]);
    expect(extractLoopVariables("from . import x")).toEqual(["x"]);
    expect(extractLoopVariables("from ..pkg.mod import x")).toEqual(["x"]);
  });

  it("reads a parenthesized from-import spread over several lines", () => {
    expect(extractLoopVariables("from m import (a,\n    b)")).toEqual(["a", "b"]);
    expect(extractLoopVariables("from m import (\n    a,  # first\n    b as c,\n)\nk = 0")).toEqual(["a", "c", "k"]);
  });

  it("reads the name an import binds: the module, the first part of a dotted one, or the alias", () => {
    expect(extractLoopVariables("import m")).toEqual(["m"]);
    expect(extractLoopVariables("import a.b.c")).toEqual(["a"]);
    expect(extractLoopVariables("import m as n")).toEqual(["n"]);
    expect(extractLoopVariables("import a.b as n")).toEqual(["n"]);
    expect(extractLoopVariables("import os, numpy as np")).toEqual(["os", "np"]);
  });

  it("reads several imports on one line separated by semicolons, among assignments", () => {
    expect(extractLoopVariables("import m; from math import pi; k = 0")).toEqual(["m", "pi", "k"]);
  });

  it("knows no name a star import binds", () => {
    expect(extractLoopVariables("from m import *")).toEqual([]);
    expect(extractLoopVariables("from m import *; k = 0")).toEqual(["k"]);
  });

  it("reads an import under a compound statement, but not one in a def body", () => {
    expect(extractLoopVariables("try:\n    import numpy as np\nexcept ImportError:\n    pass")).toEqual(["np"]);
    expect(extractLoopVariables("try: from math import tau\nexcept: pass")).toEqual(["tau"]);
    expect(extractLoopVariables("def f():\n    from math import pi\n    return pi")).toEqual([]);
  });

  it("does not take a name that merely starts with from or import for an import", () => {
    expect(extractLoopVariables("from_ = 1; import_x = 2")).toEqual(["from_", "import_x"]);
    expect(extractLoopVariables("from importlib import reload")).toEqual(["reload"]);
  });

  it("does not offer an imported name a $reference cannot spell", () => {
    expect(extractLoopVariables("from m import é")).toEqual([]);
    expect(extractLoopVariables("import m as é")).toEqual([]);
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
  let graph: WorkflowGraph;

  afterEach(() => graph.destroyYModel());

  /** Loop Starts, Loop Ends and plain operators as "id:LoopStart=initialization", "id:LoopEnd" and "id". */
  function graphOf(operators: string[], links: string[]): WorkflowGraph {
    return new WorkflowGraph(
      operators.map(spec => {
        const equals = spec.indexOf("=");
        const head = equals === -1 ? spec : spec.slice(0, equals);
        const [operatorID, operatorType = "Limit"] = head.split(":");
        return operator(operatorID, operatorType, equals === -1 ? {} : { initialization: spec.slice(equals + 1) });
      }),
      links.map(spec => link(...(spec.split(">") as [string, string])))
    );
  }

  it("collects the variables of every Loop Start around the operator, outermost first, without duplicates", () => {
    // the ids sort against the nesting: the order follows the blocks
    graph = graphOf(
      [
        "z-outer:LoopStart=K = 2; i = 0",
        "a-inner:LoopStart=j = 0; K = 5",
        "body",
        "inner-end:LoopEnd",
        "outer-end:LoopEnd",
      ],
      ["z-outer>a-inner", "a-inner>body", "body>inner-end", "inner-end>outer-end"]
    );
    expect(loopVariablesInScope(graph, "body")).toEqual(["K", "i", "j"]);
    // an inner block's Loop Start is inside the outer block, not its own
    expect(loopVariablesInScope(graph, "a-inner")).toEqual(["K", "i"]);
  });

  it("returns undefined for an operator outside every block, telling it from a block that declares nothing", () => {
    graph = graphOf(["s:LoopStart=print(1)", "body", "e:LoopEnd", "after"], ["s>body", "body>e", "e>after"]);
    expect(loopVariablesInScope(graph, "body")).toEqual([]);
    expect(loopVariablesInScope(graph, "after")).toBeUndefined();
    expect(loopVariablesInScope(graph, "s")).toBeUndefined();
    expect(loopVariablesInScope(graph, "e")).toBeUndefined();
    expect(loopVariablesInScope(graph, "not-in-the-graph")).toBeUndefined();
  });

  it("counts no block that a Loop End does not close yet", () => {
    graph = graphOf(["s:LoopStart=k = 1", "body"], ["s>body"]);
    expect(loopVariablesInScope(graph, "body")).toBeUndefined();
  });

  it("makes a name a LoopStart imports a valid reference", () => {
    graph = graphOf(["s:LoopStart=from math import pi", "body", "e:LoopEnd"], ["s>body", "body>e"]);
    const names = loopVariablesInScope(graph, "body");
    expect(names).toEqual(["pi"]);
    expect(unknownReferenceWarning(names ?? [])("$pi")).toBeUndefined();
  });

  it("tolerates a LoopStart whose initialization is missing or not a string", () => {
    graph = new WorkflowGraph(
      [
        operator("s1", "LoopStart", {}),
        operator("s2", "LoopStart", { initialization: 42 }),
        operator("s3", "LoopStart", { initialization: "k = 1" }),
        operator("body", "Limit", {}),
        operator("e3", "LoopEnd", {}),
        operator("e2", "LoopEnd", {}),
        operator("e1", "LoopEnd", {}),
      ],
      [link("s1", "s2"), link("s2", "s3"), link("s3", "body"), link("body", "e3"), link("e3", "e2"), link("e2", "e1")]
    );
    expect(loopVariablesInScope(graph, "body")).toEqual(["k"]);
  });

  // The backend compiles and runs the enabled operators and the links between them, valid or not
  // (WorkflowGraph.getSubDAG): a disabled operator, and every link touching one, is in no plan it sees.
  it("leaves out a disabled operator and every link touching one, as the plan the backend runs does", () => {
    graph = graphOf(["s:LoopStart=k = 1", "first", "second", "e:LoopEnd"], ["s>first", "first>second", "second>e"]);
    graph.disableOperator("first");
    expect(loopVariablesInScope(graph, "first")).toBeUndefined();
    expect(loopVariablesInScope(graph, "second")).toBeUndefined();

    // a disabled Loop End closes no block either
    graph.enableOperator("first");
    expect(loopVariablesInScope(graph, "second")).toEqual(["k"]);
    graph.disableOperator("e");
    expect(loopVariablesInScope(graph, "first")).toBeUndefined();
    expect(loopVariablesInScope(graph, "second")).toBeUndefined();
  });

  it("keeps a branch of the block that no disabled operator sits on inside it", () => {
    graph = graphOf(["s:LoopStart=k = 1", "a", "b", "e:LoopEnd"], ["s>a", "a>e", "s>b", "b>e"]);
    graph.disableOperator("b");
    expect(loopVariablesInScope(graph, "a")).toEqual(["k"]);
    expect(loopVariablesInScope(graph, "b")).toBeUndefined();
  });
});

describe("loopScopeChangeStream", () => {
  let graph: WorkflowGraph;
  let emissions: number;
  let subscription: Subscription;

  beforeEach(() => {
    graph = new WorkflowGraph();
    emissions = 0;
    subscription = loopScopeChangeStream(graph).subscribe(() => emissions++);
  });

  afterEach(() => {
    subscription.unsubscribe();
    graph.destroyYModel();
  });

  it("emits on an operator or a link added or deleted, which can move operators into or out of a block", () => {
    graph.operatorAddSubject.next(operator("a", "Limit", {}));
    graph.operatorDeleteSubject.next({ deletedOperatorID: "a" });
    graph.linkAddSubject.next(link("a", "b"));
    graph.linkDeleteSubject.next({ deletedLink: link("a", "b") });
    expect(emissions).toBe(4);
  });

  it("emits on an operator disabled or enabled, which takes it and its links out of the plan or puts them back", () => {
    graph.disabledOperatorChangedSubject.next({ newDisabled: ["a"], newEnabled: [] });
    graph.disabledOperatorChangedSubject.next({ newDisabled: [], newEnabled: ["a"] });
    expect(emissions).toBe(2);
  });

  it("emits on a change to a Loop Start's properties, which can change the variables it declares", () => {
    graph.operatorPropertyChangeSubject.next({ operator: operator("s", "LoopStart", { initialization: "k = 1" }) });
    expect(emissions).toBe(1);
  });

  it("does not emit on a change to any other operator's properties, such as a value being typed", () => {
    graph.operatorPropertyChangeSubject.next({ operator: operator("a", "Limit", { limit: "$" }) });
    graph.operatorPropertyChangeSubject.next({ operator: operator("e", "LoopEnd", { condition: "k < 3" }) });
    expect(emissions).toBe(0);
  });
});

/** An operator with one output port and two input ports. */
function operator(operatorID: string, operatorType: string, operatorProperties: Record<string, unknown>) {
  return {
    operatorID,
    operatorType,
    operatorVersion: "v1",
    operatorProperties,
    inputPorts: [{ portID: "input-0" }, { portID: "input-1" }],
    outputPorts: [{ portID: "output-0" }],
    showAdvanced: false,
    isDisabled: false,
  };
}

function link(from: string, to: string) {
  return {
    linkID: `${from}->${to}`,
    source: { operatorID: from, portID: "output-0" },
    target: { operatorID: to, portID: "input-0" },
  };
}

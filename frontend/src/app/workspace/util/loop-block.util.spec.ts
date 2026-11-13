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

// The backend's LogicalPlanSpec checks LogicalPlan.enclosingLoopStarts against the same file.
import loopBlocks from "../../../../../common/workflow-compiler/src/test/resources/loop-blocks.json";
import { enclosingLoopStarts, LOOP_END_OP_TYPE, LOOP_START_OP_TYPE } from "./loop-block.util";

/** A case of loop-blocks.json: a graph, and the Loop Starts around each operator inside a block. */
interface LoopBlockCase {
  name: string;
  operators: ReadonlyArray<{ id: string; type: string }>;
  links: ReadonlyArray<{ from: string; to: string }>;
  enclosingLoopStarts: Partial<Record<string, ReadonlyArray<string>>>;
}

/** The operators and links given as "id:type" (a plain operator by its id alone) and "from>to". */
function graph(operators: string[], links: string[]) {
  return {
    operators: operators.map(operator => {
      const [operatorID, operatorType = "Operator"] = operator.split(":");
      return { operatorID, operatorType };
    }),
    links: links.map(link => {
      const [from, to] = link.split(">");
      return { source: { operatorID: from }, target: { operatorID: to } };
    }),
  };
}

/** What enclosingLoopStarts finds in a graph, as a plain object. */
function enclosing({ operators, links }: ReturnType<typeof graph>): Record<string, ReadonlyArray<string>> {
  return Object.fromEntries(enclosingLoopStarts(operators, links));
}

/** What enclosingLoopStarts finds in a case's graph, as the fixture writes it. */
function answerOf(testCase: LoopBlockCase): Partial<Record<string, ReadonlyArray<string>>> {
  return enclosing(
    graph(
      testCase.operators.map(({ id, type }) => `${id}:${type}`),
      testCase.links.map(({ from, to }) => `${from}>${to}`)
    )
  );
}

describe("enclosingLoopStarts", () => {
  describe("on the cases the backend's LogicalPlanSpec runs too (loop-blocks.json)", () => {
    const cases: ReadonlyArray<LoopBlockCase> = loopBlocks.cases;
    const randomGraphs: ReadonlyArray<LoopBlockCase> = loopBlocks.randomGraphs;

    it("has the cases to run", () => {
      expect(cases.length).toBeGreaterThan(20);
      expect(randomGraphs.length).toBeGreaterThan(50);
    });

    for (const testCase of cases) {
      it(testCase.name, () => {
        expect(answerOf(testCase)).toEqual(testCase.enclosingLoopStarts);
      });
    }

    it("gives the backend's answer on every random graph", () => {
      for (const testCase of randomGraphs) {
        expect(answerOf(testCase), testCase.name).toEqual(testCase.enclosingLoopStarts);
      }
    });
  });

  // LogicalPlanSpec reads the fixture's Loop Start and Loop End through the backend's JSON type names, so
  // the port names them as the operators on the canvas carry them.
  it("knows every operator type the fixture names", () => {
    const types = new Set(
      [...loopBlocks.cases, ...loopBlocks.randomGraphs].flatMap(testCase => testCase.operators.map(({ type }) => type))
    );
    expect([...types].sort()).toEqual([LOOP_END_OP_TYPE, LOOP_START_OP_TYPE, "Operator"].sort());
  });

  it("finds nothing in an empty graph", () => {
    expect(enclosing(graph([], []))).toEqual({});
  });

  // The backend rejects a cycle; the canvas does not stop one from being drawn.
  describe("on a graph with a cycle, which the backend never compiles", () => {
    it("counts every operator on a cycle through a block outside every block, and ends", () => {
      expect(enclosing(graph(["S:LoopStart", "a", "E:LoopEnd"], ["S>a", "a>E", "E>S"]))).toEqual({});
    });

    it("counts the operators after a cycle outside every block too", () => {
      const cycleIntoBlock = graph(["x", "y", "S:LoopStart", "a", "E:LoopEnd"], ["x>y", "y>x", "y>S", "S>a", "a>E"]);
      expect(enclosing(cycleIntoBlock)).toEqual({});
    });

    it("leaves a block that no cycle reaches as it is", () => {
      const besideBlock = graph(["S:LoopStart", "a", "E:LoopEnd", "x", "y"], ["S>a", "a>E", "x>y", "y>x"]);
      expect(enclosing(besideBlock)).toEqual({ a: ["S"] });
      // a cycle hanging off the block's body is no path to the Loop End
      const offBody = graph(["S:LoopStart", "a", "E:LoopEnd", "x", "y"], ["S>a", "a>E", "a>x", "x>y", "y>x"]);
      expect(enclosing(offBody)).toEqual({ a: ["S"] });
    });

    it("ends on an operator linked to itself", () => {
      expect(enclosing(graph(["S:LoopStart", "a", "E:LoopEnd"], ["S>a", "a>a", "a>E"]))).toEqual({});
    });
  });

  it("ignores a link to or from an operator it was not given", () => {
    const dangling = graph(["S:LoopStart", "a", "E:LoopEnd"], ["S>a", "a>E", "gone>a", "a>gone"]);
    expect(enclosing(dangling)).toEqual({ a: ["S"] });
  });

  it("reads every operator type other than Loop Start and Loop End as a plain operator", () => {
    expect(enclosing(graph(["S:LoopStart", "a:Limit", "b:PythonUDFV2", "E:LoopEnd"], ["S>a", "a>b", "b>E"]))).toEqual({
      a: ["S"],
      b: ["S"],
    });
  });
});

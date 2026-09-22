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

import { getEnclosingLoopStarts, LOOP_END_OP_TYPE, LOOP_START_OP_TYPE } from "./loop-block.util";

/** A minimal operator for the block walk: only the id and the type matter. */
const op = (operatorID: string, operatorType = "Filter") => ({ operatorID, operatorType });
const loopStart = (operatorID: string) => op(operatorID, LOOP_START_OP_TYPE);
const loopEnd = (operatorID: string) => op(operatorID, LOOP_END_OP_TYPE);
/** A link from one operator's first output port to another's first input port. */
const link = (from: string, to: string) => ({
  source: { operatorID: from, portID: "output-0" },
  target: { operatorID: to, portID: "input-0" },
});

describe("getEnclosingLoopStarts", () => {
  describe("on a straight line  src -> S -> a -> b -> E -> sink", () => {
    const operators = [op("src"), loopStart("S"), op("a"), op("b"), loopEnd("E"), op("sink")];
    const links = [link("src", "S"), link("S", "a"), link("a", "b"), link("b", "E"), link("E", "sink")];

    it("puts every operator between the start and the end inside the block", () => {
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
      expect(getEnclosingLoopStarts("b", operators, links)).toEqual(["S"]);
    });

    it("leaves the control operators outside their own block", () => {
      expect(getEnclosingLoopStarts("S", operators, links)).toEqual([]);
      expect(getEnclosingLoopStarts("E", operators, links)).toEqual([]);
    });

    it("leaves the operators before the start and after the end outside", () => {
      expect(getEnclosingLoopStarts("src", operators, links)).toEqual([]);
      expect(getEnclosingLoopStarts("sink", operators, links)).toEqual([]);
    });

    it("returns nothing for an operator that is not in the graph", () => {
      expect(getEnclosingLoopStarts("ghost", operators, links)).toEqual([]);
    });
  });

  describe("on a branching body", () => {
    // S -> a -> E          (a closes through E)
    // S -> b -> c          (b, c dangle: no LoopEnd downstream)
    // x -> a               (an outside operator feeding the body)
    const operators = [loopStart("S"), op("a"), op("b"), op("c"), loopEnd("E"), op("x")];
    const links = [link("S", "a"), link("a", "E"), link("S", "b"), link("b", "c"), link("x", "a")];

    it("keeps the branch that reaches a LoopEnd inside the block", () => {
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
    });

    it("leaves a dangling branch (downstream of the start, no LoopEnd reachable) outside", () => {
      expect(getEnclosingLoopStarts("b", operators, links)).toEqual([]);
      expect(getEnclosingLoopStarts("c", operators, links)).toEqual([]);
    });

    it("leaves an outside operator that only feeds the body outside", () => {
      expect(getEnclosingLoopStarts("x", operators, links)).toEqual([]);
    });
  });

  describe("on nested blocks  S1 -> p -> S2 -> inner -> E2 -> q -> E1", () => {
    const operators = [loopStart("S1"), op("p"), loopStart("S2"), op("inner"), loopEnd("E2"), op("q"), loopEnd("E1")];
    const links = [
      link("S1", "p"),
      link("p", "S2"),
      link("S2", "inner"),
      link("inner", "E2"),
      link("E2", "q"),
      link("q", "E1"),
    ];

    it("returns both starts for the inner body, outermost first", () => {
      expect(getEnclosingLoopStarts("inner", operators, links)).toEqual(["S1", "S2"]);
    });

    it("returns only the outer start for operators before and after the inner block", () => {
      expect(getEnclosingLoopStarts("p", operators, links)).toEqual(["S1"]);
      expect(getEnclosingLoopStarts("q", operators, links)).toEqual(["S1"]);
    });

    it("puts the inner control operators inside the outer block but not their own", () => {
      expect(getEnclosingLoopStarts("S2", operators, links)).toEqual(["S1"]);
      expect(getEnclosingLoopStarts("E2", operators, links)).toEqual(["S1"]);
    });

    it("leaves the outer control operators outside every block", () => {
      expect(getEnclosingLoopStarts("S1", operators, links)).toEqual([]);
      expect(getEnclosingLoopStarts("E1", operators, links)).toEqual([]);
    });
  });

  describe("on two blocks in sequence  S -> a -> E -> S' -> b -> E'", () => {
    const operators = [loopStart("S"), op("a"), loopEnd("E"), loopStart("S'"), op("b"), loopEnd("E'")];
    const links = [link("S", "a"), link("a", "E"), link("E", "S'"), link("S'", "b"), link("b", "E'")];

    it("does not let the first block leak into the second through its LoopEnd", () => {
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
      expect(getEnclosingLoopStarts("b", operators, links)).toEqual(["S'"]);
    });
  });

  describe("on incomplete blocks", () => {
    it("returns nothing when a LoopStart has no LoopEnd downstream", () => {
      const operators = [loopStart("S"), op("a")];
      const links = [link("S", "a")];
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual([]);
    });

    it("returns nothing when a LoopEnd has no LoopStart upstream", () => {
      const operators = [op("a"), loopEnd("E")];
      const links = [link("a", "E")];
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual([]);
    });

    it("returns nothing on an empty graph", () => {
      expect(getEnclosingLoopStarts("a", [], [])).toEqual([]);
    });
  });

  it("terminates on a cycle in the body", () => {
    // S -> a <-> b -> E
    const operators = [loopStart("S"), op("a"), op("b"), loopEnd("E")];
    const links = [link("S", "a"), link("a", "b"), link("b", "a"), link("b", "E")];
    expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
    expect(getEnclosingLoopStarts("b", operators, links)).toEqual(["S"]);
  });

  // A cycle through a control operator opens one more block on every lap, so an unbounded open count
  // never revisits a state: these ran the tab out of memory before the count was capped.
  describe("on a cycle through a control operator", () => {
    it("terminates on a back edge from the body into its LoopStart  S -> a -> E, a -> S", () => {
      const operators = [loopStart("S"), op("a"), loopEnd("E")];
      const links = [link("S", "a"), link("a", "E"), link("a", "S")];
      expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
    });

    describe("on the loop output fed back into the body  src -> S -> a -> E -> b -> a", () => {
      const operators = [op("src"), loopStart("S"), op("a"), loopEnd("E"), op("b")];
      const links = [link("src", "S"), link("S", "a"), link("a", "E"), link("E", "b"), link("b", "a")];

      it("terminates for the body operator and keeps it inside the block", () => {
        expect(getEnclosingLoopStarts("a", operators, links)).toEqual(["S"]);
      });

      it("terminates for the feedback operator and leaves it, downstream of the LoopEnd, outside", () => {
        expect(getEnclosingLoopStarts("b", operators, links)).toEqual([]);
      });
    });
  });
});

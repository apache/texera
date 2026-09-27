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

import { segmentScript } from "./script-segmentation";

describe("segmentScript", () => {
  // Sequential ids keep assertions readable; the real factory is uuidv4.
  function counterIds(): () => string {
    let next = 0;
    return () => `c${++next}`;
  }

  function segment(source: string, ranges: Record<string, unknown> | null | undefined) {
    return segmentScript(source, ranges, counterIds());
  }

  // A 10-line script; every line is distinguishable so slices can be asserted exactly.
  const script = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"].join("\n");

  // Cells reduced to the shape the assertions care about.
  function spans(result: ReturnType<typeof segment>): [number, number][] {
    return result.cells.map(cell => [cell.startLine, cell.endLine]);
  }

  describe("well-formed input", () => {
    it("maps a single range covering the whole file to one cell", () => {
      const result = segment(script, { UDF1: [[1, 10]] });

      expect(spans(result)).toEqual([[1, 10]]);
      expect(result.cells[0].source).toBe(script);
      expect(result.udfToCellUuids).toEqual({ UDF1: ["c1"] });
    });

    it("splits adjacent ranges into one cell each, in source order", () => {
      const result = segment(script, { UDF1: [[1, 4]], UDF2: [[5, 10]] });

      expect(spans(result)).toEqual([
        [1, 4],
        [5, 10],
      ]);
      expect(result.cells[0].source).toBe("L1\nL2\nL3\nL4");
      expect(result.cells[1].source).toBe("L5\nL6\nL7\nL8\nL9\nL10");
      expect(result.udfToCellUuids).toEqual({ UDF1: ["c1"], UDF2: ["c2"] });
    });

    it("gives a UDF every cell its several ranges cover", () => {
      const result = segment(script, { UDF1: [[1, 3]], UDF2: [[4, 6]] });
      const both = segment(script, {
        UDF1: [
          [1, 3],
          [7, 10],
        ],
        UDF2: [[4, 6]],
      });

      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
      expect(both.udfToCellUuids["UDF1"]).toEqual(["c1", "c3"]);
      expect(both.udfToCellUuids["UDF2"]).toEqual(["c2"]);
    });
  });

  describe("gaps and overlaps", () => {
    it("keeps a gap between two ranges as its own cell that no UDF claims", () => {
      const result = segment(script, { UDF1: [[1, 3]], UDF2: [[8, 10]] });

      expect(spans(result)).toEqual([
        [1, 3],
        [4, 7],
        [8, 10],
      ]);
      // The gap's source survives even though nothing maps to it, so no code is lost.
      expect(result.cells[1].source).toBe("L4\nL5\nL6\nL7");
      expect(Object.values(result.udfToCellUuids).flat()).not.toContain("c2");
    });

    it("splits an overlap into a shared cell that maps to both UDFs", () => {
      const result = segment(script, {
        UDF1: [[1, 6]],
        UDF2: [[4, 10]],
      });

      expect(spans(result)).toEqual([
        [1, 3],
        [4, 6],
        [7, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1", "c2"]);
      expect(result.udfToCellUuids["UDF2"]).toEqual(["c2", "c3"]);
    });

    it("handles a range fully nested inside another", () => {
      const result = segment(script, { UDF1: [[1, 10]], UDF2: [[4, 6]] });

      expect(spans(result)).toEqual([
        [1, 3],
        [4, 6],
        [7, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1", "c2", "c3"]);
      expect(result.udfToCellUuids["UDF2"]).toEqual(["c2"]);
    });

    it("emits cells in source order even when the ranges arrive out of order", () => {
      const result = segment(script, { UDF1: [[7, 10]], UDF2: [[1, 3]] });

      expect(spans(result)).toEqual([
        [1, 3],
        [4, 6],
        [7, 10],
      ]);
      expect(result.udfToCellUuids["UDF2"]).toEqual(["c1"]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c3"]);
    });

    it("collapses duplicate ranges for the same UDF into one cell", () => {
      const result = segment(script, {
        UDF1: [
          [1, 5],
          [1, 5],
        ],
      });

      expect(spans(result)).toEqual([
        [1, 5],
        [6, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
    });
  });

  describe("malformed ranges", () => {
    it("reads a reversed range as the span it describes", () => {
      const result = segment(script, { UDF1: [[6, 2]] });

      expect(spans(result)).toEqual([
        [1, 1],
        [2, 6],
        [7, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c2"]);
    });

    it("clamps a range that runs past the end of the file", () => {
      const result = segment(script, { UDF1: [[8, 400]] });

      expect(spans(result)).toEqual([
        [1, 7],
        [8, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c2"]);
    });

    it("clamps a range that starts before the first line", () => {
      const result = segment(script, { UDF1: [[-5, 3]] });

      expect(spans(result)).toEqual([
        [1, 3],
        [4, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
    });

    it("drops a range that lies entirely past the end and omits the UDF", () => {
      const result = segment(script, { UDF1: [[40, 50]], UDF2: [[1, 10]] });

      expect(spans(result)).toEqual([[1, 10]]);
      expect(result.udfToCellUuids).toEqual({ UDF2: ["c1"] });
    });

    it("ignores unusable entries but keeps the usable ones from the same UDF", () => {
      const result = segment(script, {
        UDF1: [[1, 4], "nonsense", null, [], [1, 2, 3], { start: 5 }, [5, 10]],
      });

      expect(spans(result)).toEqual([
        [1, 4],
        [5, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1", "c2"]);
    });

    it("drops a range whose numeric bound is not finite", () => {
      expect(segment(script, { UDF1: [[1, Infinity]] }).udfToCellUuids).toEqual({});
      expect(segment(script, { UDF1: [[NaN, 5]] }).udfToCellUuids).toEqual({});
    });

    it("drops a range whose string bound is not a number", () => {
      expect(segment(script, { UDF1: [["one", "four"]] }).udfToCellUuids).toEqual({});
    });

    it("does not throw on entirely unusable input", () => {
      expect(() => segment(script, { UDF1: "everything" })).not.toThrow();
      expect(() => segment(script, { UDF1: 42 })).not.toThrow();
      expect(segment(script, { UDF1: "everything" }).udfToCellUuids).toEqual({});
    });
  });

  describe("accepted range shapes", () => {
    it("accepts numeric strings", () => {
      const result = segment(script, { UDF1: [["1", "4"]] });

      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
      expect(result.cells[0].endLine).toBe(4);
    });

    it("accepts the { start, end } object form", () => {
      const result = segment(script, { UDF1: [{ start: 1, end: 4 }] });

      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
      expect(result.cells[0].endLine).toBe(4);
    });

    it("accepts a lone { start, end } object as a UDF's whole answer", () => {
      const result = segment(script, { UDF1: { start: 1, end: 4 } });

      expect(spans(result)).toEqual([
        [1, 4],
        [5, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
    });

    it("accepts a bare [start, end] pair rather than a list of pairs", () => {
      const result = segment(script, { UDF1: [1, 4] });

      expect(spans(result)).toEqual([
        [1, 4],
        [5, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1"]);
    });

    it("still reads a two-element list of pairs as two ranges, not one", () => {
      const result = segment(script, {
        UDF1: [
          [1, 2],
          [9, 10],
        ],
      });

      expect(spans(result)).toEqual([
        [1, 2],
        [3, 8],
        [9, 10],
      ]);
      expect(result.udfToCellUuids["UDF1"]).toEqual(["c1", "c3"]);
    });

    it("truncates fractional bounds", () => {
      const result = segment(script, { UDF1: [[1.9, 4.2]] });

      expect(spans(result)).toEqual([
        [1, 4],
        [5, 10],
      ]);
    });
  });

  describe("degenerate input", () => {
    it("returns the whole file as one unmapped cell when no ranges are reported", () => {
      const result = segment(script, {});

      expect(spans(result)).toEqual([[1, 10]]);
      expect(result.cells[0].source).toBe(script);
      expect(result.udfToCellUuids).toEqual({});
    });

    it("returns nothing for an empty script", () => {
      expect(segment("", { UDF1: [[1, 3]] })).toEqual({ cells: [], udfToCellUuids: {} });
    });

    it("returns nothing for a whitespace-only script", () => {
      expect(segment("\n  \n\t\n", { UDF1: [[1, 2]] }).cells).toEqual([]);
    });

    it("tolerates null and undefined ranges", () => {
      expect(segment(script, null).udfToCellUuids).toEqual({});
      expect(spans(segment(script, undefined))).toEqual([[1, 10]]);
    });

    it("drops a blank-line-only gap rather than emitting an empty cell", () => {
      const withBlankGap = ["import os", "", "", "print(os.getcwd())"].join("\n");
      const result = segment(withBlankGap, { UDF1: [[1, 1]], UDF2: [[4, 4]] });

      expect(spans(result)).toEqual([
        [1, 1],
        [4, 4],
      ]);
      expect(result.udfToCellUuids).toEqual({ UDF1: ["c1"], UDF2: ["c2"] });
    });
  });

  describe("source fidelity", () => {
    it("preserves blank lines and indentation inside a cell", () => {
      const body = ["def f():", "    x = 1", "", "    return x"].join("\n");
      const result = segment(body, { UDF1: [[1, 4]] });

      expect(result.cells[0].source).toBe(body);
    });

    it("normalizes CRLF and does not treat a trailing newline as a line", () => {
      const result = segment("a\r\nb\r\n", { UDF1: [[1, 2]] });

      expect(spans(result)).toEqual([[1, 2]]);
      expect(result.cells[0].source).toBe("a\nb");
    });

    it("keeps every line with content, each in exactly one cell", () => {
      // A blank-only span between two reported ranges is dropped on purpose, so the cells do
      // not reassemble to the input byte for byte. The guarantee that protects the user from
      // losing code is narrower: no line with content is dropped, duplicated, or reordered.
      const withBlankGap = ["import os", "", "", "x = compute()", "", "print(x)"].join("\n");
      const result = segment(withBlankGap, { UDF1: [[1, 1]], UDF2: [[4, 4]] });

      const contentLines = (text: string) => text.split("\n").filter(line => line.trim() !== "");

      expect(result.cells.flatMap(cell => contentLines(cell.source))).toEqual(contentLines(withBlankGap));
    });

    it("gives every cell a distinct id from the supplied factory", () => {
      const result = segment(script, { UDF1: [[1, 3]], UDF2: [[7, 9]] });
      const ids = result.cells.map(cell => cell.uuid);

      expect(ids).toEqual(["c1", "c2", "c3", "c4"]);
      expect(new Set(ids).size).toBe(ids.length);
    });

    it("defaults to real uuids when no factory is supplied", () => {
      const result = segmentScript(script, { UDF1: [[1, 10]] });

      expect(result.cells[0].uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    });
  });
});
